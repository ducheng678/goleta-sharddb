from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from sharddb.checker import check_exit
from sharddb.devhost import load_checkpoint, run_file, suite_file
from sharddb.util import InputError, canon, loads_strict


ROOT = Path(__file__).resolve().parents[1]


class ScenarioTests(unittest.TestCase):
    def run_case(self, name: str) -> dict:
        with tempfile.TemporaryDirectory(prefix="sharddb-test-") as temp:
            code, output, result = run_file(ROOT / "scenarios" / name, Path(temp) / "run")
            self.assertEqual(code, 0, result)
            self.assertTrue(result["success"])
            self.assertTrue((output / "result.json").is_file())
            self.assertTrue((output / "history.json").is_file())
            self.assertTrue((output / "trace.jsonl").is_file())
            return result

    def test_healthy_transaction_migration_and_old_source_outage(self) -> None:
        result = self.run_case("healthy_migration.json")
        final = next(item for item in result["responses"] if item["label"] == "final")
        self.assertEqual(final["response"]["values"], {"a": 93, "b": 107, "c": 13})

    def test_atomic_exchange_and_historical_plan_retry(self) -> None:
        self.run_case("rebalance_exchange.json")
        result = self.run_case("rebalance_reuse.json")
        old = next(item for item in result["responses"] if item["label"] == "old-retry")
        self.assertEqual(old["response"]["owners"]["A"]["epoch"], 1)

    def test_fault_recovery_and_late_authentic_message(self) -> None:
        self.run_case("offline_coord_recovery.json")
        self.run_case("faulty_rebalance.json")
        self.run_case("lost_persist_completion.json")

    def test_closed_read_release_recovers_after_coord_restart(self) -> None:
        # The first scenario is the reported failure verbatim.  The two
        # companions additionally prove the once-only write result and that an
        # authentically late duplicate release is harmless.
        self.run_case("read_release_recovery.json")
        recovered = self.run_case("read_release_recovery_final.json")
        final = next(item for item in recovered["responses"] if item["label"] == "final")
        self.assertEqual(final["response"]["values"], {"a": 101, "b": 100, "c": 0})
        duplicate = self.run_case("read_release_late_duplicate.json")
        final = next(item for item in duplicate["responses"] if item["label"] == "final")
        self.assertEqual(final["response"]["values"], {"a": 101, "b": 100, "c": 0})
        moved = self.run_case("read_release_migration_epoch.json")
        final = next(item for item in moved["responses"] if item["label"] == "final")
        self.assertEqual(final["response"]["values"], {"a": 101, "b": 100, "c": 0})

    def test_fresh_v1_layout_and_strict_duplicate_rejection(self) -> None:
        self.run_case("baseline_v1_fresh.json")
        with self.assertRaises(InputError):
            loads_strict('{"x":1,"x":2}')

    def test_conditionals_and_settled_checkpoint_continuation(self) -> None:
        self.run_case("conditional_transactions.json")
        with tempfile.TemporaryDirectory(prefix="sharddb-checkpoint-") as temp:
            root = Path(temp); checkpoint = root / "checkpoint"
            code, seed_output, seed = run_file(ROOT / "scenarios" / "checkpoint_seed.json", root / "seed", checkpoint_out=checkpoint)
            self.assertEqual(code, 0, seed)
            self.assertEqual(check_exit(seed_output)[0], 0)
            manifest = load_checkpoint(checkpoint)
            old_incarnation = manifest["incarnations"]["b"]
            code, continuation_output, continued = run_file(ROOT / "scenarios" / "checkpoint_continue.json", root / "continued", restore=checkpoint)
            self.assertEqual(code, 0, continued)
            self.assertEqual(check_exit(continuation_output)[0], 0)
            self.assertGreaterEqual(continued["rounds"], seed["rounds"])
            # The restored trace proves that a newly spawned b incarnation is newer.
            trace = (continuation_output / "trace.jsonl").read_text(encoding="utf-8")
            self.assertIn(f'"incarnation":{old_incarnation + 1}', trace)
            tampered = root / "tampered-checkpoint"; shutil.copytree(checkpoint, tampered)
            with (tampered / "metadata.json").open("ab") as handle: handle.write(b"x")
            with self.assertRaises(InputError): load_checkpoint(tampered)

    def test_offline_checker_rejects_explicit_test_copies(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sharddb-checker-") as temp:
            root = Path(temp)
            code, actual, result = run_file(ROOT / "scenarios" / "checkpoint_seed.json", root / "actual")
            self.assertEqual(code, 0, result)
            self.assertEqual(check_exit(actual)[0], 0)

            wrong_response = root / "test-copy-wrong-response"; shutil.copytree(actual, wrong_response)
            evidence = json.loads((wrong_response / "evidence.json").read_text(encoding="utf-8"))
            evidence["responses"][0]["response"]["status"] = "ABORTED"
            (wrong_response / "evidence.json").write_bytes(canon(evidence))
            self.assertEqual(check_exit(wrong_response)[0], 1)

            conflict = root / "test-copy-conflicting-decision"; shutil.copytree(actual, conflict)
            image_path = conflict / "stores" / "b" / "image.json"
            image = json.loads(image_path.read_text(encoding="utf-8"))
            decision = next(item["decision"] for item in image["public_decisions"] if item["decision"]["txn_id"] == "seed-txn")
            conflicting = dict(decision); conflicting["outcome"] = "ABORT"
            image["public_decisions"].append({"revision": image["revision"], "ordinal": 999, "decision": conflicting})
            image_path.write_bytes(canon(image))
            self.assertEqual(check_exit(conflict)[0], 1)

            half_read = root / "test-copy-half-snapshot"; shutil.copytree(actual, half_read)
            evidence = json.loads((half_read / "evidence.json").read_text(encoding="utf-8"))
            row = next(item for item in evidence["responses"] if item["request"].get("read_id") == "seed-read")
            row["response"]["values"] = {"a": 8, "b": 10, "c": 0}
            (half_read / "evidence.json").write_bytes(canon(evidence))
            self.assertEqual(check_exit(half_read)[0], 1)

    def test_suite_isolates_cases_and_continues_after_business_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sharddb-suite-") as temp:
            code, report = suite_file(ROOT / "scenarios" / "suite_mixed.json", Path(temp) / "suite")
            self.assertEqual(code, 1)
            self.assertEqual(len(report["entries"]), 2)
            self.assertEqual(report["entries"][0]["run_exit"], 1)
            self.assertEqual(report["entries"][1]["run_exit"], 0)
            self.assertEqual(report["entries"][1]["check_exit"], 0)


if __name__ == "__main__":
    unittest.main()
