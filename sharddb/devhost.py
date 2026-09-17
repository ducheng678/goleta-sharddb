"""Deterministic development host and scenario runner for sharddb.

It is intentionally a small simulator, but workers are real isolated Python
processes.  Only the host opens store files or controls processes; the engine
communicates exclusively through its ABI proxy.  A scenario is strict JSON with
``initial`` and ``steps``.  See README.md for the supported step forms.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .util import InputError, canon, exact_keys, is_id, is_int, loads_strict, validate_json
from .checker import check_exit


ROLE_ORDER = ["coord", "a_old", "a_target", "b", "c"]
SHARDS = ["A", "B", "C"]


class BusinessFailure(RuntimeError):
    pass


@dataclass
class Worker:
    node_id: str
    incarnation: int = -1
    proc: subprocess.Popen[str] | None = None
    queue: list[dict[str, Any]] = field(default_factory=list)
    outbuf: bytes = b""
    alive: bool = False


class DevHost:
    def __init__(self, scenario: dict[str, Any], output: Path) -> None:
        self._base_init(scenario, output)
        self._setup(scenario["initial"])

    def _base_init(self, scenario: dict[str, Any], output: Path) -> None:
        self.scenario = scenario
        self.output = output
        self.output.mkdir(parents=True, exist_ok=False)
        (self.output / "stores").mkdir()
        self.trace_file = (self.output / "trace.jsonl").open("w", encoding="utf-8")
        self.round = 0
        self.seq = 0
        self.event_seq = 0
        self.pending_actions: list[tuple[int, str, int, dict[str, Any]]] = []
        self.future: list[tuple[int, int, str, dict[str, Any], str]] = []
        self.network: list[dict[str, Any]] = []
        self.timers: dict[tuple[str, str], tuple[int, str, int]] = {}
        self.rules: list[dict[str, Any]] = []
        self.drop_completions: list[dict[str, Any]] = []
        self.drop_responses = 0
        self.tokens: dict[str, dict[str, Any]] = {}
        self.responses: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []
        self.labels: dict[str, list[dict[str, Any]]] = {}
        self.txns: dict[str, dict[str, Any]] = {}
        self.plans: dict[str, dict[str, Any]] = {}
        self.migrations: dict[tuple[str, str], dict[str, Any]] = {}

    @classmethod
    def from_checkpoint(cls, scenario: dict[str, Any], output: Path, checkpoint: Path) -> "DevHost":
        manifest = load_checkpoint(checkpoint)
        host = cls.__new__(cls)
        host._base_init(scenario, output)
        host._setup_restored(manifest, checkpoint)
        return host

    # ----- setup, trace, and files ------------------------------------------------
    def trace(self, kind: str, **data: Any) -> None:
        row = {"round": self.round, "kind": kind, **data}
        self.trace_file.write(json.dumps(row, ensure_ascii=False, sort_keys=True,
                                         separators=(",", ":")) + "\n")
        self.trace_file.flush()

    def _setup(self, initial: dict[str, Any]) -> None:
        if set(initial) - {"keys", "key_shards", "roles", "epochs"}:
            raise InputError("initial accepts only keys, key_shards, roles, epochs")
        keys = initial.get("keys")
        mapping = initial.get("key_shards")
        if not isinstance(keys, dict) or not keys or not isinstance(mapping, dict) or set(keys) != set(mapping):
            raise InputError("initial.keys and initial.key_shards must be nonempty maps with identical keys")
        if any(not is_id(k) or not is_int(v) for k, v in keys.items()):
            raise InputError("initial key names must be IDs and values exact integers")
        if any(value not in SHARDS for value in mapping.values()):
            raise InputError("key_shards values must be A, B, or C")
        roles = initial.get("roles", {role: role for role in ROLE_ORDER})
        if not isinstance(roles, dict) or set(roles) != set(ROLE_ORDER) or len(set(roles.values())) != 5 or any(not is_id(v) for v in roles.values()):
            raise InputError("roles must map each fixed role to a distinct nonempty worker ID")
        epochs = initial.get("epochs", {})
        if not isinstance(epochs, dict) or set(epochs) - set(SHARDS) or any(not is_int(v) or v < 0 for v in epochs.values()):
            raise InputError("epochs must be nonnegative integer shard entries")
        self.roles, self.keys, self.key_shards = roles, keys, mapping
        self.initial_keys = copy.deepcopy(keys)
        self.workers_order = [roles[role] for role in ROLE_ORDER]
        self.owners = {
            "A": {"owner": roles["a_old"], "epoch": epochs.get("A", 0), "migration_id": None},
            "B": {"owner": roles["b"], "epoch": epochs.get("B", 0), "migration_id": None},
            "C": {"owner": roles["c"], "epoch": epochs.get("C", 0), "migration_id": None},
        }
        self.topology = {"roles": roles, "workers": self.workers_order, "shards": SHARDS,
                         "key_shards": mapping, "initial_owners": copy.deepcopy(self.owners),
                         "cross_shard_coordinator": roles["coord"]}
        self.nodes = {node: Worker(node) for node in self.workers_order}
        values: dict[str, dict[str, int]] = {shard: {} for shard in SHARDS}
        for key, value in keys.items(): values[mapping[key]][key] = value
        for node in self.workers_order:
            local_values: dict[str, dict[str, int]] = {}
            for shard, owner in self.owners.items():
                if owner["owner"] == node: local_values[shard] = values[shard]
            baseline = {"format_version": 1, "values": local_values, "txn_begins": [], "intents": [],
                        "applied": [], "aborted": [], "read_grants": [], "read_released": [],
                        "coord_reads": [], "migrations": []}
            self._write_image(node, {"store_version": 1, "revision": 0,
                                     "data": {"baseline/v1": baseline}, "public_decisions": []})
        self._write_meta()
        for node in self.workers_order: self.restart(node, initial=True)

    def _setup_restored(self, manifest: dict[str, Any], checkpoint: Path) -> None:
        self.topology = copy.deepcopy(manifest["topology"])
        self.roles = self.topology["roles"]
        self.workers_order = list(self.topology["workers"])
        self.keys = copy.deepcopy(manifest["initial_keys"])
        self.initial_keys = copy.deepcopy(self.keys)
        self.key_shards = copy.deepcopy(self.topology["key_shards"])
        self.owners = copy.deepcopy(manifest["owners"])
        self.txns = copy.deepcopy(manifest["txns"])
        self.plans = copy.deepcopy(manifest["plans"])
        self.migrations = {(item["shard"], item["target"]): item["entry"]
                           for item in manifest["migrations"]}
        self.round = manifest["round"]
        self.seq = manifest["sequence_high_water"]
        self.event_seq = manifest["event_high_water"]
        self.responses = copy.deepcopy(manifest["responses"])
        self.calls = copy.deepcopy(manifest["calls"])
        shutil.copytree(checkpoint / "stores", self.output / "stores", dirs_exist_ok=True)
        shutil.copy2(checkpoint / "metadata.json", self.output / "metadata.json")
        self.nodes = {node: Worker(node, incarnation=manifest["incarnations"][node])
                      for node in self.workers_order}
        for node in self.workers_order: self.restart(node, initial=False)
        self.trace("checkpoint_restore", checkpoint=str(checkpoint), source=manifest["source_run"],
                   checkpoint_round=manifest["round"])

    def _store_path(self, node: str) -> Path:
        folder = self.output / "stores" / node
        folder.mkdir(exist_ok=True)
        return folder / "image.json"

    def _read_image(self, node: str) -> dict[str, Any]:
        return loads_strict(self._store_path(node).read_text(encoding="utf-8"))

    def _write_image(self, node: str, image: dict[str, Any]) -> None:
        raw = canon(image)
        if len(raw) > 8 * 1024 * 1024: raise InputError("StoreImage exceeds 8 MiB")
        path = self._store_path(node); tmp = path.with_suffix(".tmp")
        with tmp.open("wb") as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def _write_meta(self) -> None:
        (self.output / "metadata.json").write_bytes(canon({"owners": self.owners, "plans": self.plans}))

    # ----- worker process lifecycle ----------------------------------------------
    def restart(self, node: str, initial: bool = False) -> None:
        worker = self.nodes[node]
        if worker.alive: self.crash(node)
        worker.incarnation += 1
        env = dict(os.environ)
        source_root = str(Path(__file__).resolve().parents[1])
        env["PYTHONPATH"] = source_root + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        worker.proc = subprocess.Popen([sys.executable, "-m", "sharddb.worker"], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False, bufsize=0,
                                       cwd=source_root, env=env)
        worker.alive = True
        worker.queue.clear()
        worker.outbuf = b""
        worker.queue.append(self._event(node, "boot", topology=self.topology))
        self.trace("restart", worker=node, incarnation=worker.incarnation, initial=initial)

    def crash(self, node: str) -> None:
        worker = self.nodes[node]
        if not worker.alive: return
        for token in self.tokens.values():
            if token["worker"] == node and token["incarnation"] == worker.incarnation: token["active"] = False
        worker.queue.clear()
        self.future = [item for item in self.future if item[2] != node]
        self.timers = {key: value for key, value in self.timers.items() if key[0] != node}
        assert worker.proc is not None
        process = worker.proc
        process.kill(); process.wait(timeout=2)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        worker.alive = False; worker.proc = None
        self.trace("crash", worker=node, incarnation=worker.incarnation)

    def close(self) -> None:
        for node in self.workers_order:
            if self.nodes[node].alive: self.crash(node)
        self.trace_file.close()

    # ----- events and rounds ------------------------------------------------------
    def _event(self, node: str, kind: str, **fields: Any) -> dict[str, Any]:
        self.event_seq += 1
        return {"abi_version": 1, "node_id": node, "incarnation": self.nodes[node].incarnation,
                "event_id": f"e{self.event_seq}", "kind": kind, **fields}

    def _future(self, due: int, sequence: int, node: str, event: dict[str, Any], why: str) -> None:
        self.future.append((due, sequence, node, event, why))

    def _deliver_future(self) -> None:
        kept: list[tuple[int, int, str, dict[str, Any], str]] = []
        due = []
        for item in self.future:
            if item[2] == "__client__":
                kept.append(item)
            elif item[0] <= self.round and self.nodes[item[2]].alive: due.append(item)
            else: kept.append(item)
        self.future = kept
        for _, sequence, node, event, why in sorted(due, key=lambda item: item[1]):
            self.nodes[node].queue.append(event); self.trace("enqueue", target=node, event=event["kind"], why=why, sequence=sequence)
        keep_messages: list[dict[str, Any]] = []
        deliver = []
        for message in self.network:
            if message["due"] <= self.round and self.nodes[message["target"]].alive: deliver.append(message)
            else: keep_messages.append(message)
        self.network = keep_messages
        for message in sorted(deliver, key=lambda item: item["sequence"]):
            event = self._event(message["target"], "message", message_id=message["message_id"],
                                source=message["source"], source_incarnation=message["source_incarnation"],
                                body=message["body"])
            self.nodes[message["target"]].queue.append(event)
            self.trace("message_deliver", message_id=message["message_id"], target=message["target"], source=message["source"])
        due_timers = []
        for key, timer in list(self.timers.items()):
            if timer[0] <= self.round and self.nodes[key[0]].alive:
                due_timers.append((key, timer)); del self.timers[key]
        for (node, token), (_, timer_id, sequence) in sorted(due_timers, key=lambda item: item[1][2]):
            self.nodes[node].queue.append(self._event(node, "timer", token=token, timer_id=timer_id))
            self.trace("timer_enqueue", worker=node, token=token)

    def one_round(self) -> None:
        self.round += 1
        self._deliver_future()
        for node in self.workers_order:
            worker = self.nodes[node]
            if worker.alive and worker.queue:
                event = worker.queue.pop(0)
                self.trace("event", worker=node, event=event["kind"], event_id=event["event_id"])
                for action in self._call_worker(worker, event):
                    self.seq += 1
                    self.pending_actions.append((self.seq, node, worker.incarnation, action))
        actions, self.pending_actions = self.pending_actions, []
        rank = {node: index for index, node in enumerate(self.workers_order)}
        for sequence, node, incarnation, action in sorted(actions, key=lambda item: (rank[item[1]], item[0])):
            self.execute_action(sequence, node, incarnation, action)
        self._enforce_progress()

    def run_rounds(self, count: int) -> None:
        if not is_int(count) or count < 0: raise InputError("round count must be a nonnegative integer")
        for _ in range(count): self.one_round()

    def _enforce_progress(self) -> None:
        """Keep public liveness ceilings independent of a scenario's await value."""
        for token_id, token in self.tokens.items():
            if not token["active"] or "reply" in token:
                continue
            if self.round > token["deadline"]:
                raise BusinessFailure(f"request {token_id} exceeded its public {token['budget']}-round progress budget")

    def _call_worker(self, worker: Worker, event: dict[str, Any]) -> list[dict[str, Any]]:
        assert worker.proc and worker.proc.stdin and worker.proc.stdout
        worker.proc.stdin.write((json.dumps({"kind": "event", "event": event}, ensure_ascii=False,
                                            separators=(",", ":")) + "\n").encode("utf-8")); worker.proc.stdin.flush()
        actions: list[dict[str, Any]] = []; deadline = time.monotonic() + 5
        while True:
            timeout = deadline - time.monotonic()
            if timeout <= 0: raise BusinessFailure(f"worker {worker.node_id} callback exceeded 5 seconds")
            line: bytes | None = None
            if b"\n" in worker.outbuf:
                line, worker.outbuf = worker.outbuf.split(b"\n", 1)
            else:
                readable, _, _ = select.select([worker.proc.stdout], [], [], timeout)
                if not readable:
                    continue
                chunk = os.read(worker.proc.stdout.fileno(), 65536)
                if not chunk:
                    stderr = worker.proc.stderr.read() if worker.proc.stderr else b""
                    raise BusinessFailure(f"worker {worker.node_id} exited during callback: {stderr.decode('utf-8', 'replace')[-1000:]}")
                worker.outbuf += chunk
                continue
            if line is None:
                continue
            if not line:
                stderr = worker.proc.stderr.read() if worker.proc.stderr else ""
                raise BusinessFailure(f"worker {worker.node_id} exited during callback: {stderr[-1000:]}")
            try: message = loads_strict(line.decode("utf-8"))
            except InputError as exc: raise BusinessFailure(f"invalid worker frame: {exc}") from exc
            if message.get("kind") == "action":
                if set(message) != {"kind", "action_id", "op", "args"}: raise BusinessFailure("malformed action frame")
                actions.append(message); self.trace("action_accept", worker=worker.node_id, action_id=message["action_id"], op=message["op"], args=message["args"])
            elif message.get("kind") == "done" and message.get("event_id") == event["event_id"]:
                return actions
            else: raise BusinessFailure("unexpected worker frame")

    # ----- actions ---------------------------------------------------------------
    def completion(self, sequence: int, node: str, incarnation: int, action: dict[str, Any], result: dict[str, Any]) -> None:
        if self._consume_drop_completion(action["op"]):
            self.trace("completion_dropped", worker=node, action_id=action["action_id"], op=action["op"]); return
        if self.nodes[node].alive and self.nodes[node].incarnation == incarnation:
            event = self._event(node, "completion", action_id=action["action_id"], op=action["op"], result=result)
            self._future(self.round + 1, sequence, node, event, "completion")

    def _consume_drop_completion(self, op: str) -> bool:
        for rule in self.drop_completions:
            if rule["count"] and (rule["op"] == "*" or rule["op"] == op):
                rule["count"] -= 1; return True
        return False

    def execute_action(self, sequence: int, node: str, incarnation: int, action: dict[str, Any]) -> None:
        op, args = action["op"], action["args"]
        try:
            if op == "send": result = self.action_send(sequence, node, incarnation, action, args)
            elif op == "set_timer": result = self.action_timer(sequence, node, action, args)
            elif op == "read_local": result = {"status": "OK", "image": self._read_image(node)}
            elif op == "persist": result = self.action_persist(node, args)
            elif op == "read_owner": result = self.action_read_owner(args)
            elif op == "read_owners": result = self.action_read_owners(args)
            elif op == "cas_owner": result = self.action_cas_owner(node, args)
            elif op == "cas_owners": result = self.action_cas_owners(node, args)
            elif op == "reply": result = self.action_reply(sequence, node, incarnation, args)
            else: raise BusinessFailure(f"unknown engine action {op}")
            self.trace("action_execute", worker=node, action_id=action["action_id"], op=op, result=result)
            self.completion(sequence, node, incarnation, action, result)
        except (InputError, KeyError, TypeError) as exc:
            raise BusinessFailure(f"invalid {op} action from {node}: {exc}") from exc

    def action_send(self, sequence: int, node: str, incarnation: int, action: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"peer", "body"} or args["peer"] not in self.nodes: raise InputError("invalid send")
        validate_json(args["body"])
        copies, delay, drop, late_duplicate = self.network_effect(args["body"], "message")
        message_id = action["action_id"]
        if not drop:
            for copy_index in range(copies):
                copy_delay = late_duplicate if copy_index and late_duplicate is not None else delay
                self.network.append({"sequence": sequence * 100 + copy_index, "due": self.round + 1 + copy_delay,
                    "message_id": message_id, "source": node, "source_incarnation": incarnation,
                    "target": args["peer"], "body": copy.deepcopy(args["body"])})
        self.trace("message_send", message_id=message_id, source=node, target=args["peer"], delay=delay, copies=copies, dropped=drop)
        return {"status": "QUEUED", "message_id": message_id}

    def network_effect(self, body: Any, channel: str) -> tuple[int, int, bool, int | None]:
        copies, delay, drop, late_duplicate = 1, 0, False, None
        typ = body.get("type") if isinstance(body, dict) else None
        for rule in self.rules:
            if rule["count"] == 0 or rule.get("channel", "message") != channel: continue
            match = rule.get("type")
            if match is not None and typ != match: continue
            rule["count"] -= 1
            delay = max(delay, rule.get("delay", 0)); copies += rule.get("duplicate", 0); drop = drop or rule.get("drop", False)
            late_duplicate = rule.get("late_duplicate")
            break
        return copies, delay, drop, late_duplicate

    def action_timer(self, sequence: int, node: str, action: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"token", "rounds"} or not is_id(args["token"]) or not is_int(args["rounds"]) or args["rounds"] <= 0:
            raise InputError("invalid set_timer")
        self.timers[(node, args["token"])] = (self.round + args["rounds"], action["action_id"], sequence)
        return {"status": "SCHEDULED", "timer_id": action["action_id"]}

    def action_persist(self, node: str, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"expected_revision", "edits", "public_decisions"} or not is_int(args["expected_revision"]): raise InputError("invalid persist")
        image = self._read_image(node)
        if image["revision"] != args["expected_revision"]: return {"status": "CONFLICT", "revision": image["revision"]}
        edits = args["edits"]
        if not isinstance(edits, list) or not isinstance(args["public_decisions"], list): raise InputError("invalid persist lists")
        used: set[str] = set(); data = copy.deepcopy(image["data"])
        for edit in edits:
            if not isinstance(edit, dict) or edit.get("op") not in ("put", "delete") or not is_id(edit.get("key")) or edit["key"] in used:
                raise InputError("invalid persist edit")
            used.add(edit["key"])
            if edit["op"] == "put":
                if set(edit) != {"op", "key", "value"}: raise InputError("invalid put")
                validate_json(edit["value"]); data[edit["key"]] = edit["value"]
            else:
                if set(edit) != {"op", "key"}: raise InputError("invalid delete")
                data.pop(edit["key"], None)
        for decision in args["public_decisions"]:
            if not isinstance(decision, dict) or set(decision) != {"txn_id", "request_digest", "coordinator", "outcome"} or decision["outcome"] not in ("COMMIT", "ABORT"):
                raise InputError("invalid public decision")
        revision = image["revision"] + 1
        journal = copy.deepcopy(image["public_decisions"])
        for ordinal, decision in enumerate(args["public_decisions"]): journal.append({"revision": revision, "ordinal": ordinal, "decision": decision})
        new = {"store_version": 1, "revision": revision, "data": data, "public_decisions": journal}
        self._write_image(node, new)
        return {"status": "STORED", "revision": revision}

    def action_read_owner(self, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"shard"} or args["shard"] not in SHARDS: raise InputError("invalid read_owner")
        return {"status": "OK", "record": copy.deepcopy(self.owners[args["shard"]])}

    def action_read_owners(self, args: dict[str, Any]) -> dict[str, Any]:
        shards = args.get("shards") if isinstance(args, dict) else None
        if set(args) != {"shards"} or not isinstance(shards, list) or not shards or len(set(shards)) != len(shards) or any(shard not in SHARDS for shard in shards):
            raise InputError("invalid read_owners")
        return {"status": "OK", "records": {shard: copy.deepcopy(self.owners[shard]) for shard in shards}}

    def action_cas_owner(self, node: str, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"shard", "expected", "replacement"} or args["shard"] not in SHARDS: raise InputError("invalid cas_owner")
        shard, expected, replacement = args["shard"], args["expected"], args["replacement"]
        registered = next((entry for entry in self.migrations.values() if entry["expected"] == {shard: expected} and entry["replacement"] == {shard: replacement}), None)
        if not registered or node not in (expected["owner"], replacement["owner"]): raise InputError("unregistered ownership CAS")
        current = self.owners[shard]
        if current != expected: return {"status": "MISMATCH", "record": copy.deepcopy(current)}
        if expected == replacement: return {"status": "UNCHANGED", "record": copy.deepcopy(current)}
        self.owners[shard] = copy.deepcopy(replacement); self._write_meta()
        return {"status": "SWAPPED", "record": copy.deepcopy(replacement)}

    def action_cas_owners(self, node: str, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"expected", "replacement"} or not isinstance(args["expected"], dict) or set(args["expected"]) != set(args["replacement"]): raise InputError("invalid cas_owners")
        expected, replacement = args["expected"], args["replacement"]
        registered = next((plan for plan in self.plans.values() if plan["expected"] == expected and plan["replacement"] == replacement), None)
        if not registered or node not in {x["owner"] for x in expected.values()} | {x["owner"] for x in replacement.values()}:
            raise InputError("unregistered owner-vector CAS")
        current = {shard: self.owners[shard] for shard in expected}
        if current != expected: return {"status": "MISMATCH", "records": copy.deepcopy(current)}
        if expected == replacement: return {"status": "UNCHANGED", "records": copy.deepcopy(current)}
        self.owners.update(copy.deepcopy(replacement)); self._write_meta()
        return {"status": "SWAPPED", "records": copy.deepcopy(replacement)}

    def action_reply(self, sequence: int, node: str, incarnation: int, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"request_token", "payload"} or args["request_token"] not in self.tokens: raise InputError("invalid reply token")
        token = self.tokens[args["request_token"]]
        if token["worker"] != node or token["incarnation"] != incarnation or not token["active"]: return {"status": "STALE_TOKEN"}
        self.validate_response(token["request"], args["payload"])
        if "reply" in token:
            if token["reply"] != args["payload"]: raise InputError("conflicting reply for token")
            return {"status": "ALREADY_RECORDED"}
        token["reply"] = copy.deepcopy(args["payload"])
        if self.drop_responses:
            self.drop_responses -= 1; self.trace("response_dropped", token=args["request_token"])
        else:
            _, delay, drop, _ = self.network_effect(args["payload"], "response")
            if not drop:
                self._future(self.round + 1 + delay, sequence, "__client__", {"token": args["request_token"]}, "response")
        return {"status": "RECORDED"}

    # ----- client requests and schemas -------------------------------------------
    def validate_request(self, request: Any) -> None:
        if not isinstance(request, dict) or not isinstance(request.get("op"), str): raise InputError("request must be an object with op")
        op = request["op"]
        forms = {"txn": {"op", "txn_id", "delta_map"},
                 "conditional_txn": {"op", "txn_id", "expected", "delta_map"}, "status": {"op", "txn_id"},
                 "snapshot": {"op", "read_id", "keys"}, "migrate": {"op", "shard", "target"},
                 "rebalance": {"op", "plan_id", "moves"}}
        if op not in forms or set(request) != forms[op]: raise InputError(f"invalid {op!r} request fields")
        if op == "txn":
            if not is_id(request["txn_id"]) or not isinstance(request["delta_map"], dict) or not request["delta_map"] or any(key not in self.keys or not is_int(value) for key, value in request["delta_map"].items()): raise InputError("invalid txn")
        elif op == "conditional_txn":
            if (not is_id(request["txn_id"]) or not isinstance(request["expected"], dict) or not request["expected"] or
                    not isinstance(request["delta_map"], dict) or not request["delta_map"] or
                    any(key not in self.keys or not is_int(value) for key, value in request["expected"].items()) or
                    any(key not in self.keys or not is_int(value) for key, value in request["delta_map"].items())):
                raise InputError("invalid conditional_txn")
        elif op == "status":
            if not is_id(request["txn_id"]) or request["txn_id"] not in self.txns: raise InputError("status requires an already begun txn")
        elif op == "snapshot":
            if not is_id(request["read_id"]) or not isinstance(request["keys"], list) or not request["keys"] or len(set(request["keys"])) != len(request["keys"]) or any(key not in self.keys for key in request["keys"]): raise InputError("invalid snapshot")
        elif op == "migrate":
            if request["shard"] != "A" or request["target"] != self.roles["a_target"]: raise InputError("legacy migrate is only A to roles.a_target")
        else:
            if not is_id(request["plan_id"]) or not isinstance(request["moves"], dict) or not request["moves"] or set(request["moves"]) - {"A", "B"} or any(target not in (self.roles["a_old"], self.roles["a_target"], self.roles["b"]) for target in request["moves"].values()): raise InputError("invalid rebalance")

    def invoke(self, request: dict[str, Any], label: str | None = None) -> str:
        self.validate_request(request)
        request = copy.deepcopy(request); op = request["op"]
        if op in ("txn", "conditional_txn"):
            existing = self.txns.get(request["txn_id"])
            if existing and existing != request: raise InputError("a txn_id must retain the exact original request")
            self.txns[request["txn_id"]] = request
            touched = set(request["delta_map"])
            if op == "conditional_txn": touched.update(request["expected"])
            parts = sorted({self.key_shards[key] for key in touched})
            target = self.owners[parts[0]]["owner"] if len(parts) == 1 else self.roles["coord"]
        elif op == "status":
            original = self.txns[request["txn_id"]]; touched = set(original["delta_map"])
            if original["op"] == "conditional_txn": touched.update(original["expected"])
            parts = sorted({self.key_shards[key] for key in touched})
            target = self.owners[parts[0]]["owner"] if len(parts) == 1 else self.roles["coord"]
        elif op == "snapshot":
            parts = sorted({self.key_shards[key] for key in request["keys"]})
            target = self.owners[parts[0]]["owner"] if len(parts) == 1 else self.roles["coord"]
        elif op == "migrate":
            target = self.register_migration(request)
        else:
            target = self.register_plan(request)
        self.seq += 1; token = f"r{self.seq}"
        worker = self.nodes[target]
        self.tokens[token] = {"request": request, "worker": target, "incarnation": worker.incarnation,
                              "active": worker.alive, "label": label,
                              "budget": self.request_budget(request), "deadline": self.round + self.request_budget(request)}
        fields: dict[str, Any] = {"request_token": token, "request": request}
        if op == "migrate":
            entry = self.migrations[(request["shard"], request["target"])]
            fields.update({"migration_id": entry["id"]})
        elif op == "rebalance":
            entry = self.plans[request["plan_id"]]
            fields["rebalance"] = {"plan_id": request["plan_id"], "expected": entry["expected"], "replacement": entry["replacement"]}
        if worker.alive:
            self._future(self.round + 1, self.seq, target, self._event(target, "client_request", **fields), "client_request")
        self.calls.append({"round": self.round, "token": token, "label": label,
                           "target": target, "request": copy.deepcopy(request)})
        self.trace("request", token=token, target=target, request=request, label=label)
        return token

    def request_budget(self, request: dict[str, Any]) -> int:
        if request["op"] in ("migrate", "rebalance"):
            return 2000
        if request["op"] in ("txn", "conditional_txn"):
            touched = set(request["delta_map"])
            if request["op"] == "conditional_txn": touched.update(request["expected"])
            shards = {self.key_shards[key] for key in touched}
            if shards == {"C"}: return 1000
        if request["op"] == "snapshot":
            shards = {self.key_shards[key] for key in request["keys"]}
            if shards == {"C"}: return 1000
        return 10000

    def register_migration(self, request: dict[str, Any]) -> str:
        key = (request["shard"], request["target"]); old = self.migrations.get(key)
        if old: return self.owners[request["shard"]]["owner"]
        shard = request["shard"]; expected_record = copy.deepcopy(self.owners[shard])
        if expected_record["owner"] == request["target"]: raise InputError("migration target already owns shard")
        ident = f"migration:{shard}:{request['target']}:{expected_record['epoch']}"
        replacement = {"owner": request["target"], "epoch": expected_record["epoch"] + 1, "migration_id": ident}
        entry = {"id": ident, "expected": {shard: expected_record}, "replacement": {shard: replacement}}
        self.migrations[key] = entry; self._write_meta(); return expected_record["owner"]

    def register_plan(self, request: dict[str, Any]) -> str:
        old = self.plans.get(request["plan_id"])
        if old:
            if old["moves"] != request["moves"]: raise InputError("plan_id must retain its original moves")
            first = sorted(old["moves"])[0]; return self.owners[first]["owner"]
        moves = request["moves"]
        expected = {shard: copy.deepcopy(self.owners[shard]) for shard in moves}
        if any(expected[shard]["owner"] == target for shard, target in moves.items()): raise InputError("a plan destination must differ from its current owner")
        replacement = {shard: {"owner": target, "epoch": expected[shard]["epoch"] + 1,
                               "migration_id": request["plan_id"]} for shard, target in moves.items()}
        entry = {"moves": copy.deepcopy(moves), "expected": expected, "replacement": replacement}
        self.plans[request["plan_id"]] = entry; self._write_meta()
        return expected[sorted(moves)[0]]["owner"]

    def validate_response(self, request: dict[str, Any], payload: Any) -> None:
        if not isinstance(payload, dict): raise InputError("response is not object")
        op = request["op"]
        if op in ("txn", "conditional_txn", "status"):
            exact_keys(payload, {"txn_id", "status"}, "transaction response")
            if payload["txn_id"] != request["txn_id"] or payload["status"] not in ("COMMITTED", "ABORTED", "UNKNOWN"): raise InputError("invalid transaction response")
        elif op == "snapshot":
            allowed = {"read_id", "status"} if payload.get("status") == "UNKNOWN" else {"read_id", "status", "values"}
            exact_keys(payload, allowed, "snapshot response")
            if payload["read_id"] != request["read_id"] or payload["status"] not in ("OK", "UNKNOWN"): raise InputError("invalid snapshot response")
            if payload["status"] == "OK" and (not isinstance(payload["values"], dict) or set(payload["values"]) != set(request["keys"]) or any(not is_int(value) for value in payload["values"].values())): raise InputError("invalid snapshot values")
        elif op == "migrate":
            allowed = {"shard", "target", "status"} if payload.get("status") == "UNKNOWN" else {"shard", "target", "status", "epoch"}
            exact_keys(payload, allowed, "migration response")
            if payload["shard"] != request["shard"] or payload["target"] != request["target"] or payload["status"] not in ("COMPLETE", "UNKNOWN") or (payload["status"] == "COMPLETE" and (not is_int(payload["epoch"]) or payload["epoch"] < 0)): raise InputError("invalid migration response")
        else:
            allowed = {"plan_id", "status"} if payload.get("status") == "UNKNOWN" else {"plan_id", "status", "owners"}
            exact_keys(payload, allowed, "rebalance response")
            if payload["plan_id"] != request["plan_id"] or payload["status"] not in ("COMPLETE", "UNKNOWN"): raise InputError("invalid rebalance response")
            if payload["status"] == "COMPLETE" and payload["owners"] != self.plans[request["plan_id"]]["replacement"]: raise InputError("wrong historical replacement vector")

    def receive_response(self, token_id: str) -> None:
        token = self.tokens[token_id]; payload = token["reply"]
        token["received"] = True
        record = {"round": self.round, "token": token_id, "label": token["label"], "request": token["request"], "response": payload}
        self.responses.append(record)
        if token["label"]: self.labels.setdefault(token["label"], []).append(record)
        self.trace("response", **record)

    # ----- scenario program -------------------------------------------------------
    def await_label(self, label: str, maximum: int) -> None:
        if label in self.labels and self.labels[label]: return
        for _ in range(maximum):
            self.one_round()
            # Client response pseudo-events deliberately bypass worker queues.
            self._drain_client_events()
            if label in self.labels and self.labels[label]: return
        raise BusinessFailure(f"request {label!r} did not receive a response in {maximum} logical rounds")

    def _drain_client_events(self) -> None:
        kept = []
        for due, sequence, node, event, why in self.future:
            if node == "__client__" and due <= self.round:
                self.receive_response(event["token"])
            else: kept.append((due, sequence, node, event, why))
        self.future = kept

    def execute_steps(self) -> None:
        for index, step in enumerate(self.scenario["steps"]):
            if not isinstance(step, dict) or not isinstance(step.get("action"), str): raise InputError(f"steps[{index}] needs action")
            action = step["action"]
            if action == "request":
                if set(step) - {"action", "request", "label"} or "request" not in step: raise InputError("request step fields")
                self.invoke(step["request"], step.get("label"))
            elif action == "rounds":
                exact_keys(step, {"action", "count"}, "rounds step"); self.run_rounds(step["count"]); self._drain_client_events()
            elif action == "await":
                if set(step) - {"action", "label", "max_rounds"} or not is_id(step.get("label")) or not is_int(step.get("max_rounds", 2000)): raise InputError("await step")
                self.await_label(step["label"], step.get("max_rounds", 2000))
            elif action == "crash":
                exact_keys(step, {"action", "worker"}, "crash step"); self._known_worker(step["worker"]); self.crash(step["worker"])
            elif action == "restart":
                exact_keys(step, {"action", "worker"}, "restart step"); self._known_worker(step["worker"]); self.restart(step["worker"])
            elif action == "network_rule":
                exact_keys(step, {"action", "rule"}, "network_rule step"); self.add_rule(step["rule"])
            elif action == "drop_completion":
                if set(step) - {"action", "op", "count"} or not isinstance(step.get("op", "*"), str) or not is_int(step.get("count", 1)) or step.get("count", 1) < 1: raise InputError("drop_completion step")
                self.drop_completions.append({"op": step.get("op", "*"), "count": step.get("count", 1)})
            elif action == "drop_response":
                exact_keys(step, {"action", "count"}, "drop_response step")
                if not is_int(step["count"]) or step["count"] < 1: raise InputError("drop_response count")
                self.drop_responses += step["count"]
            elif action == "assert_response": self.assert_response(step)
            elif action == "assert_owners": self.assert_owners(step)
            elif action == "assert_values": self.assert_values(step)
            else: raise InputError(f"unknown scenario action {action!r}")
            self._drain_client_events()

    def _known_worker(self, worker: Any) -> None:
        if worker not in self.nodes: raise InputError("unknown worker")

    def add_rule(self, rule: Any) -> None:
        if not isinstance(rule, dict) or set(rule) - {"channel", "type", "delay", "duplicate", "late_duplicate", "drop", "count"}: raise InputError("invalid network rule")
        channel = rule.get("channel", "message")
        if channel not in ("message", "response") or ("type" in rule and not isinstance(rule["type"], str)) or not is_int(rule.get("delay", 0)) or rule.get("delay", 0) < 0 or not is_int(rule.get("duplicate", 0)) or rule.get("duplicate", 0) < 0 or ("late_duplicate" in rule and (not is_int(rule["late_duplicate"]) or rule["late_duplicate"] < 0 or rule.get("duplicate", 0) < 1)) or not isinstance(rule.get("drop", False), bool) or not is_int(rule.get("count", 1)) or rule.get("count", 1) < 1: raise InputError("invalid network rule values")
        self.rules.append({"channel": channel, "type": rule.get("type"), "delay": rule.get("delay", 0),
                           "duplicate": rule.get("duplicate", 0), "late_duplicate": rule.get("late_duplicate"), "drop": rule.get("drop", False), "count": rule.get("count", 1)})

    def assert_response(self, step: dict[str, Any]) -> None:
        exact_keys(step, {"action", "label", "equals"}, "assert_response step")
        if step["label"] not in self.labels or not self.labels[step["label"]]: raise BusinessFailure(f"no response for {step['label']!r}")
        actual = self.labels[step["label"]][-1]["response"]
        if actual != step["equals"]: raise BusinessFailure(f"response {step['label']!r}: expected {step['equals']!r}, got {actual!r}")

    def assert_owners(self, step: dict[str, Any]) -> None:
        exact_keys(step, {"action", "equals"}, "assert_owners step")
        if self.owners != step["equals"]: raise BusinessFailure(f"owners mismatch: {self.owners!r}")

    def assert_values(self, step: dict[str, Any]) -> None:
        # This assertion derives values from an actual successful snapshot, never disk.
        exact_keys(step, {"action", "label", "equals"}, "assert_values step")
        if step["label"] not in self.labels or not self.labels[step["label"]]: raise BusinessFailure("no snapshot response")
        response = self.labels[step["label"]][-1]["response"]
        if response.get("status") != "OK" or response.get("values") != step["equals"]: raise BusinessFailure("snapshot values mismatch")

    # ----- settled checkpoints ---------------------------------------------------
    def drain_for_checkpoint(self, maximum: int = 10_000) -> None:
        """Run real events until no business responsibility remains.

        Future maintenance timers are deliberately excluded: the checkpoint
        boundary may cancel only those timers, never a queued message/action.
        """
        for _ in range(maximum + 1):
            self._drain_client_events()
            if self._checkpoint_settled():
                return
            if _ == maximum:
                break
            self.one_round()
        raise BusinessFailure("checkpoint export refused: database did not settle within 10000 logical rounds")

    def _checkpoint_settled(self) -> bool:
        if self.pending_actions or self.future or self.network:
            return False
        if any(worker.queue for worker in self.nodes.values()):
            return False
        received = {item["token"] for item in self.responses}
        if any(call["token"] not in received for call in self.calls):
            return False
        decisions: dict[str, set[str]] = {}
        for node in self.workers_order:
            image = self._read_image(node)
            for entry in image.get("public_decisions", []):
                decision = entry.get("decision", {})
                tid, outcome = decision.get("txn_id"), decision.get("outcome")
                if isinstance(tid, str) and outcome in ("COMMIT", "ABORT"):
                    decisions.setdefault(tid, set()).add(outcome)
            state = image.get("data", {}).get("sharddb/v2")
            if isinstance(state, dict):
                for shard in state.get("shards", {}).values():
                    if shard.get("write_lock") or shard.get("read_locks"):
                        return False
                if any(tx.get("phase") not in ("COMMITTED", "ABORTED")
                       for tx in state.get("coord_txns", {}).values()):
                    return False
                if any(read.get("phase") != "CLOSED" for read in state.get("coord_reads", {}).values()):
                    return False
            else:
                base = image.get("data", {}).get("baseline/v1", {})
                if base.get("intents") or base.get("read_grants") or any(
                        read.get("state") == "OPEN" for read in base.get("coord_reads", [])):
                    return False
        if any(len(outcomes) != 1 for outcomes in decisions.values()):
            return False
        if any(tid not in decisions for tid in self.txns):
            return False
        for plan_id in self.plans:
            if not any(row["request"].get("op") == "rebalance" and row["request"].get("plan_id") == plan_id and row["response"].get("status") == "COMPLETE" for row in self.responses):
                return False
        for shard, target in self.migrations:
            if not any(row["request"].get("op") == "migrate" and row["request"].get("shard") == shard and row["request"].get("target") == target and row["response"].get("status") == "COMPLETE" for row in self.responses):
                return False
        return True

    def export_checkpoint(self, destination: Path) -> None:
        self.drain_for_checkpoint()
        if destination.exists():
            raise InputError(f"checkpoint destination already exists: {destination}")
        destination.mkdir(parents=True)
        shutil.copytree(self.output / "stores", destination / "stores")
        self._write_meta()
        shutil.copy2(self.output / "metadata.json", destination / "metadata.json")
        files: dict[str, str] = {}
        for path in sorted((destination / "stores").rglob("image.json")) + [destination / "metadata.json"]:
            relative = path.relative_to(destination).as_posix()
            files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest = {"checkpoint_version": 1, "source_run": str(self.output), "round": self.round,
                    "sequence_high_water": self.seq, "event_high_water": self.event_seq,
                    "topology": self.topology, "initial_keys": self.initial_keys,
                    "owners": self.owners, "txns": self.txns, "plans": self.plans,
                    "migrations": [{"shard": shard, "target": target, "entry": entry}
                                   for (shard, target), entry in self.migrations.items()],
                    "incarnations": {node: worker.incarnation for node, worker in self.nodes.items()},
                    "calls": self.calls, "responses": self.responses, "files": files}
        (destination / "checkpoint.json").write_bytes(canon(manifest))
        self.trace("checkpoint_export", destination=str(destination), files=files)

    def result(self, success: bool, error: str | None = None) -> dict[str, Any]:
        return {"success": success, "error": error, "rounds": self.round, "owners": self.owners,
                "responses": self.responses, "calls": self.calls, "topology": self.topology,
                "initial_keys": self.initial_keys, "output": str(self.output)}


def load_checkpoint(checkpoint: Path) -> dict[str, Any]:
    if not checkpoint.is_dir():
        raise InputError(f"checkpoint is not a directory: {checkpoint}")
    manifest_path = checkpoint / "checkpoint.json"
    if not manifest_path.is_file():
        raise InputError("checkpoint is missing checkpoint.json")
    manifest = loads_strict(manifest_path.read_text(encoding="utf-8"))
    required = {"checkpoint_version", "source_run", "round", "sequence_high_water", "event_high_water",
                "topology", "initial_keys", "owners", "txns", "plans", "migrations", "incarnations",
                "calls", "responses", "files"}
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise InputError("unsupported or malformed checkpoint manifest")
    if manifest["checkpoint_version"] != 1:
        raise InputError(f"unsupported checkpoint version: {manifest['checkpoint_version']!r}")
    if not all(is_int(manifest[field]) and manifest[field] >= 0
               for field in ("round", "sequence_high_water", "event_high_water")):
        raise InputError("checkpoint sequence fields must be nonnegative integers")
    topology = manifest["topology"]
    if not isinstance(topology, dict) or set(topology) != {"roles", "workers", "shards", "key_shards", "initial_owners", "cross_shard_coordinator"}:
        raise InputError("checkpoint topology is malformed")
    workers = topology["workers"]
    if not isinstance(workers, list) or workers != [topology["roles"][role] for role in ROLE_ORDER]:
        raise InputError("checkpoint worker order is malformed")
    if not isinstance(manifest["incarnations"], dict) or set(manifest["incarnations"]) != set(workers) or any(not is_int(value) or value < 0 for value in manifest["incarnations"].values()):
        raise InputError("checkpoint incarnations are malformed")
    files = manifest["files"]
    if not isinstance(files, dict) or not files:
        raise InputError("checkpoint file manifest is malformed")
    expected = {"metadata.json", *(f"stores/{node}/image.json" for node in workers)}
    if set(files) != expected:
        raise InputError("checkpoint file manifest is incomplete or has unexpected entries")
    for relative, expected_hash in files.items():
        if not isinstance(expected_hash, str) or len(expected_hash) != 64:
            raise InputError(f"invalid SHA256 manifest entry for {relative}")
        target = checkpoint / relative
        if not target.is_file():
            raise InputError(f"checkpoint file is missing: {relative}")
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual != expected_hash:
            raise InputError(f"checkpoint file SHA256 mismatch: {relative}")
    metadata = loads_strict((checkpoint / "metadata.json").read_text(encoding="utf-8"))
    if not isinstance(metadata, dict) or metadata.get("owners") != manifest["owners"] or metadata.get("plans") != manifest["plans"]:
        raise InputError("checkpoint metadata does not agree with manifest")
    return manifest


def run_file(path: Path, output: Path | None, restore: Path | None = None,
             checkpoint_out: Path | None = None) -> tuple[int, Path, dict[str, Any]]:
    scenario = loads_strict(path.read_text(encoding="utf-8"))
    if not isinstance(scenario, dict) or not isinstance(scenario.get("steps"), list):
        raise InputError("scenario must contain a steps array")
    if restore is None:
        if set(scenario) != {"initial", "steps"} or not isinstance(scenario.get("initial"), dict):
            raise InputError("fresh scenario must have exactly initial and steps")
    elif set(scenario) != {"steps"}:
        raise InputError("restored continuation scenario must contain exactly steps")
    if output is None:
        stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        output = Path("repro-runs") / f"{path.stem}-{stamp}-{uuid.uuid4().hex[:8]}"
    host: DevHost | None = None
    try:
        host = DevHost.from_checkpoint(scenario, output, restore) if restore else DevHost(scenario, output)
        host.execute_steps()
        if checkpoint_out is not None:
            host.export_checkpoint(checkpoint_out)
        result = host.result(True)
        code = 0
    except BusinessFailure as exc:
        result = host.result(False, str(exc)) if host else {"success": False, "error": str(exc)}; code = 1
    finally:
        if host: host.close()
    output.mkdir(parents=True, exist_ok=True)
    (output / "result.json").write_bytes(canon(result))
    (output / "history.json").write_bytes(canon(result.get("responses", [])))
    (output / "evidence.json").write_bytes(canon({"format": 1, "initial_keys": result.get("initial_keys"),
        "topology": result.get("topology"), "calls": result.get("calls", []), "responses": result.get("responses", []),
        "owners": result.get("owners")}))
    return code, output, result


def suite_file(manifest_path: Path, output: Path) -> tuple[int, dict[str, Any]]:
    manifest = loads_strict(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or set(manifest) != {"scenarios"} or not isinstance(manifest["scenarios"], list) or not manifest["scenarios"]:
        raise InputError("suite manifest must have one nonempty scenarios array")
    if output.exists():
        raise InputError(f"suite output already exists: {output}")
    entries = manifest["scenarios"]
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"name", "file"} or not is_id(entry["name"]) or not isinstance(entry["file"], str) or not entry["file"] or Path(entry["file"]).is_absolute() or entry["name"] in (".", "..") or "/" in entry["name"] or "\\" in entry["name"]:
            raise InputError("each suite entry needs a safe distinct name and file")
        if entry["name"] in names: raise InputError(f"duplicate suite name: {entry['name']}")
        names.add(entry["name"])
    output.mkdir(parents=True)
    summary: list[dict[str, Any]] = []
    overall = 0
    for entry in entries:
        case_path = (manifest_path.parent / entry["file"]).resolve()
        case_output = output / entry["name"]
        run_code, check_code, result, error = 2, 2, None, None
        try:
            run_code, actual_output, result = run_file(case_path, case_output)
            check_code, report = check_exit(actual_output)
            if check_code:
                error = report.get("error")
            elif run_code:
                error = result.get("error") if result else "business failure"
        except (InputError, OSError, json.JSONDecodeError) as exc:
            error = str(exc)
            case_output.mkdir(parents=True, exist_ok=True)
        rounds = result.get("rounds") if isinstance(result, dict) else None
        summary.append({"name": entry["name"], "scenario": str(case_path), "run_exit": run_code,
                        "check_exit": check_code, "rounds": rounds, "output": str(case_output), "error": error})
        if run_code == 2:
            overall = 2
        elif overall != 2 and (run_code == 1 or check_code == 1):
            overall = 1
        elif overall != 2 and check_code == 2 and run_code == 0:
            overall = 2
    report = {"format": 1, "entries": summary, "exit_code": overall}
    (output / "suite.json").write_bytes(canon(report))
    return overall, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic sharddb fault scenarios")
    parser.add_argument("--engine", default="sharddb.engine", help="kept for ABI-compatible invocation; built-in engine is used")
    parser.add_argument("--output", type=Path, help="new output directory")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run a scenario JSON file")
    run.add_argument("scenario", type=Path)
    run.add_argument("--output", dest="run_output", type=Path, help="new output directory")
    run.add_argument("--restore", type=Path, help="read-only settled checkpoint directory")
    run.add_argument("--checkpoint-out", type=Path, help="new directory for a settled checkpoint")
    check = sub.add_parser("check", help="offline consistency check of a run output")
    check.add_argument("run_output", type=Path)
    suite = sub.add_parser("suite", help="run isolated scenarios from a manifest")
    suite.add_argument("manifest", type=Path)
    suite.add_argument("--output", dest="suite_output", type=Path, required=True, help="new suite output directory")
    args = parser.parse_args(argv)
    if args.command == "check":
        code, report = check_exit(args.run_output)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return code
    if args.command == "suite":
        try:
            code, report = suite_file(args.manifest, args.suite_output)
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return code
        except (InputError, OSError, json.JSONDecodeError) as exc:
            print(f"host/input error: {exc}", file=sys.stderr); return 2
    try:
        code, output, result = run_file(args.scenario, args.run_output or args.output,
                                        restore=args.restore, checkpoint_out=args.checkpoint_out)
        print(json.dumps({"exit_code": code, "output": str(output), "rounds": result.get("rounds"), "success": result.get("success")}, ensure_ascii=False))
        return code
    except (InputError, OSError, json.JSONDecodeError) as exc:
        print(f"host/input error: {exc}", file=sys.stderr); return 2


if __name__ == "__main__":
    raise SystemExit(main())
