"""Offline evidence checker for sharddb development runs.

It never starts workers and never writes into the supplied run directory.
The finite serial-history search is deliberately small because the public
workload bound is at most six writes and eight successful snapshots.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import InputError, digest, is_int, loads_strict


class ConsistencyFailure(ValueError):
    pass


def _load(path: Path) -> Any:
    if not path.is_file():
        raise InputError(f"missing evidence file: {path.name}")
    return loads_strict(path.read_text(encoding="utf-8"))


def _txn_shape(request: dict[str, Any], response: dict[str, Any]) -> None:
    if set(response) != {"txn_id", "status"} or response["txn_id"] != request["txn_id"] or response["status"] not in ("COMMITTED", "ABORTED", "UNKNOWN"):
        raise ConsistencyFailure(f"malformed transaction response for {request['txn_id']}")


def _snapshot_shape(request: dict[str, Any], response: dict[str, Any]) -> None:
    if response.get("status") == "UNKNOWN":
        if set(response) != {"read_id", "status"} or response["read_id"] != request["read_id"]:
            raise ConsistencyFailure(f"malformed UNKNOWN snapshot {request['read_id']}")
    elif response.get("status") == "OK":
        if set(response) != {"read_id", "status", "values"} or response["read_id"] != request["read_id"] or not isinstance(response["values"], dict) or set(response["values"]) != set(request["keys"]) or any(not is_int(value) for value in response["values"].values()):
            raise ConsistencyFailure(f"malformed successful snapshot {request['read_id']}")
    else:
        raise ConsistencyFailure(f"invalid snapshot status {request['read_id']}")


def _other_shape(request: dict[str, Any], response: dict[str, Any]) -> None:
    op = request["op"]
    if op == "migrate":
        fields = {"shard", "target", "status"} if response.get("status") == "UNKNOWN" else {"shard", "target", "status", "epoch"}
        if set(response) != fields or response.get("shard") != request["shard"] or response.get("target") != request["target"] or response.get("status") not in ("COMPLETE", "UNKNOWN"):
            raise ConsistencyFailure("malformed migration response")
    elif op == "rebalance":
        fields = {"plan_id", "status"} if response.get("status") == "UNKNOWN" else {"plan_id", "status", "owners"}
        if set(response) != fields or response.get("plan_id") != request["plan_id"] or response.get("status") not in ("COMPLETE", "UNKNOWN"):
            raise ConsistencyFailure("malformed rebalance response")


@dataclass
class Op:
    name: str
    start: int
    end: int
    kind: str
    request: dict[str, Any]
    response: dict[str, Any]


def _participants(request: dict[str, Any], mapping: dict[str, str]) -> set[str]:
    keys = set(request["delta_map"])
    if request["op"] == "conditional_txn": keys.update(request["expected"])
    return {mapping[key] for key in keys}


def _authoritative_value(image: dict[str, Any], shard: str, key: str) -> Any:
    """Read an untouched shard from either current or public v1 storage."""
    state = image.get("data", {}).get("sharddb/v2")
    if isinstance(state, dict) and shard in state.get("shards", {}):
        return state["shards"][shard].get("values", {}).get(key)
    baseline = image.get("data", {}).get("baseline/v1", {})
    if isinstance(baseline, dict):
        return baseline.get("values", {}).get(shard, {}).get(key)
    return None


def _apply_or_read(op: Op, values: dict[str, int]) -> dict[str, int] | None:
    request, response = op.request, op.response
    if op.kind == "snapshot":
        if {key: values[key] for key in request["keys"]} != response["values"]:
            return None
        return values
    if op.kind == "conditional_abort":
        return values if any(values[key] != expected for key, expected in request["expected"].items()) else None
    if request["op"] == "conditional_txn":
        if any(values[key] != expected for key, expected in request["expected"].items()):
            return None
    next_values = copy.deepcopy(values)
    for key, amount in request["delta_map"].items(): next_values[key] += amount
    return next_values


def _serializable(operations: list[Op], initial: dict[str, int]) -> tuple[bool, list[str] | None]:
    if len(operations) > 14:
        raise InputError("evidence exceeds public finite serial-history limit")
    predecessors: dict[str, set[str]] = {op.name: set() for op in operations}
    for first in operations:
        for second in operations:
            if first.name != second.name and first.end <= second.start:
                predecessors[second.name].add(first.name)
    by_name = {op.name: op for op in operations}

    def visit(done: set[str], values: dict[str, int], order: list[str]) -> list[str] | None:
        if len(done) == len(operations): return order
        ready = [op for op in operations if op.name not in done and predecessors[op.name] <= done]
        # Reads first dramatically prunes impossible half-transaction snapshots.
        ready.sort(key=lambda op: (0 if op.kind == "snapshot" else 1, op.end, op.name))
        for op in ready:
            next_values = _apply_or_read(op, values)
            if next_values is None: continue
            result = visit(done | {op.name}, next_values, order + [op.name])
            if result is not None: return result
        return None

    order = visit(set(), copy.deepcopy(initial), [])
    return order is not None, order


def check_run(run_dir: Path) -> dict[str, Any]:
    evidence = _load(run_dir / "evidence.json")
    if not isinstance(evidence, dict) or set(evidence) != {"format", "initial_keys", "topology", "calls", "responses", "owners"} or evidence["format"] != 1:
        raise InputError("unsupported or insufficient evidence format")
    keys, topology, calls, responses, owners = (evidence["initial_keys"], evidence["topology"],
                                                  evidence["calls"], evidence["responses"], evidence["owners"])
    if not isinstance(keys, dict) or not keys or any(not is_int(value) for value in keys.values()):
        raise InputError("invalid initial value evidence")
    if not isinstance(topology, dict) or not isinstance(topology.get("key_shards"), dict) or set(topology["key_shards"]) != set(keys):
        raise InputError("invalid topology evidence")
    if not isinstance(calls, list) or not isinstance(responses, list) or not isinstance(owners, dict):
        raise InputError("calls, responses, or owners evidence is malformed")
    calls_by_token: dict[str, dict[str, Any]] = {}
    for call in calls:
        if not isinstance(call, dict) or set(call) != {"round", "token", "label", "target", "request"} or not is_int(call["round"]) or not isinstance(call["token"], str) or call["token"] in calls_by_token or not isinstance(call["request"], dict):
            raise InputError("malformed request-call evidence")
        calls_by_token[call["token"]] = call
    response_by_token: dict[str, dict[str, Any]] = {}
    for row in responses:
        if not isinstance(row, dict) or set(row) != {"round", "token", "label", "request", "response"} or not is_int(row["round"]) or row["token"] in response_by_token:
            raise InputError("malformed response evidence")
        call = calls_by_token.get(row["token"])
        if not call or call["request"] != row["request"] or row["round"] < call["round"]:
            raise InputError("response has no matching request invocation evidence")
        response_by_token[row["token"]] = row
    if set(calls_by_token) != set(response_by_token):
        raise InputError("incomplete response evidence; cannot check unfinished attempts")

    logical: dict[str, dict[str, Any]] = {}
    snapshots: list[Op] = []
    plan_results: dict[str, dict[str, Any]] = {}
    migration_results: dict[tuple[str, str], int] = {}
    for token, call in calls_by_token.items():
        request, response = call["request"], response_by_token[token]["response"]
        op = request.get("op")
        if op in ("txn", "conditional_txn", "status"):
            _txn_shape(request, response)
        elif op == "snapshot":
            _snapshot_shape(request, response)
        elif op in ("migrate", "rebalance"):
            _other_shape(request, response)
            if op == "rebalance" and response["status"] == "COMPLETE":
                previous = plan_results.setdefault(request["plan_id"], response["owners"])
                if previous != response["owners"]:
                    raise ConsistencyFailure(f"plan {request['plan_id']} has inconsistent COMPLETE owner vectors")
            if op == "migrate" and response["status"] == "COMPLETE":
                key = (request["shard"], request["target"])
                previous = migration_results.setdefault(key, response["epoch"])
                if previous != response["epoch"]:
                    raise ConsistencyFailure(f"migration {key!r} has inconsistent COMPLETE epochs")
        else:
            raise InputError("unknown operation in evidence")
        if op in ("txn", "conditional_txn"):
            tid = request["txn_id"]
            existing = logical.setdefault(tid, {"request": request, "calls": [], "terminal": None})
            if existing["request"] != request: raise ConsistencyFailure(f"txn_id {tid} has conflicting requests")
            existing["calls"].append((call, response))
            if response["status"] in ("COMMITTED", "ABORTED"):
                if existing["terminal"] and existing["terminal"][1]["status"] != response["status"]:
                    raise ConsistencyFailure(f"txn_id {tid} has conflicting terminal responses")
                end = response_by_token[token]["round"]
                if existing["terminal"] is None or end < existing["terminal"][2]:
                    existing["terminal"] = (call, response, end)
        elif op == "snapshot" and response["status"] == "OK":
            snapshots.append(Op(f"read:{request['read_id']}", call["round"], response_by_token[token]["round"], "snapshot", request, response))

    # status is permitted only for an already begun ID and may not disagree with
    # the logical terminal outcome.
    for token, call in calls_by_token.items():
        if call["request"].get("op") == "status":
            tid = call["request"]["txn_id"]
            if tid not in logical: raise ConsistencyFailure(f"status references unbegun txn {tid}")
            terminal = logical[tid]["terminal"]
            status = response_by_token[token]["response"]["status"]
            if terminal and status != terminal[1]["status"]:
                raise ConsistencyFailure(f"status disagrees with terminal txn {tid}")
            if status in ("COMMITTED", "ABORTED") and terminal is None:
                logical[tid]["terminal"] = (call, response_by_token[token]["response"], response_by_token[token]["round"])

    decisions: dict[str, set[tuple[str, str]]] = {}
    images: dict[str, dict[str, Any]] = {}
    for shard, owner in owners.items():
        if shard not in ("A", "B", "C") or not isinstance(owner, dict) or not isinstance(owner.get("owner"), str):
            raise InputError("malformed final owner evidence")
    for worker in topology.get("workers", []):
        image = _load(run_dir / "stores" / worker / "image.json")
        images[worker] = image
        for item in image.get("public_decisions", []):
            decision = item.get("decision", {})
            if decision.get("outcome") in ("COMMIT", "ABORT") and isinstance(decision.get("txn_id"), str):
                decisions.setdefault(decision["txn_id"], set()).add((decision.get("request_digest"), decision["outcome"]))
    writes: list[Op] = []
    for tid, item in logical.items():
        terminal = item["terminal"]
        if terminal is None:
            raise InputError(f"no terminal response evidence for logical txn {tid}")
        request, response = item["request"], terminal[1]
        facts = decisions.get(tid, set())
        expected_fact = (digest(request), "COMMIT" if response["status"] == "COMMITTED" else "ABORT")
        if facts != {expected_fact}:
            raise ConsistencyFailure(f"public decisions for {tid} are missing, conflicting, or use the wrong digest")
        start = min(call["round"] for call, _ in item["calls"])
        if response["status"] == "COMMITTED":
            writes.append(Op(f"txn:{tid}", start, terminal[2], "write", request, response))
        elif request["op"] == "conditional_txn":
            writes.append(Op(f"txn:{tid}", start, terminal[2], "conditional_abort", request, response))
        # Final authoritative records demonstrate one application per logical
        # participant (including a zero-delta condition-only shard).
        for shard in _participants(request, topology["key_shards"]):
            owner = owners[shard]["owner"]
            state = images[owner].get("data", {}).get("sharddb/v2")
            if not isinstance(state, dict) or shard not in state.get("shards", {}):
                raise InputError(f"authoritative shard state unavailable for {shard}")
            record = state["shards"][shard].get("txns", {}).get(tid)
            wanted = "APPLIED" if response["status"] == "COMMITTED" else "ABORTED"
            if not isinstance(record, dict) or record.get("phase") != wanted:
                raise ConsistencyFailure(f"transaction {tid} lacks exactly-once {wanted} record on {shard}")

    complete = writes + snapshots
    serializable, order = _serializable(complete, keys)
    if not serializable:
        raise ConsistencyFailure("no common strict serial history explains committed writes and successful snapshots")
    final_values = copy.deepcopy(keys)
    for op in writes:
        # Additive final values are commutative; condition validity was already
        # checked at the operation's chosen point in the successful full search.
        if op.kind == "write":
            for key, amount in op.request["delta_map"].items():
                final_values[key] += amount
    # The serial order can differ from this writes-only order when snapshots
    # constrain it, but write/write ordering does not change additive values.
    for key, expected in final_values.items():
        shard = topology["key_shards"][key]
        owner = owners[shard]["owner"]
        if _authoritative_value(images[owner], shard, key) != expected:
            raise ConsistencyFailure(f"final authoritative value for {key} is not exactly-once logical result")
    return {"ok": True, "operations": len(complete), "writes": len(writes), "snapshots": len(snapshots),
            "serial_order": order, "message": "evidence passes response, decision, exactly-once, and serial-history checks"}


def check_exit(run_dir: Path) -> tuple[int, dict[str, Any]]:
    try:
        return 0, check_run(run_dir)
    except ConsistencyFailure as exc:
        return 1, {"ok": False, "error": str(exc), "kind": "consistency"}
    except (InputError, OSError, KeyError, TypeError) as exc:
        return 2, {"ok": False, "error": str(exc), "kind": "evidence"}
