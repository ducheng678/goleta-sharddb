"""Event-driven transactional engine.

This module intentionally contains no filesystem, process, socket, or thread
API.  ``Node`` sees only the asynchronous ABI supplied by ``worker.py``.
The durable ``sharddb/v2`` record is a compact state machine: locks, unresolved
transactions, snapshots, and migration exports are all ordinary local state.
"""

from __future__ import annotations

import copy
from typing import Any, Callable

from sharddb.util import digest, is_int


ENGINE_KEY = "sharddb/v2"
MAINTENANCE_TOKEN = "maintenance"


def _empty() -> dict[str, Any]:
    return {"format": 2, "shards": {}, "coord_txns": {}, "coord_reads": {}, "plans": {}}


def _decision(txn: dict[str, Any], outcome: str) -> dict[str, Any]:
    return {"txn_id": txn["txn_id"], "request_digest": txn["digest"],
            "coordinator": txn["coordinator"], "outcome": outcome}


def _participants(topology: dict[str, Any], keys: list[str]) -> list[str]:
    return sorted({topology["key_shards"][key] for key in keys})


class Node:
    def __init__(self, event: dict[str, Any], host: Any) -> None:
        self.node_id = event["node_id"]
        self.host = host
        self.topology = event["topology"]
        self.state: dict[str, Any] | None = None
        self.revision = 0
        self.ready = False
        self.persisting = False
        self.reloading = False
        self.deferred: list[Callable[[], None]] = []
        self.actions: dict[str, Callable[[dict[str, Any]], None]] = {}
        self.tx_waiters: dict[str, list[str]] = {}
        self.local_waiters: dict[str, list[str]] = {}
        self.read_waiters: dict[str, str] = {}
        self.plan_waiters: dict[str, list[tuple[str, str, dict[str, Any]]]] = {}
        self.owners: dict[str, dict[str, Any]] = copy.deepcopy(self.topology["initial_owners"])
        self._track(host.read_local(), self._loaded)
        host.set_timer(MAINTENANCE_TOKEN, 30)

    # ----- asynchronous plumbing -------------------------------------------------
    def _track(self, action: str, callback: Callable[[dict[str, Any]], None]) -> None:
        self.actions[action] = callback

    def completion(self, event: dict[str, Any]) -> None:
        callback = self.actions.pop(event["action_id"], None)
        if callback is not None:
            callback(event["result"])

    def defer(self, fn: Callable[[], None]) -> None:
        self.deferred.append(fn)

    def pump(self) -> None:
        if not self.ready or self.persisting:
            return
        pending, self.deferred = self.deferred, []
        for fn in pending:
            if self.persisting:
                self.deferred.append(fn)
                continue
            fn()

    def persist(self, proposed: dict[str, Any], after: Callable[[], None] | None = None,
                decisions: list[dict[str, Any]] | None = None) -> None:
        if self.persisting:
            self.defer(lambda: self.persist(proposed, after, decisions))
            return
        assert self.state is not None
        self.persisting = True
        action = self.host.persist(self.revision, [{"op": "put", "key": ENGINE_KEY,
                                                     "value": proposed}], decisions or [])

        def done(result: dict[str, Any]) -> None:
            self.persisting = False
            if result["status"] == "STORED":
                self.state = proposed
                self.revision = result["revision"]
                if after:
                    after()
            else:
                # A conflict is only possible after an interrupted incarnation or
                # an externally supplied fixture.  Reloading is safer than replaying
                # a stale in-memory mutation.
                self._track(self.host.read_local(), self._loaded)
            self.pump()
        self._track(action, done)

    def _loaded(self, result: dict[str, Any]) -> None:
        image = result["image"]
        self.revision = image["revision"]
        candidate = image["data"].get(ENGINE_KEY)
        if isinstance(candidate, dict) and candidate.get("format") == 2:
            self.state = candidate
        else:
            self.state = self._from_baseline(image)
        self.ready = True
        self.persisting = False
        self.reloading = False
        self.recover_local_waiters()
        self.pump()

    # ----- v1 compatibility ------------------------------------------------------
    def _from_baseline(self, image: dict[str, Any]) -> dict[str, Any]:
        """Conservatively reconstruct the durable responsibilities v1 exposes."""
        out = _empty()
        base = image.get("data", {}).get("baseline/v1")
        if not isinstance(base, dict):
            return out
        decisions: dict[str, str] = {}
        for item in image.get("public_decisions", []):
            decision = item.get("decision", {})
            if decision.get("outcome") in ("COMMIT", "ABORT"):
                decisions[decision.get("txn_id", "")] = decision["outcome"]
        values = base.get("values", {})
        for shard, shard_values in values.items():
            epoch = self.topology["initial_owners"].get(shard, {}).get("epoch", 0)
            out["shards"][shard] = {"epoch": epoch, "values": copy.deepcopy(shard_values),
                                    "txns": {}, "write_lock": None, "read_locks": {},
                                    "history": {}, "frozen": None}
        for applied in base.get("applied", []):
            shard = applied.get("shard")
            if shard in out["shards"]:
                out["shards"][shard]["txns"][applied["txn_id"]] = {
                    "phase": "APPLIED", "delta": copy.deepcopy(applied["delta_map"]),
                    "digest": applied["request_digest"], "coordinator": applied["coordinator"]}
        for aborted in base.get("aborted", []):
            shard = aborted.get("shard")
            if shard in out["shards"]:
                out["shards"][shard]["txns"][aborted["txn_id"]] = {
                    "phase": "ABORTED", "delta": copy.deepcopy(aborted.get("delta_map", {})),
                    "digest": aborted["request_digest"], "coordinator": aborted["coordinator"]}
        for intent in base.get("intents", []):
            shard = intent.get("shard")
            if shard in out["shards"] and intent["txn_id"] not in out["shards"][shard]["txns"]:
                out["shards"][shard]["txns"][intent["txn_id"]] = {
                    "phase": "PREPARED", "delta": copy.deepcopy(intent["delta_map"]),
                    "digest": intent["request_digest"], "coordinator": intent["coordinator"]}
                out["shards"][shard]["write_lock"] = intent["txn_id"]
        for grant in base.get("read_grants", []):
            shard = grant.get("shard")
            if shard in out["shards"]:
                out["shards"][shard]["read_locks"][grant["read_id"]] = {
                    "keys": grant["keys"], "values": grant["values"]}
        for begin in base.get("txn_begins", []):
            # Only the cross coordinator owns multi-shard progress records.
            if begin.get("coordinator", {}).get("type") == "worker":
                outcome = decisions.get(begin["txn_id"])
                out["coord_txns"][begin["txn_id"]] = {
                    "txn_id": begin["txn_id"], "delta": begin["delta_map"],
                    "participants": begin["participants"], "digest": begin["request_digest"],
                    "coordinator": begin["coordinator"],
                    "phase": "COMMIT" if outcome == "COMMIT" else
                             "ABORT" if outcome == "ABORT" else "PREPARING",
                    "prepared": [], "applied": []}
        for migration in base.get("migrations", []):
            shard, mid = migration.get("shard"), migration.get("migration_id")
            if shard not in ("A", "B", "C") or not isinstance(mid, str):
                continue
            expected = {shard: {"owner": migration.get("source"), "epoch": migration.get("source_epoch"),
                                "migration_id": None}}
            replacement = {shard: {"owner": migration.get("target"), "epoch": migration.get("target_epoch"),
                                   "migration_id": mid}}
            out["plans"][mid] = {"id": mid, "kind": "migrate", "moves": {shard: migration.get("target")},
                "expected": expected, "replacement": replacement, "leader": migration.get("source"),
                "status": "PUBLISHED" if migration.get("state") == "COMPLETE" else "START",
                "ready": [shard] if migration.get("state") in ("SETTLED", "COMPLETE") else [], "installed": []}
            if migration.get("role") == "SOURCE" and migration.get("state") == "DRAINING" and shard in out["shards"]:
                out["shards"][shard]["frozen"] = mid
            # A SETTLED target has an independently durable v1 export.  It is
            # enough to resume responsibility even if its old values map was
            # intentionally empty in the checkpoint.
            settled = migration.get("settled_image")
            if migration.get("role") == "TARGET" and isinstance(settled, dict) and shard not in out["shards"]:
                out["shards"][shard] = {"epoch": migration.get("target_epoch", 0),
                    "values": copy.deepcopy(settled.get("values", {})), "txns": {}, "write_lock": None,
                    "read_locks": {}, "history": {}, "frozen": None}
            if migration.get("state") == "COMPLETE" and shard in out["shards"] and migration.get("role") == "TARGET":
                out["shards"][shard]["epoch"] = migration.get("target_epoch", 0)
        return out

    # ----- shared helpers ---------------------------------------------------------
    def owner_lookup(self, shards: list[str], after: Callable[[dict[str, dict[str, Any]]], None]) -> None:
        action = self.host.read_owners(sorted(shards))
        def done(result: dict[str, Any]) -> None:
            if result["status"] == "OK":
                self.owners.update(result["records"])
                after(result["records"])
        self._track(action, done)

    def send(self, peer: str, body: dict[str, Any]) -> None:
        self.host.send(peer, body)

    def reply(self, token: str, payload: dict[str, Any]) -> None:
        self.host.reply(token, payload)

    def shard_state(self, shard: str) -> dict[str, Any] | None:
        assert self.state is not None
        return self.state["shards"].get(shard)

    def local_coordinator(self, participants: list[str]) -> dict[str, str]:
        if len(participants) == 1:
            return {"type": "shard", "id": participants[0]}
        return {"type": "worker", "id": self.topology["cross_shard_coordinator"]}

    # ----- client requests --------------------------------------------------------
    def client(self, event: dict[str, Any]) -> None:
        if not self.ready or self.persisting:
            self.defer(lambda: self.client(event))
            return
        request, token = event["request"], event["request_token"]
        op = request["op"]
        if op in ("txn", "conditional_txn"):
            keys = list(request["delta_map"])
            if op == "conditional_txn":
                keys.extend(request["expected"])
            parts = _participants(self.topology, keys)
            if len(parts) == 1:
                self.local_txn(request, token, parts[0])
            else:
                self.cross_txn(request, token, parts)
        elif op == "status":
            self.status(request, token)
        elif op == "snapshot":
            parts = _participants(self.topology, request["keys"])
            if len(parts) == 1:
                self.local_snapshot(request, token, parts[0])
            else:
                self.cross_snapshot(request, token, parts)
        elif op in ("migrate", "rebalance"):
            self.plan_request(event)

    def local_txn(self, request: dict[str, Any], token: str, shard: str) -> None:
        if self.persisting:
            self.defer(lambda: self.local_txn(request, token, shard)); return
        st = self.shard_state(shard)
        if st is None or st.get("frozen"):
            self.reply(token, {"txn_id": request["txn_id"], "status": "UNKNOWN"}); return
        tid = request["txn_id"]
        old = st["txns"].get(tid)
        if old and old["phase"] == "APPLIED":
            self.reply(token, {"txn_id": tid, "status": "COMMITTED"}); return
        if old and old["phase"] == "ABORTED":
            self.reply(token, {"txn_id": tid, "status": "ABORTED"}); return
        if st["write_lock"] not in (None, tid) or st["read_locks"]:
            self.reply(token, {"txn_id": tid, "status": "UNKNOWN"}); return
        proposed = copy.deepcopy(self.state)
        ps = proposed["shards"][shard]
        conditional = request["op"] == "conditional_txn"
        condition_ok = not conditional or all(ps["values"].get(key) == value
                                                for key, value in request["expected"].items())
        if condition_ok:
            for key, amount in request["delta_map"].items():
                ps["values"][key] = ps["values"].get(key, 0) + amount
        txn = {"phase": "APPLIED" if condition_ok else "ABORTED", "delta": copy.deepcopy(request["delta_map"]),
               "digest": digest(request), "coordinator": {"type": "shard", "id": shard}}
        ps["txns"][tid] = txn
        decision = {"txn_id": tid, "request_digest": txn["digest"],
                    "coordinator": txn["coordinator"], "outcome": "COMMIT" if condition_ok else "ABORT"}
        self.local_waiters.setdefault(tid, []).append(token)
        self.persist(proposed, lambda: self.finish_local(tid),
                     [decision])

    def finish_local(self, tid: str) -> None:
        if not self.state: return
        status: str | None = None
        for st in self.state["shards"].values():
            tx = st["txns"].get(tid)
            if tx and tx["phase"] == "APPLIED": status = "COMMITTED"
            elif tx and tx["phase"] == "ABORTED": status = "ABORTED"
        if status:
            for token in self.local_waiters.pop(tid, []):
                self.reply(token, {"txn_id": tid, "status": status})

    def recover_local_waiters(self) -> None:
        for tid in list(self.local_waiters): self.finish_local(tid)

    def cross_txn(self, request: dict[str, Any], token: str, parts: list[str]) -> None:
        if self.persisting:
            self.defer(lambda: self.cross_txn(request, token, parts)); return
        tid = request["txn_id"]
        record = self.state["coord_txns"].get(tid)  # type: ignore[index]
        if record:
            if record["phase"] == "COMMITTED":
                self.reply(token, {"txn_id": tid, "status": "COMMITTED"})
            elif record["phase"] == "ABORTED":
                self.reply(token, {"txn_id": tid, "status": "ABORTED"})
            else:
                self.tx_waiters.setdefault(tid, []).append(token)
                self.drive_txn(tid)
            return
        proposed = copy.deepcopy(self.state)
        proposed["coord_txns"][tid] = {"txn_id": tid, "op": request["op"], "delta": copy.deepcopy(request["delta_map"]),
            "expected": copy.deepcopy(request.get("expected", {})),
            "participants": parts, "digest": digest(request),
            "coordinator": {"type": "worker", "id": self.node_id}, "phase": "PREPARING",
            "prepared": [], "applied": []}
        self.tx_waiters.setdefault(tid, []).append(token)
        self.persist(proposed, lambda: self.drive_txn(tid))

    def status(self, request: dict[str, Any], token: str) -> None:
        tid = request["txn_id"]
        record = self.state["coord_txns"].get(tid) if self.state else None
        if record and record["phase"] in ("COMMITTED", "ABORTED"):
            self.reply(token, {"txn_id": tid, "status": record["phase"]}); return
        if self.state:
            for st in self.state["shards"].values():
                tx = st["txns"].get(tid)
                if tx and tx["phase"] == "APPLIED":
                    self.reply(token, {"txn_id": tid, "status": "COMMITTED"}); return
                if tx and tx["phase"] == "ABORTED":
                    self.reply(token, {"txn_id": tid, "status": "ABORTED"}); return
        self.reply(token, {"txn_id": tid, "status": "UNKNOWN"})

    def local_snapshot(self, request: dict[str, Any], token: str, shard: str) -> None:
        st = self.shard_state(shard)
        if st is None or st.get("frozen") or st["write_lock"]:
            self.reply(token, {"read_id": request["read_id"], "status": "UNKNOWN"}); return
        values = {key: st["values"][key] for key in request["keys"]}
        self.reply(token, {"read_id": request["read_id"], "status": "OK", "values": values})

    # ----- cross-shard write protocol --------------------------------------------
    def drive_txn(self, tid: str) -> None:
        if self.persisting or not self.state:
            self.defer(lambda: self.drive_txn(tid)); return
        tx = self.state["coord_txns"].get(tid)
        if not tx:
            return
        phase = tx["phase"]
        if phase == "PREPARING":
            self.owner_lookup(tx["participants"], lambda owners: self._send_prepares(tid, owners))
        elif phase in ("COMMIT", "ABORT", "COMMITTING"):
            self.owner_lookup(tx["participants"], lambda owners: self._send_decisions(tid, owners))
        elif phase == "COMMITTED":
            self.finish_txn(tid, "COMMITTED")
        elif phase == "ABORTED":
            self.finish_txn(tid, "ABORTED")

    def _send_prepares(self, tid: str, owners: dict[str, dict[str, Any]]) -> None:
        tx = self.state["coord_txns"].get(tid) if self.state else None
        if not tx or tx["phase"] != "PREPARING": return
        for shard in tx["participants"]:
            delta = {key: value for key, value in tx["delta"].items()
                     if self.topology["key_shards"][key] == shard}
            expected = {key: value for key, value in tx.get("expected", {}).items()
                        if self.topology["key_shards"][key] == shard}
            self.send(owners[shard]["owner"], {"type": "PREPARE", "shard": shard,
                "txn_id": tid, "delta": delta, "expected": expected, "digest": tx["digest"],
                "coordinator": tx["coordinator"]})

    def _send_decisions(self, tid: str, owners: dict[str, dict[str, Any]]) -> None:
        tx = self.state["coord_txns"].get(tid) if self.state else None
        if not tx or tx["phase"] not in ("COMMIT", "ABORT", "COMMITTING"): return
        outcome = "COMMIT" if tx["phase"] in ("COMMIT", "COMMITTING") else "ABORT"
        for shard in tx["participants"]:
            self.send(owners[shard]["owner"], {"type": "DECISION", "shard": shard,
                "txn_id": tid, "outcome": outcome, "digest": tx["digest"],
                "coordinator": tx["coordinator"]})

    def prepare(self, body: dict[str, Any]) -> None:
        shard, tid = body["shard"], body["txn_id"]
        st = self.shard_state(shard)
        if st is None or st.get("frozen"):
            return
        old = st["txns"].get(tid)
        if old and old["phase"] in ("PREPARED", "APPLIED"):
            self.send(body["coordinator"]["id"], {"type": "PREPARED", "shard": shard, "txn_id": tid}); return
        if old and old["phase"] == "ABORTED":
            self.send(body["coordinator"]["id"], {"type": "ABORTED", "shard": shard, "txn_id": tid}); return
        if st["write_lock"] not in (None, tid) or st["read_locks"]:
            self.send(body["coordinator"]["id"], {"type": "BUSY", "shard": shard, "txn_id": tid}); return
        if any(st["values"].get(key) != value for key, value in body.get("expected", {}).items()):
            self.send(body["coordinator"]["id"], {"type": "CONDITION_FALSE", "shard": shard, "txn_id": tid}); return
        proposed = copy.deepcopy(self.state)
        ps = proposed["shards"][shard]
        ps["write_lock"] = tid
        ps["txns"][tid] = {"phase": "PREPARED", "delta": body["delta"],
                             "digest": body["digest"], "coordinator": body["coordinator"]}
        self.persist(proposed, lambda: self.send(body["coordinator"]["id"],
                    {"type": "PREPARED", "shard": shard, "txn_id": tid}))

    def prepared_reply(self, body: dict[str, Any]) -> None:
        tid, shard = body["txn_id"], body["shard"]
        tx = self.state["coord_txns"].get(tid) if self.state else None
        if not tx or tx["phase"] != "PREPARING": return
        if body["type"] == "BUSY" and tx.get("op") == "conditional_txn":
            # Do not convert a lock conflict into a condition result.  Retrying
            # keeps already compared shards locked until every comparison can be
            # evaluated at one protected transaction point.
            return
        if body["type"] in ("BUSY", "ABORTED", "CONDITION_FALSE"):
            self.decide(tid, "ABORT"); return
        if shard in tx["prepared"]: return
        proposed = copy.deepcopy(self.state)
        proposed["coord_txns"][tid]["prepared"].append(shard)
        def after() -> None:
            if len(self.state["coord_txns"][tid]["prepared"]) == len(tx["participants"]):
                self.decide(tid, "COMMIT")
        self.persist(proposed, after)

    def decide(self, tid: str, outcome: str) -> None:
        if self.persisting:
            self.defer(lambda: self.decide(tid, outcome)); return
        tx = self.state["coord_txns"].get(tid) if self.state else None
        if not tx or tx["phase"] not in ("PREPARING",): return
        proposed = copy.deepcopy(self.state)
        proposed["coord_txns"][tid]["phase"] = outcome
        self.persist(proposed, lambda: self.drive_txn(tid), [_decision(tx, outcome)])

    def decision(self, body: dict[str, Any]) -> None:
        shard, tid, outcome = body["shard"], body["txn_id"], body["outcome"]
        st = self.shard_state(shard)
        if st is None or st.get("frozen"):
            return
        old = st["txns"].get(tid)
        if old and old["phase"] == "APPLIED":
            self.send(body["coordinator"]["id"], {"type": "APPLIED", "shard": shard, "txn_id": tid}); return
        if old and old["phase"] == "ABORTED":
            self.send(body["coordinator"]["id"], {"type": "ABORTED", "shard": shard, "txn_id": tid}); return
        if outcome == "COMMIT" and not old:
            # It is a real decision but this incarnation has not recovered the
            # prepare yet; coordinator maintenance will resend safely.
            return
        proposed = copy.deepcopy(self.state)
        ps = proposed["shards"][shard]
        if outcome == "COMMIT":
            for key, amount in old["delta"].items():
                ps["values"][key] = ps["values"].get(key, 0) + amount
            ps["txns"][tid]["phase"] = "APPLIED"
            ps["write_lock"] = None
            ack = "APPLIED"
        else:
            ps["txns"][tid] = {"phase": "ABORTED", "delta": old["delta"] if old else {},
                                "digest": body["digest"], "coordinator": body["coordinator"]}
            if ps["write_lock"] == tid: ps["write_lock"] = None
            ack = "ABORTED"
        txfact = {"txn_id": tid, "digest": body["digest"], "coordinator": body["coordinator"]}
        self.persist(proposed, lambda: self.send(body["coordinator"]["id"],
                    {"type": ack, "shard": shard, "txn_id": tid}), [_decision(txfact, outcome)])

    def applied_reply(self, body: dict[str, Any]) -> None:
        tid, shard = body["txn_id"], body["shard"]
        tx = self.state["coord_txns"].get(tid) if self.state else None
        if not tx or tx["phase"] not in ("COMMIT", "ABORT", "COMMITTING"): return
        if shard in tx["applied"]: return
        proposed = copy.deepcopy(self.state)
        proposed["coord_txns"][tid]["applied"].append(shard)
        def after() -> None:
            now = self.state["coord_txns"][tid]
            if len(now["applied"]) == len(now["participants"]):
                final = "COMMITTED" if now["phase"] in ("COMMIT", "COMMITTING") else "ABORTED"
                p = copy.deepcopy(self.state); p["coord_txns"][tid]["phase"] = final
                self.persist(p, lambda: self.finish_txn(tid, final))
        self.persist(proposed, after)

    def finish_txn(self, tid: str, status: str) -> None:
        for token in self.tx_waiters.pop(tid, []):
            self.reply(token, {"txn_id": tid, "status": status})

    # ----- cross-shard read protocol ---------------------------------------------
    def cross_snapshot(self, request: dict[str, Any], token: str, parts: list[str]) -> None:
        if self.persisting:
            self.defer(lambda: self.cross_snapshot(request, token, parts)); return
        rid = request["read_id"]
        existing = self.state["coord_reads"].get(rid)  # type: ignore[index]
        if existing and existing["phase"] == "CLOSED" and existing.get("values") is not None:
            self.reply(token, {"read_id": rid, "status": "OK", "values": existing["values"]}); return
        if existing:
            self.reply(token, {"read_id": rid, "status": "UNKNOWN"}); return
        proposed = copy.deepcopy(self.state)
        proposed["coord_reads"][rid] = {"read_id": rid, "keys": list(request["keys"]),
            "participants": parts, "digest": digest({"op": "snapshot", "read_id": rid,
                                                        "keys": sorted(request["keys"])}),
            "phase": "OPEN", "grants": [], "values": {}}
        self.read_waiters[rid] = token
        self.persist(proposed, lambda: self.drive_read(rid))

    def drive_read(self, rid: str) -> None:
        if self.persisting or not self.state:
            self.defer(lambda: self.drive_read(rid)); return
        read = self.state["coord_reads"].get(rid)
        if not read:
            return
        if read["phase"] == "OPEN":
            self.owner_lookup(read["participants"], lambda owners: self._send_grants(rid, owners))
        elif read["phase"] == "CLOSED":
            # A successful read is allowed to reply before all of its release
            # messages have reached the shards.  The durable CLOSED record is
            # therefore also a durable release responsibility.  In particular,
            # recovery must not treat CLOSED as terminal for maintenance: a
            # dropped release otherwise leaves a persistent reader lock behind.
            self.owner_lookup(read["participants"],
                              lambda owners: self._release_read(rid, owners, read.get("values")))

    def _send_grants(self, rid: str, owners: dict[str, dict[str, Any]]) -> None:
        read = self.state["coord_reads"].get(rid) if self.state else None
        if not read or read["phase"] != "OPEN": return
        coordinator = {"type": "worker", "id": self.node_id}
        for shard in read["participants"]:
            keys = [key for key in read["keys"] if self.topology["key_shards"][key] == shard]
            self.send(owners[shard]["owner"], {"type": "READ_GRANT", "shard": shard,
                "read_id": rid, "keys": keys, "digest": read["digest"], "coordinator": coordinator})

    def read_grant(self, body: dict[str, Any]) -> None:
        st = self.shard_state(body["shard"])
        if st is None or st.get("frozen"): return
        rid = body["read_id"]
        if rid in st["read_locks"]:
            self.send(body["coordinator"]["id"], {"type": "GRANTED", "shard": body["shard"],
                "read_id": rid, "values": st["read_locks"][rid]["values"]}); return
        if st["write_lock"]:
            self.send(body["coordinator"]["id"], {"type": "READ_BUSY", "shard": body["shard"], "read_id": rid}); return
        proposed = copy.deepcopy(self.state)
        ps = proposed["shards"][body["shard"]]
        values = {key: ps["values"][key] for key in body["keys"]}
        ps["read_locks"][rid] = {"keys": body["keys"], "values": values}
        self.persist(proposed, lambda: self.send(body["coordinator"]["id"],
                    {"type": "GRANTED", "shard": body["shard"], "read_id": rid, "values": values}))

    def granted_reply(self, body: dict[str, Any]) -> None:
        rid, shard = body["read_id"], body["shard"]
        read = self.state["coord_reads"].get(rid) if self.state else None
        if not read or read["phase"] != "OPEN": return
        if body["type"] == "READ_BUSY":
            self.close_read(rid, None); return
        if shard in read["grants"]: return
        proposed = copy.deepcopy(self.state)
        pr = proposed["coord_reads"][rid]; pr["grants"].append(shard); pr["values"].update(body["values"])
        def after() -> None:
            now = self.state["coord_reads"][rid]
            if len(now["grants"]) == len(now["participants"]):
                self.close_read(rid, now["values"])
        self.persist(proposed, after)

    def close_read(self, rid: str, values: dict[str, int] | None) -> None:
        if self.persisting:
            self.defer(lambda: self.close_read(rid, values)); return
        read = self.state["coord_reads"].get(rid) if self.state else None
        if not read or read["phase"] == "CLOSED": return
        proposed = copy.deepcopy(self.state)
        proposed["coord_reads"][rid]["phase"] = "CLOSED"; proposed["coord_reads"][rid]["values"] = values
        def after() -> None:
            self.owner_lookup(read["participants"], lambda owners: self._release_read(rid, owners, values))
        self.persist(proposed, after)

    def _release_read(self, rid: str, owners: dict[str, dict[str, Any]], values: dict[str, int] | None) -> None:
        read = self.state["coord_reads"].get(rid) if self.state else None
        if not read: return
        released = read.get("released", {})
        coordinator = {"type": "worker", "id": self.node_id}
        for shard in read["grants"]:
            # An acknowledgement belongs to an owner *epoch*, not to a worker
            # forever.  A handoff can happen after an old owner accepts a
            # release but before its copied lock is published at the target.
            # Looking up on every maintenance pass makes that old acknowledgement
            # expire when the authoritative epoch changes.
            owner = owners[shard]
            confirmed = isinstance(released, dict) and released.get(shard) == owner["epoch"]
            if not confirmed:
                self.send(owners[shard]["owner"], {"type": "READ_RELEASE", "shard": shard,
                                                    "read_id": rid, "epoch": owner["epoch"],
                                                    "coordinator": coordinator})
        token = self.read_waiters.pop(rid, None)
        if token:
            if values is None: self.reply(token, {"read_id": rid, "status": "UNKNOWN"})
            else: self.reply(token, {"read_id": rid, "status": "OK", "values": values})

    def read_release(self, body: dict[str, Any]) -> None:
        st = self.shard_state(body["shard"])
        coordinator = body.get("coordinator", {})
        epoch = body.get("epoch")

        def acknowledge() -> None:
            # Acknowledge even an already released duplicate.  The coordinator
            # can then durably retire this retry responsibility; a late release
            # is a no-op and cannot recreate a reader lock.
            if isinstance(coordinator, dict) and coordinator.get("type") == "worker" and \
                    isinstance(coordinator.get("id"), str):
                self.send(coordinator["id"], {"type": "READ_RELEASED", "shard": body["shard"],
                                               "read_id": body["read_id"], "epoch": epoch})

        # New releases are generation-scoped.  A delayed release from a source
        # epoch must not clear a read lock transferred to a newer owner.
        if st and is_int(epoch) and st.get("epoch") != epoch:
            return
        if not st or body["read_id"] not in st["read_locks"]:
            acknowledge(); return
        proposed = copy.deepcopy(self.state)
        proposed["shards"][body["shard"]]["read_locks"].pop(body["read_id"], None)
        self.persist(proposed, acknowledge)

    def read_released_reply(self, body: dict[str, Any]) -> None:
        rid, shard, epoch = body["read_id"], body["shard"], body.get("epoch")
        read = self.state["coord_reads"].get(rid) if self.state else None
        if not read or read.get("phase") != "CLOSED" or shard not in read.get("grants", []):
            return
        if not is_int(epoch):
            return
        released = read.get("released", {})
        if isinstance(released, dict) and released.get(shard) == epoch:
            return
        proposed = copy.deepcopy(self.state)
        # Older v2 records had no acknowledgement information (and an
        # intermediate development revision used a list).  Treat both as
        # unconfirmed and upgrade on the next real acknowledgement.
        old = proposed["coord_reads"][rid].get("released")
        proposed["coord_reads"][rid]["released"] = dict(old) if isinstance(old, dict) else {}
        proposed["coord_reads"][rid]["released"][shard] = epoch
        self.persist(proposed)

    # ----- transfer and atomic owner-vector plans --------------------------------
    def plan_request(self, event: dict[str, Any]) -> None:
        request, token = event["request"], event["request_token"]
        if request["op"] == "rebalance":
            meta = event["rebalance"]; pid = meta["plan_id"]; kind = "rebalance"
            moves, expected, replacement = request["moves"], meta["expected"], meta["replacement"]
        else:
            pid = event["migration_id"]; kind = "migrate"; moves = {request["shard"]: request["target"]}
            expected = replacement = None
        old = self.state["plans"].get(pid) if self.state else None
        if not old and self.state:
            # Completed plans travel with a shard's durable state.  This is what
            # lets a retry of an old plan be answered after a still later move.
            for st in self.state["shards"].values():
                historical = st.get("history", {}).get(pid)
                if historical:
                    old = historical
                    break
        if old and old["status"] in ("PUBLISHED", "COMPLETE"):
            self.reply_plan(token, kind, request, old); return
        self.plan_waiters.setdefault(pid, []).append((token, kind, request))
        if old:
            self.resume_plan(pid); return
        if kind == "migrate":
            # Legacy migrate has only migration_id in its public event.  The
            # source obtains the complete registered source record by an ABI
            # metadata read and deterministically reconstructs its replacement.
            self.owner_lookup([request["shard"]], lambda records: self._new_legacy_plan(
                pid, request, records[request["shard"]]))
            return
        leader = self.node_id
        proposed = copy.deepcopy(self.state)
        proposed["plans"][pid] = {"id": pid, "kind": kind, "moves": moves,
            "expected": expected, "replacement": replacement, "leader": leader,
            "status": "START", "ready": [], "installed": []}
        self.persist(proposed, lambda: self.resume_plan(pid))

    def _new_legacy_plan(self, pid: str, request: dict[str, Any], expected: dict[str, Any]) -> None:
        if not self.state or pid in self.state["plans"]: return
        shard = request["shard"]
        # A completed target that has lost only its local progress record will
        # still get a retry through copied plan history; a newly derived record
        # is only used while this migration is genuinely at its old source.
        replacement = {"owner": request["target"], "epoch": expected["epoch"] + 1,
                       "migration_id": pid}
        proposed = copy.deepcopy(self.state)
        proposed["plans"][pid] = {"id": pid, "kind": "migrate", "moves": {shard: request["target"]},
            "expected": {shard: expected}, "replacement": {shard: replacement}, "leader": self.node_id,
            "status": "START", "ready": [], "installed": []}
        self.persist(proposed, lambda: self.resume_plan(pid))

    def plan_template(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"id": body["plan_id"], "kind": body["kind"], "moves": body["moves"],
                "expected": body["expected"], "replacement": body["replacement"],
                "leader": body["leader"], "status": "START", "ready": [], "installed": []}

    def resume_plan(self, pid: str) -> None:
        if self.persisting or not self.state:
            self.defer(lambda: self.resume_plan(pid)); return
        plan = self.state["plans"].get(pid)
        if not plan: return
        if plan["status"] in ("PUBLISHED", "COMPLETE"):
            self.finish_plan(pid); return
        if self.node_id == plan["leader"]:
            if set(plan["ready"]) == set(plan["moves"]):
                # CAS may have committed before its completion was lost.
                self.plan_cas(pid)
                return
            peers = {record["owner"] for record in plan["expected"].values()}
            for peer in peers:
                self.send(peer, {"type": "PLAN_START", "plan_id": pid, "kind": plan["kind"],
                    "moves": plan["moves"], "expected": plan["expected"],
                    "replacement": plan["replacement"], "leader": plan["leader"]})
        self.become_plan_source(pid)

    def plan_start(self, body: dict[str, Any]) -> None:
        pid = body["plan_id"]
        old = self.state["plans"].get(pid) if self.state else None
        if old:
            self.become_plan_source(pid); return
        proposed = copy.deepcopy(self.state)
        proposed["plans"][pid] = self.plan_template(body)
        self.persist(proposed, lambda: self.become_plan_source(pid))

    def become_plan_source(self, pid: str) -> None:
        if self.persisting or not self.state:
            self.defer(lambda: self.become_plan_source(pid)); return
        plan = self.state["plans"].get(pid)
        if not plan or plan["status"] in ("PUBLISHED", "COMPLETE"): return
        source_shards = [shard for shard, rec in plan["expected"].items() if rec["owner"] == self.node_id]
        available = [shard for shard in source_shards if self.shard_state(shard)]
        pending = [shard for shard in available if self.shard_state(shard).get("frozen") != pid]
        if not pending:
            # A crash may happen after freezing is durable but before the send
            # action executes or its completion arrives.  Frozen state is an
            # immutable export, so sending it again is the recovery protocol.
            for shard in available:
                st = self.shard_state(shard)
                if st and st.get("frozen") == pid:
                    self.send(plan["replacement"][shard]["owner"], {"type": "PLAN_INSTALL",
                        "plan_id": pid, "kind": plan["kind"], "moves": plan["moves"],
                        "expected": plan["expected"], "replacement": plan["replacement"],
                        "leader": plan["leader"], "shard": shard, "state": copy.deepcopy(st)})
            return
        proposed = copy.deepcopy(self.state)
        exports: dict[str, dict[str, Any]] = {}
        for shard in pending:
            proposed["shards"][shard]["frozen"] = pid
            # Carry historical completion answers along every later handoff.
            proposed["shards"][shard].setdefault("history", {})
            for old_id, old_plan in proposed["plans"].items():
                if old_plan.get("status") in ("PUBLISHED", "COMPLETE"):
                    proposed["shards"][shard]["history"][old_id] = copy.deepcopy(old_plan)
            exports[shard] = copy.deepcopy(proposed["shards"][shard])
        def after() -> None:
            for shard, exported in exports.items():
                self.send(plan["replacement"][shard]["owner"], {"type": "PLAN_INSTALL",
                    "plan_id": pid, "kind": plan["kind"], "moves": plan["moves"],
                    "expected": plan["expected"], "replacement": plan["replacement"],
                    "leader": plan["leader"], "shard": shard, "state": exported})
        self.persist(proposed, after)

    def plan_install(self, body: dict[str, Any]) -> None:
        pid, shard = body["plan_id"], body["shard"]
        if body["replacement"][shard]["owner"] != self.node_id: return
        oldplan = self.state["plans"].get(pid) if self.state else None
        proposed = copy.deepcopy(self.state)
        if not oldplan:
            proposed["plans"][pid] = self.plan_template(body)
        plan = proposed["plans"][pid]
        incoming_epoch = body["replacement"][shard]["epoch"]
        current = proposed["shards"].get(shard)
        if current and current.get("epoch", 0) > incoming_epoch:
            # Authentic late installation from an older plan; never roll state back.
            self.send(body["leader"], {"type": "PLAN_READY", "plan_id": pid, "shard": shard}); return
        if shard not in plan["installed"]:
            imported = copy.deepcopy(body["state"]); imported["epoch"] = incoming_epoch; imported["frozen"] = None
            imported.setdefault("history", {})
            proposed["shards"][shard] = imported; plan["installed"].append(shard)
        self.persist(proposed, lambda: self.send(body["leader"],
                     {"type": "PLAN_READY", "plan_id": pid, "shard": shard}))

    def plan_ready(self, body: dict[str, Any]) -> None:
        pid, shard = body["plan_id"], body["shard"]
        plan = self.state["plans"].get(pid) if self.state else None
        if not plan or self.node_id != plan["leader"] or plan["status"] in ("PUBLISHED", "COMPLETE"): return
        if shard in plan["ready"]: return
        proposed = copy.deepcopy(self.state); proposed["plans"][pid]["ready"].append(shard)
        def after() -> None:
            now = self.state["plans"][pid]
            if set(now["ready"]) == set(now["moves"]): self.plan_cas(pid)
        self.persist(proposed, after)

    def plan_cas(self, pid: str) -> None:
        plan = self.state["plans"].get(pid) if self.state else None
        if not plan or plan["status"] in ("PUBLISHED", "COMPLETE"): return
        if plan["kind"] == "migrate":
            shard = next(iter(plan["moves"])); action = self.host.cas_owner(shard, plan["expected"][shard], plan["replacement"][shard])
        else:
            action = self.host.cas_owners(plan["expected"], plan["replacement"])
        self._track(action, lambda result: self.plan_cas_result(pid, result))

    def plan_cas_result(self, pid: str, result: dict[str, Any]) -> None:
        plan = self.state["plans"].get(pid) if self.state else None
        if not plan: return
        records = result.get("records") or ({next(iter(plan["moves"])): result.get("record")} if result.get("record") else {})
        if result["status"] in ("SWAPPED", "UNCHANGED") or records == plan["replacement"]:
            proposed = copy.deepcopy(self.state); proposed["plans"][pid]["status"] = "PUBLISHED"
            def after() -> None:
                for peer in {record["owner"] for record in plan["replacement"].values()}:
                    self.send(peer, {"type": "PLAN_PUBLISHED", "plan_id": pid})
                self.finish_plan(pid)
            self.persist(proposed, after)

    def plan_published(self, body: dict[str, Any]) -> None:
        pid = body["plan_id"]
        plan = self.state["plans"].get(pid) if self.state else None
        if not plan or plan["status"] in ("PUBLISHED", "COMPLETE"): return
        proposed = copy.deepcopy(self.state); proposed["plans"][pid]["status"] = "PUBLISHED"
        self.persist(proposed, lambda: self.finish_plan(pid))

    def reply_plan(self, token: str, kind: str, request: dict[str, Any], plan: dict[str, Any]) -> None:
        if kind == "rebalance":
            self.reply(token, {"plan_id": request["plan_id"], "status": "COMPLETE", "owners": plan["replacement"]})
        else:
            shard = request["shard"]
            self.reply(token, {"shard": shard, "target": request["target"], "status": "COMPLETE",
                               "epoch": plan["replacement"][shard]["epoch"]})

    def finish_plan(self, pid: str) -> None:
        if not self.state: return
        plan = self.state["plans"].get(pid)
        if not plan or plan["status"] not in ("PUBLISHED", "COMPLETE"): return
        for token, kind, request in self.plan_waiters.pop(pid, []): self.reply_plan(token, kind, request, plan)

    # ----- maintenance and wire dispatch -----------------------------------------
    def maintenance(self) -> None:
        self.host.set_timer(MAINTENANCE_TOKEN, 30)
        if not self.ready or not self.state: return
        if self.persisting:
            # A completed persistence action can lose its completion.  A fresh
            # ABI read is the durable source of truth; it also prevents an
            # otherwise live incarnation from waiting forever for a callback.
            if not self.reloading:
                self.reloading = True
                self._track(self.host.read_local(), self._loaded)
            return
        for tid in list(self.state["coord_txns"]): self.drive_txn(tid)
        for rid in list(self.state["coord_reads"]): self.drive_read(rid)
        for pid in list(self.state["plans"]): self.resume_plan(pid)

    def message(self, body: Any) -> None:
        if not isinstance(body, dict) or "type" not in body or not self.ready:
            return
        if self.persisting:
            self.defer(lambda: self.message(body))
            return
        typ = body["type"]
        if typ == "PREPARE": self.prepare(body)
        elif typ in ("PREPARED", "BUSY", "CONDITION_FALSE"): self.prepared_reply(body)
        elif typ == "DECISION": self.decision(body)
        elif typ in ("APPLIED", "ABORTED"): self.applied_reply(body)
        elif typ == "READ_GRANT": self.read_grant(body)
        elif typ in ("GRANTED", "READ_BUSY"): self.granted_reply(body)
        elif typ == "READ_RELEASE": self.read_release(body)
        elif typ == "READ_RELEASED": self.read_released_reply(body)
        elif typ == "PLAN_START": self.plan_start(body)
        elif typ == "PLAN_INSTALL": self.plan_install(body)
        elif typ == "PLAN_READY": self.plan_ready(body)
        elif typ == "PLAN_PUBLISHED": self.plan_published(body)

    def event(self, event: dict[str, Any]) -> None:
        kind = event["kind"]
        if kind == "completion": self.completion(event)
        elif kind == "client_request": self.client(event)
        elif kind == "message": self.message(event["body"])
        elif kind == "timer" and event["token"] == MAINTENANCE_TOKEN: self.maintenance()


NODE: Node | None = None


def on_event(event: dict[str, Any], host: Any) -> None:
    global NODE
    if event["kind"] == "boot":
        NODE = Node(event, host)
        return
    if NODE is not None:
        NODE.host = host
        NODE.event(event)
