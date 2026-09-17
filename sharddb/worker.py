"""Isolated worker executable used by the development host.

The protocol deliberately has only line-delimited JSON over stdin/stdout.  The
engine never sees a filesystem path, another worker, or a process handle.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from .engine.node import on_event


class HostProxy:
    def __init__(self) -> None:
        self.node = ""
        self.incarnation = 0
        self.sequence = 0

    def begin(self, node: str, incarnation: int) -> None:
        self.node = node
        self.incarnation = incarnation

    def _call(self, op: str, args: dict[str, Any]) -> str:
        self.sequence += 1
        action_id = f"{self.node}:{self.incarnation}:{self.sequence}"
        print(json.dumps({"kind": "action", "action_id": action_id, "op": op,
                          "args": args}, ensure_ascii=False, separators=(",", ":")),
              flush=True)
        return action_id

    def send(self, peer: str, body: Any) -> str:
        return self._call("send", {"peer": peer, "body": body})

    def set_timer(self, token: str, rounds: int) -> str:
        return self._call("set_timer", {"token": token, "rounds": rounds})

    def read_local(self) -> str:
        return self._call("read_local", {})

    def persist(self, expected_revision: int, edits: list[dict[str, Any]],
                public_decisions: list[dict[str, Any]]) -> str:
        return self._call("persist", {"expected_revision": expected_revision,
                                        "edits": edits,
                                        "public_decisions": public_decisions})

    def read_owner(self, shard: str) -> str:
        return self._call("read_owner", {"shard": shard})

    def read_owners(self, shards: list[str]) -> str:
        return self._call("read_owners", {"shards": shards})

    def cas_owner(self, shard: str, expected: dict[str, Any], replacement: dict[str, Any]) -> str:
        return self._call("cas_owner", {"shard": shard, "expected": expected,
                                         "replacement": replacement})

    def cas_owners(self, expected: dict[str, Any], replacement: dict[str, Any]) -> str:
        return self._call("cas_owners", {"expected": expected, "replacement": replacement})

    def reply(self, request_token: str, payload: dict[str, Any]) -> str:
        return self._call("reply", {"request_token": request_token, "payload": payload})


def main() -> int:
    host = HostProxy()
    for raw in sys.stdin:
        command = json.loads(raw)
        if command.get("kind") != "event":
            continue
        event = command["event"]
        host.begin(event["node_id"], event["incarnation"])
        on_event(event, host)
        print(json.dumps({"kind": "done", "event_id": event["event_id"]},
                         separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
