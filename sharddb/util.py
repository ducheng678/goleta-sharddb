"""Shared strict-JSON and canonical-encoding helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any


class InputError(ValueError):
    """A scenario or wire value is not in the public JSON subset."""


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def loads_strict(text: str) -> Any:
    try:
        value = json.loads(text, object_pairs_hook=_no_duplicates,
                           parse_constant=lambda value: (_ for _ in ()).throw(
                               InputError(f"invalid JSON constant {value}")))
    except json.JSONDecodeError as exc:
        raise InputError(f"invalid JSON: {exc.msg}") from exc
    validate_json(value)
    return value


def validate_json(value: Any, path: str = "$") -> None:
    if value is None or isinstance(value, (str, bool)):
        if isinstance(value, str) and any(0xD800 <= ord(c) <= 0xDFFF for c in value):
            raise InputError(f"isolated surrogate in string at {path}")
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        raise InputError(f"floats are forbidden at {path}")
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_json(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise InputError(f"non-string object key at {path}")
            validate_json(key, f"{path}.<key>")
            validate_json(item, f"{path}.{key}")
        return
    raise InputError(f"unsupported JSON value at {path}: {type(value).__name__}")


def canon(value: Any) -> bytes:
    validate_json(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canon(value)).hexdigest()


def is_id(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def exact_keys(obj: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(obj, dict) or set(obj) != keys:
        got = sorted(obj) if isinstance(obj, dict) else type(obj).__name__
        raise InputError(f"{label} must have exactly {sorted(keys)} (got {got})")
    return obj
