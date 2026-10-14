"""Host validation for Rust actor identities and bounded integer JSON."""

import hashlib
import json
import re

from .contracts import RunnerError


JSON_BYTES = 65_536
ENCODE_ENVELOPE_BYTES = JSON_BYTES + 256
_ACTOR_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}")


def is_actor_identity(value: object) -> bool:
    """Match the Rust actor grammar; protocol admission remains authoritative."""
    return type(value) is str and _ACTOR_IDENTITY.fullmatch(value) is not None


class JsonSizeError(RunnerError):
    """Canonical JSON exceeds the caller's explicit byte bound."""


def integer_json(value: object, limit: int = JSON_BYTES) -> str:
    """Reject Python coercions before handing JSON to the authoritative codec."""
    def check(item: object, depth: int) -> None:
        if depth > 32:
            raise RunnerError("JSON nesting limit exceeded")
        if item is None or type(item) in (str, bool):
            return
        if type(item) is int:
            if len(str(abs(item))) > 1024:
                raise RunnerError("JSON integer limit exceeded")
            return
        if type(item) is list:
            for child in item:
                check(child, depth + 1)
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise RunnerError("JSON object keys must be strings")
                check(child, depth + 1)
            return
        raise RunnerError("protocol data must use integer-only JSON")

    try:
        check(value, 0)
        source = json.dumps(value, ensure_ascii=False, allow_nan=False,
                            separators=(",", ":"))
        if len(source.encode("utf-8")) > limit:
            raise JsonSizeError("JSON byte limit exceeded")
        return source
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise RunnerError("invalid protocol JSON") from None


def canonical_json(value: object, limit: int = JSON_BYTES) -> str:
    """Return bounded integer JSON with sorted keys and unescaped Unicode.

    Validation precedes serialization so Python keys, floats and subclasses
    cannot be coerced into a different value. The returned UTF-8 representation
    preserves the existing host descriptor encoding used for durable identities.
    Callers translate ``RunnerError`` into their own boundary's diagnostic.
    """
    checked = json.loads(integer_json(value, limit))
    return json.dumps(checked, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_identity(value: object, limit: int = JSON_BYTES) -> str:
    """SHA-256 identity of ``canonical_json``'s bounded UTF-8 representation."""
    return hashlib.sha256(canonical_json(value, limit).encode("utf-8")).hexdigest()
