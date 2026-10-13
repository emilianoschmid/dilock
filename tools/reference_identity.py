"""Reference encoder for Dilock draft-1 identity, not a lock implementation."""
from __future__ import annotations

import hashlib
import re
import secrets
import struct

_NAMESPACE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}", re.ASCII)
_DOMAIN = b"dilock.lock.identity\x00"
_MAX_RESOURCE_BYTES = 1024


def resource_id(namespace: str, resource: str) -> str:
    """Return the canonical lowercase SHA-256 ID; reject invalid input.

    Unicode is deliberately not normalized. Namespace/resource must be strings;
    resource is a non-empty strict UTF-8 scalar string with no NUL, <=1024 bytes.
    """
    if not isinstance(namespace, str) or _NAMESPACE.fullmatch(namespace) is None:
        raise ValueError("namespace must match [a-z0-9][a-z0-9_.-]{0,63}")
    if not isinstance(resource, str) or not resource or "\x00" in resource:
        raise ValueError("resource must be a non-empty string without NUL")
    try:
        ns = namespace.encode("ascii", "strict")
        name = resource.encode("utf-8", "strict")
    except UnicodeError as exc:
        raise ValueError("resource must contain Unicode scalar values") from exc
    if len(name) > _MAX_RESOURCE_BYTES:
        raise ValueError("resource exceeds 1024 UTF-8 bytes")
    framed = _DOMAIN + struct.pack(">I", len(ns)) + ns + struct.pack(">I", len(name)) + name
    return hashlib.sha256(framed).hexdigest()


def redis_key(namespace: str, resource: str) -> str:
    """Return the Redis draft-1 key, including literal hash-tag braces."""
    return "dilock:{" + resource_id(namespace, resource) + "}:lease"


def new_redis_owner_value() -> str:
    """Generate a fresh unpredictable acquisition value; never log it."""
    return "dilock/1/" + secrets.token_hex(32)
