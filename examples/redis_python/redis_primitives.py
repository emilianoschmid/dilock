"""Small Redis command adapter, not a complete distributed lease manager.

It sends real Redis commands through an injected async client and shares the
canonical Lua/identity reference. It deliberately does not turn an acknowledgement
into a valid lease, implement retries, renew scheduling or classify transport loss.
The enclosing lifecycle engine must perform all of those safety checks.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import re
import sys
from time import monotonic
from typing import Protocol

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.reference_identity import redis_key, new_redis_owner_value

_RELEASE = (ROOT / "spec/backends/redis-release.lua").read_text(encoding="utf-8")
_RENEW = (ROOT / "spec/backends/redis-renew.lua").read_text(encoding="utf-8")
_OWNER = re.compile(r"dilock/1/[0-9a-f]{64}", re.ASCII)

class AsyncRedisCommands(Protocol):
    """Narrow port implemented, for example, by redis.asyncio.Redis."""
    async def execute_command(self, *args: object) -> object: ...

@dataclass(frozen=True, slots=True)
class AcquisitionIdentity:
    key: str
    owner: str = field(repr=False)  # Owner credentials must not leak through repr.

@dataclass(frozen=True, slots=True)
class ClaimReply:
    """Command acknowledgement only: not permission to execute protected work."""
    accepted: bool
    elapsed_seconds: float

class RedisPrimitiveAdapter:
    """Borrow a client configured with bounded I/O and no hidden mutation retries.

    Client setup is caller-owned. Exceptions deliberately propagate: the lifecycle
    layer must classify possibly committed operations as outcome-unknown, not busy.
    No method retries or falls back to another coordinator.
    """
    def __init__(self, client: AsyncRedisCommands) -> None:
        self._client = client

    @staticmethod
    def new_identity(namespace: str, resource: str) -> AcquisitionIdentity:
        return AcquisitionIdentity(redis_key(namespace, resource), new_redis_owner_value())

    @staticmethod
    def _validate(identity: AcquisitionIdentity, ttl_ms: int | None = None) -> None:
        if not re.fullmatch(r"dilock:\{[0-9a-f]{64}\}:lease", identity.key):
            raise ValueError("invalid canonical Redis key")
        if not _OWNER.fullmatch(identity.owner):
            raise ValueError("invalid acquisition owner")
        if ttl_ms is not None and (type(ttl_ms) is not int or not 1 <= ttl_ms <= 2_147_483_647):
            raise ValueError("ttl_ms must be an integer in 1..2147483647")

    async def claim(self, identity: AcquisitionIdentity, ttl_ms: int) -> ClaimReply:
        self._validate(identity, ttl_ms)
        started = monotonic()
        reply = await self._client.execute_command("SET", identity.key, identity.owner, "NX", "PX", ttl_ms)
        # Redis clients may expose SET's OK as bytes, string or boolean.
        if reply is not None and reply is not True and reply != b"OK" and reply != "OK":
            raise RuntimeError("unexpected Redis SET response")
        return ClaimReply(reply is not None, monotonic() - started)

    async def release(self, identity: AcquisitionIdentity) -> bool:
        self._validate(identity)
        reply = await self._client.execute_command("EVAL", _RELEASE, 1, identity.key, identity.owner)
        if type(reply) is not int or reply not in (0, 1):
            raise RuntimeError("unexpected Redis release response")
        return reply == 1

    async def renew(self, identity: AcquisitionIdentity, ttl_ms: int) -> bool:
        self._validate(identity, ttl_ms)
        reply = await self._client.execute_command("EVAL", _RENEW, 1, identity.key, identity.owner, ttl_ms)
        if type(reply) is not int or reply not in (0, 1):
            raise RuntimeError("unexpected Redis renew response")
        return reply == 1
