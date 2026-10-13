"""Local-only API example: no Redis, TTL, renewal, fencing or wire protocol."""
from __future__ import annotations
import asyncio
from enum import Enum
from types import TracebackType
from typing import Protocol, Self

class ReleaseOutcome(Enum):
    RELEASED = "released"
    ALREADY_NOT_OWNED = "already_not_owned"

class OwnershipStore(Protocol):
    """The deliberately tiny local test port; not a production backend API."""
    async def claim(self, resource: str, owner: object) -> bool: ...
    async def release(self, resource: str, owner: object) -> ReleaseOutcome: ...

class MemoryStore:
    """Single-event-loop store; an asyncio lock serializes task access."""
    def __init__(self) -> None:
        self._owners: dict[str, object] = {}
        self._gate = asyncio.Lock()

    async def claim(self, resource: str, owner: object) -> bool:
        async with self._gate:
            if resource in self._owners:
                return False
            self._owners[resource] = owner
            return True

    async def release(self, resource: str, owner: object) -> ReleaseOutcome:
        async with self._gate:
            if self._owners.get(resource) is not owner:
                return ReleaseOutcome.ALREADY_NOT_OWNED
            del self._owners[resource]
            return ReleaseOutcome.RELEASED

    async def expire_for_test(self, resource: str) -> None:
        """Test hook only. No corresponding public force-unlock API is proposed."""
        async with self._gate:
            self._owners.pop(resource, None)

class Lease:
    """One acquisition; closed leases cannot be re-entered or reused."""
    def __init__(self, store: OwnershipStore, resource: str, owner: object) -> None:
        self._store, self._resource, self._owner = store, resource, owner
        self._closed = False
        self._gate = asyncio.Lock()

    async def release(self) -> ReleaseOutcome:
        async with self._gate:
            if self._closed:
                return ReleaseOutcome.ALREADY_NOT_OWNED
            # For a remote backend this transition and ambiguous cleanup need
            # the full state machine, independent cleanup deadline and tracking.
            self._closed = True
            return await self._store.release(self._resource, self._owner)

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("lease is closed")
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, tb: TracebackType | None) -> None:
        await self.release()

class ResourceLock:
    """A reusable resource reference, not ownership of that resource."""
    def __init__(self, store: OwnershipStore, resource: str) -> None:
        if not resource:
            raise ValueError("resource must not be empty")
        self._store, self._resource = store, resource

    async def try_acquire(self) -> Lease | None:
        owner = object()  # Local identity only, NOT a production wire token.
        if not await self._store.claim(self._resource, owner):
            return None
        return Lease(self._store, self._resource, owner)

class Provider:
    """Composition: the provider borrows the injected local store."""
    def __init__(self, store: OwnershipStore) -> None:
        self._store = store

    def lock(self, resource: str) -> ResourceLock:
        return ResourceLock(self._store, resource)

async def self_test() -> None:
    store = MemoryStore()
    lock = Provider(store).lock("example/resource")
    first = await lock.try_acquire()
    assert first is not None
    assert await lock.try_acquire() is None
    await store.expire_for_test("example/resource")
    second = await lock.try_acquire()
    assert second is not None
    assert await first.release() is ReleaseOutcome.ALREADY_NOT_OWNED
    assert await lock.try_acquire() is None
    assert await second.release() is ReleaseOutcome.RELEASED
    assert await second.release() is ReleaseOutcome.ALREADY_NOT_OWNED
    try:
        lease = await lock.try_acquire()
        assert lease is not None
        async with lease:
            raise ValueError("consumer failure")
    except ValueError:
        pass
    final = await lock.try_acquire()
    assert final is not None
    await final.release()
    try:
        Provider(store).lock("")
        raise AssertionError("empty resource accepted")
    except ValueError:
        pass
    print("PASS python: busy, stale owner, idempotence, scope cleanup, validation")

if __name__ == "__main__":
    asyncio.run(self_test())
