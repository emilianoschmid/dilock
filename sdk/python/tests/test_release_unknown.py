import asyncio
import time

import pytest

from dilock import Options, ReleaseOutcome, State
from dilock.redis import RedisLease


@pytest.mark.asyncio
async def test_cancelled_release_caches_unknown_and_does_not_retry() -> None:
    backend_started = asyncio.Event()
    backend_finish = asyncio.Event()

    class Provider:
        def __init__(self) -> None:
            self._leases: set[RedisLease] = set()
            self.calls = 0
            self.cleanup_task: asyncio.Task[ReleaseOutcome] | None = None

        async def _cleanup_owner(self, key: str, owner: bytes, budget: float) -> ReleaseOutcome:
            self.calls += 1

            async def execute() -> ReleaseOutcome:
                backend_started.set()
                await backend_finish.wait()
                return ReleaseOutcome.RELEASED

            self.cleanup_task = asyncio.create_task(execute())
            return await asyncio.shield(self.cleanup_task)

    provider = Provider()
    lease = RedisLease(provider, "key", b"owner", Options(), time.monotonic() + 30)
    provider._leases.add(lease)

    release_task = asyncio.create_task(lease.release())
    await backend_started.wait()
    release_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await release_task

    assert lease.state is State.RELEASED_UNKNOWN
    assert await lease.release() is ReleaseOutcome.UNKNOWN
    assert provider.calls == 1
    assert lease not in provider._leases

    backend_finish.set()
    assert provider.cleanup_task is not None
    assert await provider.cleanup_task is ReleaseOutcome.RELEASED
