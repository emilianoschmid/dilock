import asyncio
import json
import os
import uuid

import pytest
from redis.asyncio import Redis

from dilock import (
    AcquireOptions, Options, ProtocolMismatch, ReleaseOutcome, State, UnsupportedCapability, WaitTimeout,
)
from dilock.core import identity
from dilock.redis import RedisProvider

ENDPOINT = os.environ.get("DILOCK_REDIS_URL")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="DILOCK_REDIS_URL required: real backend")


@pytest.fixture
async def provider():
    ns = "pytest-" + uuid.uuid4().hex
    async with Redis.from_url(ENDPOINT) as client:
        await client.set("dilock:namespace:" + ns + ":meta", json.dumps(dict(
            protocol_major=1, identity_scheme="sha256-framed-utf8-v1",
            profile="redis-single-authority-v1", coordination_epoch="test",
            fixed_lease_ms=None)), nx=True)
    async with RedisProvider(ENDPOINT, ns, "test") as p:
        yield p


async def test_contention_expiry_stale_release(provider):
    handle = provider.lock("same")
    options = Options(lease=.4, rpc=.1)
    old = await handle.try_acquire(options)
    assert old is not None
    assert await handle.try_acquire(options) is None
    await asyncio.sleep(.5)
    assert old.state == State.LOST
    new = await handle.try_acquire(options)
    assert new is not None
    assert await old.release() == ReleaseOutcome.ALREADY_NOT_OWNED
    assert await old.renew() is False
    assert await handle.try_acquire(options) is None
    assert await new.release() == ReleaseOutcome.RELEASED
    assert await new.release() == ReleaseOutcome.RELEASED


async def test_wrong_type_is_reported_and_preserved(provider):
    resource = "wrong-type"
    key = f"dilock:{{{identity(provider.namespace, resource)}}}:lease"
    await provider._client.lpush(key, "preserve-me")

    with pytest.raises(ProtocolMismatch):
        await provider.lock(resource).try_acquire(Options(lease=1, rpc=.2))

    assert await provider._client.type(key) == b"list"
    assert await provider._client.lrange(key, 0, -1) == [b"preserve-me"]


async def test_renew_and_auto(provider):
    handle = provider.lock("renew")
    lease = await handle.try_acquire(Options(lease=.4, rpc=.1, auto_renew=True))
    assert lease is not None
    await asyncio.sleep(1)
    assert lease.state == State.HELD
    assert await handle.try_acquire(Options(lease=.4, rpc=.1)) is None
    await lease.release()
    assert await lease.renew() is False


async def test_timeout_cancel_fencing(provider):
    h = provider.lock("wait")
    lease = await h.try_acquire()
    with pytest.raises(WaitTimeout):
        await h.acquire(.15)
    task = asyncio.create_task(h.acquire(3))
    await asyncio.sleep(.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(UnsupportedCapability):
        await h.try_acquire(Options(require_fencing=True))
    await lease.release()


async def test_missing_metadata():
    async with RedisProvider(ENDPOINT, "missing-" + uuid.uuid4().hex, "test") as p:
        with pytest.raises(ProtocolMismatch):
            await p.lock("resource").try_acquire()


async def test_wrong_type_namespace_metadata_is_preserved(provider):
    key = f"dilock:namespace:{provider.namespace}:meta"
    await provider._client.delete(key)
    await provider._client.lpush(key, "preserve-metadata-list")
    try:
        with pytest.raises(ProtocolMismatch):
            await provider.lock("metadata-wrong-type").try_acquire()
        assert await provider._client.type(key) == b"list"
        assert await provider._client.lrange(key, 0, -1) == [b"preserve-metadata-list"]
    finally:
        await provider._client.delete(key)


async def test_hold_uses_provider_defaults_and_shared_client(provider):
    policy = AcquireOptions(wait=1.0, lease=Options(lease=3.0, rpc=.5, cleanup=.5))
    async with RedisProvider(
        ENDPOINT, provider.namespace, "test", default_acquisition=policy,
    ) as configured:
        daily = configured.lock("quickstart/daily")
        weekly = configured.lock("quickstart/weekly")
        client = configured._client
        async with daily.hold() as first:
            async with weekly.hold() as second:
                assert first.state == second.state == State.HELD
                assert first.effective_lease_ms == second.effective_lease_ms == 3000
                assert configured._client is client
        lease = await daily.try_acquire()
        assert lease is not None
        await lease.release()
