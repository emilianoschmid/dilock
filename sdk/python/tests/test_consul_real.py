"""Opt-in integration tests against an explicitly provisioned Consul namespace."""
from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest

from dilock import Options, ReleaseOutcome, UnsupportedCapability
from dilock.consul import ConsulProvider

ENDPOINT = os.environ.get("DILOCK_CONSUL_ENDPOINT")
NAMESPACE = os.environ.get("DILOCK_CONSUL_NAMESPACE")
EPOCH = os.environ.get("DILOCK_CONSUL_EPOCH")
TOKEN = os.environ.get("CONSUL_HTTP_TOKEN")
pytestmark = pytest.mark.skipif(
    not ENDPOINT or not NAMESPACE or not EPOCH,
    reason="DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH required",
)


@pytest.fixture
async def provider():
    async with ConsulProvider(ENDPOINT, NAMESPACE, EPOCH, token=TOKEN) as instance:
        yield instance


@pytest.mark.asyncio
async def test_real_contention_release_and_lockindex(provider):
    handle = provider.lock("real/" + uuid.uuid4().hex)
    options = Options(lease=10, rpc=2, cleanup=2, require_fencing=True)
    first = await handle.try_acquire(options)
    assert first is not None
    assert first.fence[0] == EPOCH
    assert first.fence[1] > 0
    first_fence = first.fence[1]
    assert await handle.try_acquire(options) is None
    assert await first.release() is ReleaseOutcome.RELEASED
    second = await handle.try_acquire(options)
    assert second is not None
    assert second.fence[1] > first_fence
    assert await second.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_real_session_renewal_and_stale_owner_cleanup(provider):
    handle = provider.lock("renew/" + uuid.uuid4().hex)
    options = Options(lease=10, rpc=2, cleanup=2)
    old = await handle.try_acquire(options)
    assert old is not None
    assert await old.renew()

    async with httpx.AsyncClient(
        base_url=ENDPOINT, headers={"X-Consul-Token": TOKEN} if TOKEN else {},
        follow_redirects=False,
    ) as client:
        renewed = await client.put(f"/v1/session/renew/{old._session_id}", timeout=2)
        assert renewed.status_code == 200
        assert renewed.json()[0]["ID"] == old._session_id
        destroyed = await client.put(f"/v1/session/destroy/{old._session_id}", timeout=2)
        assert destroyed.status_code == 200

    successor = await handle.try_acquire(options)
    assert successor is not None
    assert successor.fence[1] > old.fence[1]
    assert await old.release() is ReleaseOutcome.ALREADY_NOT_OWNED
    assert await successor._verify(2)
    assert await successor.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_consul_rejects_sub_ten_second_lease_before_io(provider):
    with pytest.raises(UnsupportedCapability):
        await provider.lock("short").try_acquire(Options(lease=9.99, rpc=.5))


async def _run_live_checks() -> None:
    if not ENDPOINT or not NAMESPACE or not EPOCH:
        raise RuntimeError("set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH")
    async with ConsulProvider(ENDPOINT, NAMESPACE, EPOCH, token=TOKEN) as instance:
        await test_real_contention_release_and_lockindex(instance)
        await test_real_session_renewal_and_stale_owner_cleanup(instance)
        await test_consul_rejects_sub_ten_second_lease_before_io(instance)


if __name__ == "__main__":
    asyncio.run(_run_live_checks())
