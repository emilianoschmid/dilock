"""Opt-in integration tests for an explicitly provisioned local etcd namespace."""
import asyncio
import os
import uuid

import pytest

from dilock import AcquireOptions, AcquisitionUnknown, Options, ProtocolMismatch, State

ENDPOINT = os.environ.get("DILOCK_ETCD_ENDPOINT")
NAMESPACE = os.environ.get("DILOCK_ETCD_NAMESPACE")
EPOCH = os.environ.get("DILOCK_ETCD_EPOCH")


@pytest.mark.asyncio
@pytest.mark.skipif(not all((ENDPOINT, NAMESPACE, EPOCH)), reason="no provisioned etcd integration namespace")
async def test_native_lock_keeper_fence_handoff_and_old_release() -> None:
    assert ENDPOINT and NAMESPACE and EPOCH
    from dilock.etcd import EtcdProvider
    first = EtcdProvider(ENDPOINT, NAMESPACE, EPOCH)
    second = EtcdProvider(ENDPOINT, NAMESPACE, EPOCH)
    resource = "python-etcd-test/" + uuid.uuid4().hex
    options = Options(lease=1, rpc=0.3, cleanup=1)
    try:
        old = await first.lock(resource).try_acquire(options)
        assert old is not None and old.fence[0] == EPOCH and old.fence_revision > 0
        assert await old.renew()
        wait_options = Options(lease=1, rpc=0.3, cleanup=1, auto_renew=True)
        waiting = asyncio.create_task(second.lock(resource).acquire(4, wait_options))
        await asyncio.sleep(1.3)  # waiter must keep its own lease alive past one TTL
        assert await old.release() == "released"
        successor = await waiting
        assert successor.fence_revision > old.fence_revision
        assert await old.release() == "released"
        assert await first.lock(resource).try_acquire(options) is None
        assert successor.state is State.HELD
        assert await successor.release() == "released"
    finally:
        await first.aclose()
        await second.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not all((ENDPOINT, NAMESPACE, EPOCH)), reason="no provisioned etcd integration namespace")
async def test_etcd_rejects_a_different_epoch() -> None:
    assert ENDPOINT and NAMESPACE and EPOCH
    from dilock.etcd import EtcdProvider
    provider = EtcdProvider(ENDPOINT, NAMESPACE, EPOCH + "-wrong")
    try:
        with pytest.raises(ProtocolMismatch):
            await provider.lock("wrong-epoch").try_acquire(Options(lease=3, rpc=1, cleanup=1))
    finally:
        await provider.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not all((ENDPOINT, NAMESPACE, EPOCH)), reason="no provisioned etcd integration namespace")
async def test_cancellation_revokes_a_queued_native_lock() -> None:
    assert ENDPOINT and NAMESPACE and EPOCH
    from dilock.etcd import EtcdProvider

    provider = EtcdProvider(ENDPOINT, NAMESPACE, EPOCH)
    resource = "python-etcd-cancel/" + uuid.uuid4().hex
    options = Options(lease=3, rpc=1, cleanup=1)
    try:
        handle = provider.lock(resource)
        owner = await handle.try_acquire(options)
        assert owner is not None
        queued = asyncio.create_task(handle.acquire(8, options))
        await asyncio.sleep(0.2)
        queued.cancel()
        with pytest.raises(AcquisitionUnknown):
            await queued
        assert await owner.release() == "released"
        successor = await handle.try_acquire(options)
        assert successor is not None
        assert await successor.release() == "released"
    finally:
        await provider.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not all((ENDPOINT, NAMESPACE, EPOCH)), reason="no provisioned etcd integration namespace")
async def test_hold_defaults_share_provider_and_preserve_body_error() -> None:
    assert ENDPOINT and NAMESPACE and EPOCH
    from dilock.etcd import EtcdProvider

    observations = []
    policy = AcquireOptions(
        wait=2.0,
        lease=Options(lease=3.0, rpc=.5, cleanup=.5, require_fencing=True),
    )
    provider = EtcdProvider(
        ENDPOINT, NAMESPACE, EPOCH, default_acquisition=policy,
        cleanup_observer=observations.append,
    )
    daily = provider.lock("python-quickstart/daily/" + uuid.uuid4().hex)
    weekly = provider.lock("python-quickstart/weekly/" + uuid.uuid4().hex)
    try:
        async with daily.hold() as first:
            async with weekly.hold() as second:
                assert first.state is second.state is State.HELD
                assert first.effective_lease_ms == second.effective_lease_ms == 3000
                assert first.fence[0] == second.fence[0] == EPOCH
        with pytest.raises(RuntimeError, match="body error"):
            async with daily.hold():
                raise RuntimeError("body error")
        assert not observations
    finally:
        await provider.aclose()
