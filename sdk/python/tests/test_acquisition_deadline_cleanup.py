"""Late ownership evidence must not escape the wait or hide uncertain cleanup."""
import asyncio
from types import SimpleNamespace

import grpc
import pytest

from dilock import AcquisitionUnknown, Options, ReleaseOutcome, WaitTimeout
from dilock import etcd as etcd_module
from dilock import redis as redis_module


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (ReleaseOutcome.RELEASED, WaitTimeout),
        (ReleaseOutcome.ALREADY_NOT_OWNED, WaitTimeout),
        (ReleaseOutcome.UNKNOWN, AcquisitionUnknown),
    ],
)
async def test_redis_late_positive_acquisition_preserves_cleanup_uncertainty(
    monkeypatch: pytest.MonkeyPatch,
    outcome: ReleaseOutcome,
    expected: type[Exception],
) -> None:
    now = 100.0
    releases = 0
    attempts = 0

    # Replace only the adapter's clock; asyncio must retain its real clock.
    monkeypatch.setattr(redis_module, "time", SimpleNamespace(monotonic=lambda: now))

    class Lease:
        async def release(self) -> ReleaseOutcome:
            nonlocal releases
            releases += 1
            return outcome

    async def late_acquisition(self: object, options: Options, wait_deadline: float) -> Lease:
        nonlocal now, attempts
        attempts += 1
        assert options.rpc <= 0.5
        now = wait_deadline
        return Lease()

    monkeypatch.setattr(redis_module.RedisHandle, "_try_acquire", late_acquisition)
    handle = redis_module.RedisHandle(SimpleNamespace(), "key")

    with pytest.raises(expected):
        await handle.acquire(0.5, Options())
    assert attempts == 1
    assert releases == 1


@pytest.mark.asyncio
async def test_redis_cancelled_late_acquisition_cleanup_is_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0
    monkeypatch.setattr(redis_module, "time", SimpleNamespace(monotonic=lambda: now))

    class Lease:
        async def release(self) -> ReleaseOutcome:
            raise asyncio.CancelledError()

    async def late_acquisition(self: object, options: Options, wait_deadline: float) -> Lease:
        nonlocal now
        now = wait_deadline
        return Lease()

    monkeypatch.setattr(redis_module.RedisHandle, "_try_acquire", late_acquisition)
    handle = redis_module.RedisHandle(SimpleNamespace(), "key")
    with pytest.raises(AcquisitionUnknown, match="cleanup was cancelled"):
        await handle.acquire(0.5, Options())


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["lock", "verify", "shutdown", "rpc_deadline", "rpc_early"])
@pytest.mark.parametrize("cleanup_acknowledged", [True, False])
async def test_etcd_expired_acquisition_revokes_and_never_returns_a_lease(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    cleanup_acknowledged: bool,
) -> None:
    now = 100.0
    deadline = now + 0.5
    verification_budgets: list[float] = []
    revoked: list[int] = []

    class RpcFailure(Exception):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.DEADLINE_EXCEEDED

    monkeypatch.setattr(etcd_module, "time", SimpleNamespace(monotonic=lambda: now))
    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", RpcFailure)

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            nonlocal now
            verification_budgets.append(timeout)
            if phase in ("verify", "rpc_deadline"):
                now = deadline
            if phase in ("rpc_deadline", "rpc_early"):
                raise RpcFailure()
            return SimpleNamespace(
                count=1, kvs=[SimpleNamespace(lease=42, create_revision=17)],
            )

    class Leases:
        async def LeaseGrant(self, request: object, timeout: float) -> object:
            return SimpleNamespace(ID=42, TTL=3)

        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            revoked.append(request.ID)
            assert 0 < timeout <= 1
            if not cleanup_acknowledged:
                raise RpcFailure()
            return SimpleNamespace()

    class Locks:
        async def Lock(self, request: object, timeout: float) -> object:
            nonlocal now
            now = deadline if phase == "lock" else 100.25
            return SimpleNamespace(key=b"opaque-key")

    async def metadata(_: float) -> None:
        pass

    async def keeper(self: object) -> None:
        nonlocal now
        try:
            await asyncio.Future()
        finally:
            if phase == "shutdown":
                now = deadline

    # Let keepalive actually start so its cancellation can model scheduler delay.
    original_lock = Locks.Lock

    async def lock_after_keeper_started(self: object, request: object, timeout: float) -> object:
        await asyncio.sleep(0)
        return await original_lock(self, request, timeout)

    monkeypatch.setattr(Locks, "Lock", lock_after_keeper_started)
    monkeypatch.setattr(etcd_module.EtcdLease, "_keepalive", keeper)
    provider = SimpleNamespace(
        _closed=False, _metadata=metadata, epoch="epoch", _leases=set(),
        _kv=KV(), _leases_rpc=Leases(), _locks=Locks(),
    )
    handle = etcd_module.EtcdHandle(provider, b"resource")
    expected = WaitTimeout if cleanup_acknowledged and phase != "rpc_early" else AcquisitionUnknown
    with pytest.raises(expected):
        await handle.acquire(0.5, Options(lease=3, rpc=1, cleanup=1))

    assert revoked == [42]
    assert provider._leases == set()
    assert verification_budgets == ([] if phase == "lock" else [0.25])


@pytest.mark.asyncio
async def test_etcd_on_time_verification_returns_held_lease_with_remaining_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budgets: list[float] = []
    now = 100.0
    monkeypatch.setattr(etcd_module, "time", SimpleNamespace(monotonic=lambda: now))

    async def metadata(_: float) -> None:
        pass

    async def grant(request: object, timeout: float) -> object:
        return SimpleNamespace(ID=43, TTL=3)

    async def lock(request: object, timeout: float) -> object:
        nonlocal now
        now = 100.25
        return SimpleNamespace(key=b"opaque")

    async def verify(request: object, timeout: float) -> object:
        nonlocal now
        budgets.append(timeout)
        now = 100.4
        return SimpleNamespace(count=1, kvs=[SimpleNamespace(lease=43, create_revision=18)])

    async def keeper(self: object) -> None:
        await asyncio.Future()

    monkeypatch.setattr(etcd_module.EtcdLease, "_keepalive", keeper)
    provider = SimpleNamespace(
        _closed=False, _metadata=metadata, epoch="epoch", _leases=set(),
        _kv=SimpleNamespace(Range=verify), _leases_rpc=SimpleNamespace(LeaseGrant=grant),
        _locks=SimpleNamespace(Lock=lock),
    )
    lease = await etcd_module.EtcdHandle(provider, b"resource").acquire(
        0.5, Options(lease=3, rpc=1, auto_renew=False),
    )
    assert lease.state is etcd_module.State.HELD
    assert budgets == [0.25]
    assert lease.fence_revision == 18
    assert lease._keeper is None
