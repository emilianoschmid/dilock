"""Regression coverage for transport security and lease failure handling."""
import asyncio
import json
import time
from types import SimpleNamespace

import grpc
import pytest

from dilock import AcquisitionUnknown, BackendUnavailable, Options, State
from dilock import etcd as etcd_module


def _owned_key_range(lease_id: int) -> object:
    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            return SimpleNamespace(kvs=[SimpleNamespace(key=request.key, lease=lease_id)])

    return KV()


@pytest.mark.parametrize(
    ("endpoint", "expected_target", "secure"),
    [
        ("http://etcd.example:2379", "etcd.example:2379", False),
        ("etcd.example:2379", "etcd.example:2379", False),
        ("HTTPS://etcd.example:2379", "etcd.example:2379", True),
        ("https://etcd.example", "etcd.example:2379", True),
    ],
)
def test_endpoint_scheme_selects_transport(endpoint: str, expected_target: str, secure: bool) -> None:
    assert etcd_module._parse_endpoint(endpoint) == (expected_target, secure)


def test_https_endpoint_constructs_secure_grpc_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, object | None]] = []
    channel = object()
    roots = object()

    class Stub:
        def __init__(self, _: object) -> None:
            pass

    monkeypatch.setattr(etcd_module.grpc.aio, "secure_channel", lambda target, credentials: calls.append(("secure", target, credentials)) or channel)
    monkeypatch.setattr(etcd_module.grpc.aio, "insecure_channel", lambda target: calls.append(("insecure", target, None)) or channel)
    monkeypatch.setattr(etcd_module.grpc, "ssl_channel_credentials", lambda: roots)
    monkeypatch.setattr(etcd_module.rpc_pb2_grpc, "KVStub", Stub)
    monkeypatch.setattr(etcd_module.rpc_pb2_grpc, "LeaseStub", Stub)
    monkeypatch.setattr(etcd_module.v3lock_pb2_grpc, "LockStub", Stub)

    provider = etcd_module.EtcdProvider("https://etcd.example:2379", "test", "epoch")
    assert calls == [("secure", "etcd.example:2379", roots)]
    assert provider._channel is channel


@pytest.mark.asyncio
async def test_cancellation_during_lock_key_verification_revokes_granted_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verify_started = asyncio.Event()
    revoke_calls = 0
    meta_key = b"/dilock/namespaces/test/meta"
    metadata = json.dumps({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "etcd-native-lock-v1",
        "coordination_epoch": "epoch",
        "fixed_lease_ms": None,
    }).encode()

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            if request.key == meta_key:
                return SimpleNamespace(count=1, kvs=[SimpleNamespace(key=meta_key, value=metadata)])
            verify_started.set()
            await asyncio.Future()

    class Leases:
        async def LeaseGrant(self, request: object, timeout: float) -> object:
            return SimpleNamespace(ID=42, TTL=3)

        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            nonlocal revoke_calls
            revoke_calls += 1
            return SimpleNamespace()

    class Locks:
        async def Lock(self, request: object, timeout: float) -> object:
            return SimpleNamespace(key=b"/native/opaque-lock-key")

    async def keeper(self: etcd_module.EtcdLease) -> None:
        await asyncio.Future()

    monkeypatch.setattr(etcd_module.EtcdLease, "_keepalive", keeper)
    provider = etcd_module.EtcdProvider.__new__(etcd_module.EtcdProvider)
    provider.namespace, provider.epoch = "test", "epoch"
    provider._kv, provider._leases_rpc, provider._locks = KV(), Leases(), Locks()
    provider._leases = set()
    provider._closed = False
    task = asyncio.create_task(provider.lock("cancel-verify").try_acquire(Options(lease=3, rpc=1, cleanup=1)))
    await verify_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert revoke_calls == 1
    assert provider._leases == set()


@pytest.mark.asyncio
async def test_cancelled_lock_rpc_is_backend_failure_after_confirmed_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    meta_key = b"/dilock/namespaces/test/meta"
    metadata = json.dumps({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "etcd-native-lock-v1",
        "coordination_epoch": "epoch",
        "fixed_lease_ms": None,
    }).encode()
    revoked: list[int] = []

    class CancelledRpc(Exception):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.CANCELLED

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            return SimpleNamespace(count=1, kvs=[SimpleNamespace(key=meta_key, value=metadata)])

    class Leases:
        async def LeaseGrant(self, request: object, timeout: float) -> object:
            return SimpleNamespace(ID=43, TTL=3)

        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            revoked.append(request.ID)
            return SimpleNamespace()

    class Locks:
        async def Lock(self, request: object, timeout: float) -> object:
            raise CancelledRpc()

    async def keeper(self: etcd_module.EtcdLease) -> None:
        await asyncio.Future()

    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", CancelledRpc)
    monkeypatch.setattr(etcd_module.EtcdLease, "_keepalive", keeper)
    provider = etcd_module.EtcdProvider.__new__(etcd_module.EtcdProvider)
    provider.namespace, provider.epoch = "test", "epoch"
    provider._kv, provider._leases_rpc, provider._locks = KV(), Leases(), Locks()
    provider._leases = set()
    provider._closed = False

    with pytest.raises(BackendUnavailable):
        await provider.lock("cancel-lock").try_acquire(Options(lease=3, rpc=1, cleanup=1))
    assert revoked == [43]


@pytest.mark.asyncio
async def test_cancellation_during_lease_grant_is_unknown_without_lease_id() -> None:
    meta_key = b"/dilock/namespaces/test/meta"
    metadata = json.dumps({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "etcd-native-lock-v1",
        "coordination_epoch": "epoch",
        "fixed_lease_ms": None,
    }).encode()
    grant_started = asyncio.Event()

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            return SimpleNamespace(count=1, kvs=[SimpleNamespace(key=meta_key, value=metadata)])

    class Leases:
        async def LeaseGrant(self, request: object, timeout: float) -> object:
            grant_started.set()
            await asyncio.Future()

    provider = etcd_module.EtcdProvider.__new__(etcd_module.EtcdProvider)
    provider.namespace, provider.epoch = "test", "epoch"
    provider._kv, provider._leases_rpc = KV(), Leases()
    provider._closed = False
    task = asyncio.create_task(
        provider.lock("cancel-grant").try_acquire(Options(lease=3, rpc=1, cleanup=1)),
    )
    await grant_started.wait()
    task.cancel()

    with pytest.raises(AcquisitionUnknown, match="orphan may remain until TTL expiry"):
        await task


@pytest.mark.asyncio
async def test_manual_renewal_cannot_resurrect_elapsed_confirmed_budget() -> None:
    class KeepAlive:
        async def write(self, _: object) -> None:
            return None

        async def read(self) -> object:
            await asyncio.sleep(0.04)
            return SimpleNamespace(ID=44, TTL=3)

        def cancel(self) -> None:
            return None

    provider = SimpleNamespace(_leases_rpc=SimpleNamespace(LeaseKeepAlive=KeepAlive))
    lease = etcd_module.EtcdLease(provider, b"lock", 44, 3, Options(lease=3, rpc=1), time.monotonic())
    lease._key = b"opaque"
    lease._deadline = time.monotonic() + 0.01

    assert await lease.renew() is False
    assert lease.state is State.LOST


@pytest.mark.asyncio
async def test_cancelled_release_caches_unknown_and_does_not_retry_unlock() -> None:
    unlock_started = asyncio.Event()
    release_finished = asyncio.Event()
    unlock_calls = 0

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            nonlocal unlock_calls
            unlock_calls += 1
            unlock_started.set()
            await release_finished.wait()
            return SimpleNamespace()

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            return SimpleNamespace()

    provider = SimpleNamespace(
        _kv=_owned_key_range(45), _locks=Locks(), _leases_rpc=Leases(), _leases=set(), epoch="epoch",
    )
    lease = etcd_module.EtcdLease(provider, b"resource", 45, 3, Options(lease=3), time.monotonic())
    lease._key = b"opaque-lock-key"

    task = asyncio.create_task(lease.release())
    await unlock_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert lease.state is State.RELEASED_UNKNOWN
    assert await lease.release() is etcd_module.ReleaseOutcome.UNKNOWN
    assert unlock_calls == 1
    assert lease not in provider._leases


@pytest.mark.asyncio
async def test_revoke_acknowledgement_is_not_reported_as_native_unlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnlockFailure(Exception):
        pass

    unlock_calls = 0
    revoke_calls = 0

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            nonlocal unlock_calls
            unlock_calls += 1
            raise UnlockFailure("Unlock acknowledgement was not received")

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            nonlocal revoke_calls
            revoke_calls += 1
            return SimpleNamespace()

    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", UnlockFailure)
    provider = SimpleNamespace(
        _kv=_owned_key_range(46), _locks=Locks(), _leases_rpc=Leases(), _leases=set(), epoch="epoch",
    )
    lease = etcd_module.EtcdLease(
        provider, b"resource", 46, 3, Options(lease=3), time.monotonic(),
    )
    lease._key = b"opaque-lock-key"
    provider._leases.add(lease)

    outcome = await lease.release()
    assert outcome is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED
    assert lease.state is State.RELEASED
    assert await lease.release() is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED
    assert unlock_calls == 1
    assert revoke_calls == 1
    assert lease not in provider._leases


@pytest.mark.asyncio
async def test_expired_etcd_key_skips_noop_unlock_and_reports_already_not_owned() -> None:
    unlock_calls = 0

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            return SimpleNamespace(kvs=[])

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            nonlocal unlock_calls
            unlock_calls += 1
            return SimpleNamespace()

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            return SimpleNamespace()

    provider = SimpleNamespace(_kv=KV(), _locks=Locks(), _leases_rpc=Leases(), _leases=set())
    lease = etcd_module.EtcdLease(provider, b"resource", 50, 3, Options(lease=3), time.monotonic())
    lease._key = b"expired-owner-key"

    outcome = await lease.release()

    assert outcome is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED
    assert unlock_calls == 0


@pytest.mark.asyncio
async def test_etcd_lease_expiry_between_range_and_unlock_is_not_reported_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LeaseGone(Exception):
        def code(self) -> object:
            return etcd_module.grpc.StatusCode.NOT_FOUND

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            return SimpleNamespace(kvs=[SimpleNamespace(key=request.key, lease=51)])

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            # etcd can acknowledge Unlock after the attached lease has expired.
            return SimpleNamespace()

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            raise LeaseGone("the lease expired after Range")

    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", LeaseGone)
    provider = SimpleNamespace(_kv=KV(), _locks=Locks(), _leases_rpc=Leases(), _leases=set())
    lease = etcd_module.EtcdLease(provider, b"resource", 51, 3, Options(lease=3), time.monotonic())
    lease._key = b"expiring-owner-key"

    outcome = await lease.release()

    assert outcome is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("revoke_acknowledged", "expected"),
    [
        (True, etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED),
        (False, etcd_module.ReleaseOutcome.UNKNOWN),
    ],
)
async def test_applied_unlock_with_lost_ack_is_classified_and_cannot_unlock_successor(
    monkeypatch: pytest.MonkeyPatch,
    revoke_acknowledged: bool,
    expected: etcd_module.ReleaseOutcome,
) -> None:
    """Model etcd applying Unlock before its successful RPC acknowledgement is lost."""
    class LostAck(Exception):
        pass

    lock_owner: dict[str, str | None] = {"value": "owner-46"}
    unlock_calls = 0
    revoke_calls = 0

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            nonlocal unlock_calls
            unlock_calls += 1
            # The owner-specific native key is removed by etcd before the
            # transport reports Unavailable to the client.
            if lock_owner["value"] == "owner-46":
                lock_owner["value"] = None
            raise LostAck("etcd applied Unlock; successful acknowledgement was dropped")

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            nonlocal revoke_calls
            revoke_calls += 1
            if revoke_acknowledged:
                # Revoke is scoped to the old acquisition's unique lease and
                # must not remove a lock created later by a successor.
                if lock_owner["value"] == "owner-46":
                    lock_owner["value"] = None
                return SimpleNamespace()
            raise LostAck("LeaseRevoke acknowledgement was also dropped")

    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", LostAck)
    provider = SimpleNamespace(
        _kv=_owned_key_range(46), _locks=Locks(), _leases_rpc=Leases(), _leases=set(), epoch="epoch",
    )
    lease = etcd_module.EtcdLease(
        provider, b"resource", 46, 3, Options(lease=3), time.monotonic(),
    )
    lease._key = b"owner-specific-opaque-key"
    provider._leases.add(lease)

    outcome = await lease.release()
    assert outcome is expected
    assert lease.state is (
        State.RELEASED if expected is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED
        else State.RELEASED_UNKNOWN
    )
    assert lock_owner["value"] is None

    # Model the next etcd owner acquiring its own opaque key, then retry the
    # terminal old release. A cached outcome must not replay old Unlock or
    # revoke operations against that successor.
    lock_owner["value"] = "successor-47"
    assert await lease.release() is expected
    assert lock_owner["value"] == "successor-47"
    assert unlock_calls == 1
    assert revoke_calls == 1
    assert lease not in provider._leases


@pytest.mark.asyncio
async def test_unlock_and_revoke_share_one_cleanup_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, float]] = []
    now = iter((100.0, 100.0, 100.25, 100.75))
    real_monotonic = time.monotonic

    def scripted_monotonic() -> float:
        try:
            return next(now)
        except StopIteration:
            # pytest and asyncio also consult the shared time module during
            # teardown; keep the scripted clock scoped to the SDK calls.
            return real_monotonic()

    class UnlockFailure(Exception):
        pass

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            observed.append(("unlock", timeout))
            raise UnlockFailure("unlock not acknowledged")

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            observed.append(("range", timeout))
            return SimpleNamespace(kvs=[SimpleNamespace(key=request.key, lease=47)])

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            observed.append(("revoke", timeout))
            return SimpleNamespace()

    monkeypatch.setattr(etcd_module.grpc.aio, "AioRpcError", UnlockFailure)
    monkeypatch.setattr(etcd_module.time, "monotonic", scripted_monotonic)
    provider = SimpleNamespace(
        _kv=KV(), _locks=Locks(), _leases_rpc=Leases(), _leases=set(), epoch="epoch",
    )
    lease = etcd_module.EtcdLease(
        provider, b"resource", 47, 3, Options(lease=3, cleanup=1), 0.0,
    )
    lease._key = b"opaque-lock-key"

    outcome = await lease.release()

    assert outcome is etcd_module.ReleaseOutcome.ALREADY_NOT_OWNED
    assert observed == [("range", 1.0), ("unlock", 0.75), ("revoke", 0.25)]


@pytest.mark.asyncio
async def test_release_bounds_slow_keepalive_shutdown_with_shared_deadline() -> None:
    observed: list[tuple[str, float]] = []
    keeper_started = asyncio.Event()
    cancellation_seen = asyncio.Event()

    async def slow_keeper() -> None:
        keeper_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await asyncio.sleep(0.04)

    class Locks:
        async def Unlock(self, request: object, timeout: float) -> object:
            observed.append(("unlock", timeout))
            return SimpleNamespace()

    class KV:
        async def Range(self, request: object, timeout: float) -> object:
            observed.append(("range", timeout))
            return SimpleNamespace(kvs=[SimpleNamespace(key=request.key, lease=49)])

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            observed.append(("revoke", timeout))
            return SimpleNamespace()

    provider = SimpleNamespace(_kv=KV(), _locks=Locks(), _leases_rpc=Leases(), _leases=set())
    options = Options(lease=3, cleanup=0.2)
    lease = etcd_module.EtcdLease(provider, b"lock", 49, 3, options, time.monotonic())
    lease._key = b"opaque-lock-key"
    lease._keeper = asyncio.create_task(slow_keeper())
    provider._leases.add(lease)
    await keeper_started.wait()

    started = time.monotonic()
    outcome = await lease.release()
    elapsed = time.monotonic() - started

    assert cancellation_seen.is_set()
    assert outcome is etcd_module.ReleaseOutcome.RELEASED
    assert observed[0][0] == "range"
    assert observed[1][0] == "unlock"
    assert 0 < observed[1][1] < options.cleanup - 0.02
    assert observed[2][0] == "revoke"
    assert 0 < observed[2][1] < observed[1][1]
    assert elapsed < options.cleanup + 0.1
    assert provider._leases == set()


def test_remaining_cleanup_uses_monotonic_deadline() -> None:
    assert etcd_module._remaining_cleanup(10.0, 8.75) == 1.25
    assert etcd_module._remaining_cleanup(10.0, 10.0) == 0.0
    assert etcd_module._remaining_cleanup(10.0, 11.0) == 0.0


@pytest.mark.asyncio
async def test_abandon_bounds_keeper_drain_and_revoke_to_one_budget() -> None:
    keeper_can_exit = asyncio.Event()
    revoke_timeouts: list[float] = []

    async def stubborn_keeper() -> None:
        while not keeper_can_exit.is_set():
            try:
                await keeper_can_exit.wait()
            except asyncio.CancelledError:
                # Simulate a stream task that takes time to unwind after cancel.
                continue

    class Leases:
        async def LeaseRevoke(self, request: object, timeout: float) -> object:
            revoke_timeouts.append(timeout)
            return SimpleNamespace()

    provider = SimpleNamespace(_leases_rpc=Leases(), _leases=set())
    options = Options(lease=3, cleanup=0.08)
    lease = etcd_module.EtcdLease(provider, b"lock", 48, 3, options, time.monotonic())
    lease._keeper = asyncio.create_task(stubborn_keeper())
    provider._leases.add(lease)

    started = time.monotonic()
    await lease._abandon()
    elapsed = time.monotonic() - started

    assert elapsed < 0.25
    assert len(revoke_timeouts) == 1
    assert 0 < revoke_timeouts[0] < options.cleanup
    assert lease.state is State.RELEASED
    assert lease not in provider._leases
    keeper_can_exit.set()
    await asyncio.gather(lease._keeper, return_exceptions=True)
