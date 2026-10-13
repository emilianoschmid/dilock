import pytest
import asyncio
from redis.exceptions import ConnectionError as RedisConnectionError

from dilock import (
    AcquisitionUnknown, BackendUnavailable, Options, ProtocolMismatch,
    ReleaseOutcome, WaitTimeout,
)
from dilock.redis import RedisHandle, RedisProvider


class _AcknowledgedConflictClient:
    def __init__(self, key_type: bytes | str | Exception) -> None:
        self.key_type = key_type
        self.calls: list[tuple[str, object]] = []
        self.get_calls = 0
        self.metadata_type: bytes | str | Exception = b"string"
        self.stall_metadata_type = False
        self.stall_get = False

    async def get(self, key: str) -> bytes:
        self.get_calls += 1
        if self.stall_get:
            await asyncio.Event().wait()
        return (b'{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",'
                b'"profile":"redis-single-authority-v1","coordination_epoch":"test",'
                b'"fixed_lease_ms":null}')

    async def set(self, key: str, owner: bytes, **kwargs: object) -> None:
        self.calls.append(("set", key))
        return None

    async def type(self, key: str) -> bytes | str:
        self.calls.append(("type", key))
        if key.endswith(":meta"):
            if self.stall_metadata_type:
                await asyncio.Event().wait()
            if isinstance(self.metadata_type, Exception):
                raise self.metadata_type
            return self.metadata_type
        if isinstance(self.key_type, Exception):
            raise self.key_type
        return self.key_type


def _handle(key_type: bytes | str | Exception) -> tuple[RedisHandle, _AcknowledgedConflictClient]:
    provider = RedisProvider.__new__(RedisProvider)
    provider._client = _AcknowledgedConflictClient(key_type)
    provider._closed = False
    provider.namespace = "test"
    provider.epoch = "test"
    client = provider._client
    return RedisHandle(provider, "dilock:{test}:lease"), client


@pytest.mark.parametrize("key_type", [b"string", b"none", "string", "none"])
async def test_string_or_expired_conflict_remains_busy(key_type: bytes | str) -> None:
    handle, client = _handle(key_type)

    assert await handle.try_acquire(Options()) is None
    assert client.calls == [
        ("type", "dilock:namespace:test:meta"),
        ("set", "dilock:{test}:lease"),
        ("type", "dilock:{test}:lease"),
    ]


@pytest.mark.parametrize("key_type", [b"list", b"hash", b"stream"])
async def test_non_string_conflict_is_protocol_mismatch_without_mutation(key_type: bytes) -> None:
    handle, client = _handle(key_type)

    with pytest.raises(ProtocolMismatch):
        await handle.try_acquire(Options())
    assert client.calls == [
        ("type", "dilock:namespace:test:meta"),
        ("set", "dilock:{test}:lease"),
        ("type", "dilock:{test}:lease"),
    ]


@pytest.mark.parametrize("failure", [RedisConnectionError("offline"), TimeoutError("late")])
async def test_type_failure_is_backend_unavailable_never_busy(failure: Exception) -> None:
    handle, client = _handle(failure)

    with pytest.raises(BackendUnavailable):
        await handle.try_acquire(Options())
    assert client.calls == [
        ("type", "dilock:namespace:test:meta"),
        ("set", "dilock:{test}:lease"),
        ("type", "dilock:{test}:lease"),
    ]


@pytest.mark.parametrize("failure", [RedisConnectionError("reply lost"), TimeoutError("late")])
@pytest.mark.parametrize(
    "cleanup",
    [ReleaseOutcome.RELEASED, ReleaseOutcome.ALREADY_NOT_OWNED, ReleaseOutcome.UNKNOWN],
)
async def test_dispatched_set_transport_failure_remains_unknown_after_cleanup(
    failure: Exception, cleanup: ReleaseOutcome,
) -> None:
    class FailedSetClient(_AcknowledgedConflictClient):
        applied_owner: bytes | None = None

        async def set(self, key: str, owner: bytes, **kwargs: object) -> None:
            self.calls.append(("set", key))
            # Redis commits SET, then the response is lost before reaching the SDK.
            self.applied_owner = owner
            raise failure

    handle, client = _handle(b"string")
    handle._provider._client = FailedSetClient(b"string")
    client = handle._provider._client

    async def cleanup_owner(key: str, owner: bytes, budget: float) -> ReleaseOutcome:
        assert client.applied_owner == owner
        if cleanup in (ReleaseOutcome.RELEASED, ReleaseOutcome.ALREADY_NOT_OWNED):
            client.applied_owner = None
        return cleanup

    handle._provider._cleanup_owner = cleanup_owner
    with pytest.raises(AcquisitionUnknown):
        await handle.try_acquire(Options())
    assert client.calls == [
        ("type", "dilock:namespace:test:meta"),
        ("set", "dilock:{test}:lease"),
    ]
    if cleanup == ReleaseOutcome.UNKNOWN:
        assert client.applied_owner is not None
    else:
        assert client.applied_owner is None


@pytest.mark.parametrize(
    ("cleanup", "expected"),
    [
        (ReleaseOutcome.RELEASED, asyncio.CancelledError),
        (ReleaseOutcome.ALREADY_NOT_OWNED, asyncio.CancelledError),
        (ReleaseOutcome.UNKNOWN, AcquisitionUnknown),
    ],
)
async def test_cancelled_set_uses_owner_cleanup_acknowledgement(
    cleanup: ReleaseOutcome, expected: type[BaseException],
) -> None:
    dispatched = asyncio.Event()

    class PendingSetClient(_AcknowledgedConflictClient):
        async def set(self, key: str, owner: bytes, **kwargs: object) -> None:
            self.calls.append(("set", key))
            dispatched.set()
            await asyncio.Event().wait()

    provider = RedisProvider.__new__(RedisProvider)
    provider._client = PendingSetClient(b"string")
    provider._closed = False
    provider.namespace = "test"
    provider.epoch = "test"

    async def cleanup_owner(key: str, owner: bytes, budget: float) -> ReleaseOutcome:
        return cleanup

    provider._cleanup_owner = cleanup_owner
    handle = RedisHandle(provider, "dilock:{test}:lease")

    task = asyncio.create_task(handle.try_acquire(Options()))
    await dispatched.wait()
    task.cancel()
    with pytest.raises(expected):
        await task


async def test_metadata_wrong_type_is_protocol_mismatch_without_read_or_acquire() -> None:
    handle, client = _handle(b"string")
    client.metadata_type = b"list"

    with pytest.raises(ProtocolMismatch):
        await handle.try_acquire(Options())
    assert client.calls == [("type", "dilock:namespace:test:meta")]
    assert client.get_calls == 0


async def test_metadata_type_transport_failure_is_backend_unavailable() -> None:
    handle, client = _handle(b"string")
    client.metadata_type = RedisConnectionError("offline")

    with pytest.raises(BackendUnavailable):
        await handle.try_acquire(Options())
    assert client.calls == [("type", "dilock:namespace:test:meta")]
    assert client.get_calls == 0


async def test_wait_deadline_during_metadata_is_wait_timeout_before_dispatch() -> None:
    handle, client = _handle(b"string")
    client.stall_get = True
    with pytest.raises(WaitTimeout):
        await handle.acquire(.02, Options(rpc=1.0))
    assert client.calls == [("type", "dilock:namespace:test:meta")]


async def test_rpc_timeout_during_metadata_before_wait_deadline_is_backend_unavailable() -> None:
    handle, client = _handle(b"string")
    client.stall_get = True

    with pytest.raises(BackendUnavailable):
        await handle.acquire(.2, Options(rpc=.02))
    assert client.calls == [("type", "dilock:namespace:test:meta")]


async def test_wait_deadline_during_metadata_type_is_wait_timeout_before_get() -> None:
    handle, client = _handle(b"string")
    client.stall_metadata_type = True

    with pytest.raises(WaitTimeout):
        await handle.acquire(.02, Options(rpc=1.0))
    assert client.calls == [("type", "dilock:namespace:test:meta")]
    assert client.get_calls == 0


async def test_wait_deadline_during_type_classification_is_wait_timeout_not_busy() -> None:
    handle, client = _handle(b"string")

    async def stalled_type(_: str) -> bytes:
        if _.endswith(":meta"):
            client.calls.append(("type", _))
            return b"string"
        client.calls.append(("type", "dilock:{test}:lease"))
        await asyncio.Event().wait()

    client.type = stalled_type
    with pytest.raises(WaitTimeout):
        await handle.acquire(.02, Options(rpc=1.0))
    assert client.calls == [
        ("type", "dilock:namespace:test:meta"),
        ("set", "dilock:{test}:lease"),
        ("type", "dilock:{test}:lease"),
    ]
