"""Contract tests for the PostgreSQL advisory-session profile."""
from __future__ import annotations

import pytest

from dilock import AcquisitionUnknown, Options, ReleaseOutcome, State, UnsupportedCapability
from dilock.postgres import PostgresProvider, _is_exact_metadata, advisory_key


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.row = None

    async def __aenter__(self) -> "FakeCursor":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.row = self.connection.execute(sql, params)

    async def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, peer: "FakePostgres") -> None:
        self.peer = peer
        self.closed = False
        self.held: int | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def execute(self, sql: str, params: tuple[object, ...]):
        if "FROM dilock_namespace_meta" in sql:
            return (1, "sha256-framed-utf8-v1", "postgres-advisory-session-v1", "epoch", None)
        if "pg_try_advisory_lock" in sql:
            key = int(params[0])
            if key in self.peer.locks:
                return (False,)
            self.peer.locks.add(key)
            self.held = key
            return (True,)
        if "pg_advisory_unlock" in sql:
            key = int(params[0])
            if self.held != key or key not in self.peer.locks:
                return (False,)
            self.peer.locks.remove(key)
            self.held = None
            return (True,)
        if sql.strip() == "SELECT 1":
            return (1,)
        raise AssertionError(sql)

    async def close(self) -> None:
        if self.held is not None:
            self.peer.locks.discard(self.held)
            self.held = None
        self.closed = True


class FakePostgres:
    def __init__(self) -> None:
        self.locks: set[int] = set()
        self.connections: list[FakeConnection] = []

    async def connect(self, _dsn: str) -> FakeConnection:
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection


class LostAcknowledgementConnection(FakeConnection):
    def __init__(self, peer: "FakePostgres", *, acquire: bool = False, release: bool = False) -> None:
        super().__init__(peer)
        self.lose_acquire_ack = acquire
        self.lose_release_ack = release

    def execute(self, sql: str, params: tuple[object, ...]):
        if "pg_try_advisory_lock" in sql and self.lose_acquire_ack:
            key = int(params[0])
            self.peer.locks.add(key)
            self.held = key
            raise TimeoutError("injected lost acquisition acknowledgement")
        if "pg_advisory_unlock" in sql and self.lose_release_ack:
            if self.held is not None:
                self.peer.locks.discard(self.held)
                self.held = None
            raise TimeoutError("injected lost release acknowledgement")
        return super().execute(sql, params)


def make_provider(peer: FakePostgres) -> PostgresProvider:
    return PostgresProvider("postgresql://test", "test", "epoch", _connect=peer.connect)


def test_advisory_key_is_signed_big_endian_int64() -> None:
    assert advisory_key("0000000000000001" + "0" * 48) == 1
    assert advisory_key("ffffffffffffffff" + "0" * 48) == -1


def test_metadata_profile_is_exact() -> None:
    assert _is_exact_metadata(
        {"protocol_major": 1, "identity_scheme": "sha256-framed-utf8-v1",
         "profile": "postgres-advisory-session-v1", "coordination_epoch": "epoch",
         "fixed_lease_ms": None},
        "epoch",
    )


@pytest.mark.asyncio
async def test_contention_release_and_liveness() -> None:
    peer = FakePostgres()
    provider = make_provider(peer)
    first = await provider.lock("invoice/1").acquire()
    second = await provider.lock("invoice/1").try_acquire()
    assert first.state == State.HELD
    assert second is None
    assert await first.renew()
    assert await first.release() == ReleaseOutcome.RELEASED
    successor = await provider.lock("invoice/1").try_acquire()
    assert successor is not None
    assert await successor.release() == ReleaseOutcome.RELEASED
    await provider.aclose()


@pytest.mark.asyncio
async def test_fencing_and_auto_renew_are_rejected_before_io() -> None:
    peer = FakePostgres()
    provider = make_provider(peer)
    with pytest.raises(UnsupportedCapability):
        await provider.lock("x").try_acquire(Options(require_fencing=True))
    with pytest.raises(UnsupportedCapability):
        await provider.lock("x").try_acquire(Options(auto_renew=True))
    assert len(peer.connections) == 0


@pytest.mark.asyncio
async def test_lost_acquisition_acknowledgement_is_unknown_and_cleaned_up() -> None:
    peer = FakePostgres()

    async def connect(_dsn: str) -> FakeConnection:
        connection = LostAcknowledgementConnection(peer, acquire=True)
        peer.connections.append(connection)
        return connection

    provider = PostgresProvider("postgresql://test", "test", "epoch", _connect=connect)
    with pytest.raises(AcquisitionUnknown, match="acknowledgement"):
        await provider.lock("lost-acquire-ack").try_acquire()
    assert peer.connections[-1].closed
    assert peer.locks == set()
    await provider.aclose()


@pytest.mark.asyncio
async def test_lost_release_acknowledgement_is_unknown_after_server_mutation() -> None:
    peer = FakePostgres()

    async def connect(_dsn: str) -> FakeConnection:
        connection = LostAcknowledgementConnection(peer, release=True)
        peer.connections.append(connection)
        return connection

    provider = PostgresProvider("postgresql://test", "test", "epoch", _connect=connect)
    lease = await provider.lock("lost-release-ack").acquire()
    connection = peer.connections[-1]
    assert await lease.release() == ReleaseOutcome.UNKNOWN
    assert lease.state == State.RELEASED_UNKNOWN
    assert connection.closed
    assert peer.locks == set()
    await provider.aclose()
