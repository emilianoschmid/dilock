"""PostgreSQL session advisory-lock profile. See spec/backends/POSTGRESQL.md."""
from __future__ import annotations

import asyncio
import inspect
import math
import time
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, AsyncIterator, Awaitable, Callable

from .core import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, CleanupObservation,
    LeaseLost, Options, ProtocolMismatch, ReleaseOutcome, ScopeReleaseError, State,
    UnsupportedCapability, WaitTimeout, identity, report_cleanup,
)

_PROFILE = "postgres-advisory-session-v1"
_SCHEME = "sha256-framed-utf8-v1"
_METADATA_LIMIT = 8192
_METADATA_TABLE = "dilock_namespace_meta"

ConnectionFactory = Callable[[str], Awaitable[Any]]


def advisory_key(resource_id: str) -> int:
    """Return the portable signed int64 key from a draft-1 resource identity."""
    if not isinstance(resource_id, str) or len(resource_id) != 64:
        raise ValueError("resource_id must be a 64-character hexadecimal digest")
    try:
        value = int(resource_id[:16], 16)
    except ValueError as exc:
        raise ValueError("resource_id must be hexadecimal") from exc
    return value - (1 << 64) if value >= 1 << 63 else value


def _is_exact_metadata(row: Any, epoch: str) -> bool:
    expected = {
        "protocol_major": 1,
        "identity_scheme": _SCHEME,
        "profile": _PROFILE,
        "coordination_epoch": epoch,
        "fixed_lease_ms": None,
    }
    return (
        isinstance(row, dict)
        and set(row) == set(expected)
        and type(row.get("protocol_major")) is int
        and all(row.get(key) == value for key, value in expected.items())
    )


async def _default_connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise BackendUnavailable("PostgreSQL support requires the 'postgres' extra") from exc
    try:
        return await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    except Exception as exc:
        raise BackendUnavailable("PostgreSQL connection failed") from exc


async def _close(connection: Any) -> None:
    try:
        result = connection.close()
        if inspect.isawaitable(result):
            await result
    except Exception:
        pass


class PostgresProvider:
    """Own PostgreSQL connection creation and private advisory-lock sessions."""

    def __init__(
        self, dsn: str, namespace: str = "default", epoch: str = "", *,
        default_acquisition: AcquireOptions = AcquireOptions(),
        cleanup_observer: Callable[[CleanupObservation], None] | None = None,
        _connect: ConnectionFactory | None = None,
    ) -> None:
        if not isinstance(dsn, str) or not dsn:
            raise ValueError("dsn must be a nonempty PostgreSQL connection string")
        identity(namespace, "validation")
        if not isinstance(epoch, str) or not epoch or "\0" in epoch:
            raise ValueError("epoch must be nonempty and must not contain NUL")
        if len(epoch.encode("utf-8", "strict")) > 1024:
            raise ValueError("epoch must be at most 1024 UTF-8 bytes")
        if not isinstance(default_acquisition, AcquireOptions):
            raise ValueError("default_acquisition must be AcquireOptions")
        self.dsn, self.namespace, self.epoch = dsn, namespace, epoch
        self._default_acquisition = default_acquisition
        self._cleanup_observer = cleanup_observer
        self._connect = _connect or _default_connect
        self._leases: set[PostgresLease] = set()
        self._cleanup: set[asyncio.Task[Any]] = set()
        self._closed = False

    @property
    def default_acquisition(self) -> AcquireOptions:
        return self._default_acquisition

    def lock(self, resource: str) -> PostgresHandle:
        if self._closed:
            raise RuntimeError("provider closed")
        resource_id = identity(self.namespace, resource)
        return PostgresHandle(self, advisory_key(resource_id))

    async def _open(self) -> Any:
        if self._closed:
            raise BackendUnavailable("PostgreSQL provider is closed")
        try:
            return await self._connect(self.dsn)
        except BackendUnavailable:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise BackendUnavailable("PostgreSQL connection failed") from exc

    async def _query(self, connection: Any, sql: str, params: tuple[Any, ...],
                     timeout: float, *, mutation: bool) -> Any:
        try:
            async with asyncio.timeout(timeout):
                async with connection.cursor() as cursor:
                    await cursor.execute(sql, params)
                    return await cursor.fetchone()
        except asyncio.CancelledError as exc:
            if mutation:
                raise AcquisitionUnknown("PostgreSQL mutation was cancelled after dispatch") from exc
            raise
        except TimeoutError as exc:
            if mutation:
                raise AcquisitionUnknown("PostgreSQL mutation acknowledgement is unknown") from exc
            raise BackendUnavailable("PostgreSQL query timed out") from exc
        except Exception as exc:
            if mutation:
                raise AcquisitionUnknown("PostgreSQL mutation acknowledgement is unknown") from exc
            raise BackendUnavailable("PostgreSQL query failed") from exc

    async def _metadata(self, timeout: float) -> None:
        connection = await self._open()
        try:
            row = await self._query(
                connection,
                f"SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms "
                f"FROM {_METADATA_TABLE} WHERE namespace = %s",
                (self.namespace,), timeout, mutation=False,
            )
            if row is None:
                raise ProtocolMismatch("missing PostgreSQL namespace metadata")
            if isinstance(row, dict):
                metadata = row
            else:
                try:
                    metadata = {
                        "protocol_major": row[0], "identity_scheme": row[1],
                        "profile": row[2], "coordination_epoch": row[3],
                        "fixed_lease_ms": row[4],
                    }
                except (IndexError, TypeError) as exc:
                    raise ProtocolMismatch("malformed PostgreSQL namespace metadata") from exc
            if not _is_exact_metadata(metadata, self.epoch):
                raise ProtocolMismatch("incompatible PostgreSQL namespace metadata")
        finally:
            await _close(connection)

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.gather(*(lease.release() for lease in tuple(self._leases)), return_exceptions=True)
        if self._cleanup:
            await asyncio.gather(*tuple(self._cleanup), return_exceptions=True)

    def _observe_cleanup(self, outcome: ReleaseOutcome, reason: str) -> None:
        report_cleanup(self._cleanup_observer, "postgres", outcome, reason)

    def _track_cleanup(self, task: asyncio.Task[Any]) -> None:
        self._cleanup.add(task)
        task.add_done_callback(self._cleanup.discard)

    async def __aenter__(self) -> PostgresProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class PostgresHandle:
    """Reusable resource handle; every attempt owns a distinct DB session."""

    def __init__(self, provider: PostgresProvider, key: int) -> None:
        self._provider, self._key = provider, key

    @staticmethod
    def _validate_options(options: Options) -> None:
        if options.require_fencing:
            raise UnsupportedCapability("PostgreSQL advisory locks do not provide fencing")
        if options.auto_renew:
            raise UnsupportedCapability("PostgreSQL advisory locks have no lease TTL to renew")

    async def try_acquire(self, options: Options | None = None) -> PostgresLease | None:
        selected = options or self._provider.default_acquisition.lease
        if not isinstance(selected, Options):
            raise ValueError("options must be Options")
        self._validate_options(selected)
        await self._provider._metadata(selected.rpc)
        return await self._attempt(selected, time.monotonic() + selected.rpc, trying=True)

    async def acquire(
        self, wait: float | AcquireOptions | None = None, options: Options | None = None,
    ) -> PostgresLease:
        defaults = self._provider.default_acquisition
        if isinstance(wait, AcquireOptions):
            if options is not None:
                raise ValueError("pass either AcquireOptions or wait/options")
            options, wait = wait.lease, wait.wait
        elif wait is None:
            wait, options = defaults.wait, options or defaults.lease
        options = options or defaults.lease
        if not isinstance(options, Options):
            raise ValueError("options must be Options")
        if not math.isfinite(wait) or wait <= 0:
            raise ValueError("wait must be finite and positive")
        self._validate_options(options)
        deadline = time.monotonic() + wait
        await self._provider._metadata(min(options.rpc, max(.001, deadline - time.monotonic())))
        return await self._attempt(options, deadline, trying=False)

    async def _attempt(
        self, options: Options, deadline: float, *, trying: bool,
    ) -> PostgresLease | None:
        provider = self._provider
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if trying:
                    return None
                raise WaitTimeout("PostgreSQL advisory-lock wait budget exhausted")
            connection = await provider._open()
            try:
                row = await provider._query(
                    connection,
                    "SELECT pg_try_advisory_lock(%s::bigint)",
                    (self._key,), min(options.rpc, remaining), mutation=True,
                )
                if not isinstance(row, (tuple, list)) or len(row) != 1 or type(row[0]) is not bool:
                    raise AcquisitionUnknown("PostgreSQL advisory-lock response was malformed")
                if row[0]:
                    lease = PostgresLease(provider, connection, self._key, options)
                    provider._leases.add(lease)
                    return lease
            except BaseException:
                await _close(connection)
                raise
            await _close(connection)
            if trying:
                return None
            await asyncio.sleep(min(.25, max(.01, deadline - time.monotonic())))

    @asynccontextmanager
    async def hold(self, options: AcquireOptions | None = None) -> AsyncIterator[PostgresLease]:
        lease = await self.acquire(options) if options is not None else await self.acquire()
        try:
            yield lease
        except BaseException:
            await self._scope_release(lease, "body_failed", fail_closed=False)
            raise
        else:
            await self._scope_release(lease, "scope_exit", fail_closed=True)

    async def _scope_release(self, lease: PostgresLease, reason: str, *, fail_closed: bool) -> None:
        task = asyncio.create_task(lease.release())
        self._provider._track_cleanup(task)
        try:
            outcome = await asyncio.shield(task)
        except BaseException:
            self._provider._observe_cleanup(ReleaseOutcome.UNKNOWN, reason)
            if fail_closed:
                raise ScopeReleaseError(ReleaseOutcome.UNKNOWN)
            return
        if outcome != ReleaseOutcome.RELEASED:
            self._provider._observe_cleanup(outcome, reason)
            if fail_closed:
                raise ScopeReleaseError(outcome)


class PostgresLease:
    """One advisory lock held by one physical PostgreSQL session."""

    def __init__(self, provider: PostgresProvider, connection: Any, key: int, options: Options) -> None:
        self._provider, self._connection, self._key = provider, connection, key
        self.options = options
        self.effective_lease_ms: None = None
        self._state = State.HELD
        self._outcome: ReleaseOutcome | None = None
        self._gate = asyncio.Lock()

    @property
    def state(self) -> State:
        return self._state

    @property
    def fence(self) -> None:
        raise UnsupportedCapability("PostgreSQL advisory locks do not provide fencing")

    async def renew(self) -> bool:
        async with self._gate:
            if self._state != State.HELD:
                return False
            try:
                row = await self._provider._query(
                    self._connection, "SELECT 1", (), self.options.rpc, mutation=False,
                )
                if not isinstance(row, (tuple, list)) or len(row) != 1 or row[0] != 1:
                    raise BackendUnavailable("PostgreSQL liveness response was malformed")
                return True
            except (BackendUnavailable, ProtocolMismatch, asyncio.CancelledError):
                self._state = State.LOST
                raise

    async def release(self) -> ReleaseOutcome:
        async with self._gate:
            if self._outcome is not None:
                return self._outcome
            self._state = State.RELEASING
            outcome = ReleaseOutcome.UNKNOWN
            try:
                row = await self._provider._query(
                    self._connection,
                    "SELECT pg_advisory_unlock(%s::bigint)",
                    (self._key,), self.options.cleanup, mutation=True,
                )
                if isinstance(row, (tuple, list)) and len(row) == 1 and type(row[0]) is bool:
                    outcome = ReleaseOutcome.RELEASED if row[0] else ReleaseOutcome.ALREADY_NOT_OWNED
            except (AcquisitionUnknown, BackendUnavailable, ProtocolMismatch, TimeoutError,
                    asyncio.CancelledError):
                outcome = ReleaseOutcome.UNKNOWN
            try:
                await _close(self._connection)
            except Exception:
                outcome = ReleaseOutcome.UNKNOWN
            self._outcome = outcome
            self._state = State.RELEASED if outcome != ReleaseOutcome.UNKNOWN else State.RELEASED_UNKNOWN
            self._provider._leases.discard(self)
            return outcome

    async def __aenter__(self) -> PostgresLease:
        if self.state != State.HELD:
            raise LeaseLost("PostgreSQL advisory lock is no longer held")
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, tb: TracebackType | None) -> None:
        outcome = await self.release()
        if outcome != ReleaseOutcome.RELEASED and exc_type is None:
            raise ScopeReleaseError(outcome)
