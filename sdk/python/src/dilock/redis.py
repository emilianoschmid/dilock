"""Redis single-authority profile. No failover, hidden retries or strong fencing."""
from __future__ import annotations

import asyncio
import math
import secrets
import time
from contextlib import asynccontextmanager
from importlib.resources import files
from types import TracebackType
from typing import Any, AsyncIterator, Callable

from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError, ResponseError
from redis.retry import Retry

from .core import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, CleanupObservation,
    LeaseLost, Options, ProtocolMismatch, ScopeReleaseError, report_cleanup,
    ReleaseOutcome, State, UnsupportedCapability, WaitTimeout, identity,
)
from ._metadata import parse_unique_top_level_object

_RELEASE = files("dilock").joinpath("redis-release.lua").read_text(encoding="utf-8")
_RENEW = files("dilock").joinpath("redis-renew.lua").read_text(encoding="utf-8")


class RedisProvider:
    """Owns connections, leases and cleanup tasks on one asyncio loop.

    Use async with or await aclose. Clients are constructed with zero retries;
    one explicit URL selects the authority. Missing/incompatible metadata raises
    ProtocolMismatch. Cancellation before send raises CancelledError; after SET
    dispatch it remains cancellation only when owner cleanup is acknowledged,
    otherwise it raises AcquisitionUnknown.
    Credentials in the URL are never included in SDK errors.
    """

    def __init__(
        self, url: str, namespace: str = "default", epoch: str = "", *,
        default_acquisition: AcquireOptions = AcquireOptions(),
        cleanup_observer: Callable[[CleanupObservation], None] | None = None,
    ) -> None:
        identity(namespace, "validation")
        if not epoch or len(epoch.encode()) > 1024:
            raise ValueError("epoch must be nonempty and at most 1024 bytes")
        if not isinstance(default_acquisition, AcquireOptions):
            raise ValueError("default_acquisition must be AcquireOptions")
        self.namespace, self.epoch = namespace, epoch
        self._default_acquisition = default_acquisition
        self._cleanup_observer = cleanup_observer
        self._client: Redis = Redis.from_url(
            url, decode_responses=False, retry=Retry(NoBackoff(), 0),
            retry_on_error=[], socket_connect_timeout=2, socket_timeout=2,
        )
        self._leases: set[RedisLease] = set()
        self._cleanup: set[asyncio.Task[Any]] = set()
        self._closed = False

    def lock(self, resource: str) -> RedisHandle:
        """Validate and create a reusable handle; it performs no I/O."""
        if self._closed:
            raise RuntimeError("provider closed")
        return RedisHandle(self, f"dilock:{{{identity(self.namespace, resource)}}}:lease")

    @property
    def default_acquisition(self) -> AcquireOptions:
        """Immutable acquisition defaults captured at provider creation."""
        return self._default_acquisition

    async def _metadata(self, budget: float, wait_deadline: float | None = None) -> None:
        if self._closed:
            raise RuntimeError("provider closed")
        key = f"dilock:namespace:{self.namespace}:meta"
        timeout = asyncio.timeout(budget)
        try:
            async with timeout:
                key_type = await self._client.type(key)
                if key_type not in (b"string", "string"):
                    raise ProtocolMismatch("namespace metadata must be a Redis string")
                raw = await self._client.get(key)
        except TimeoutError as exc:
            if (timeout.expired() and wait_deadline is not None
                    and time.monotonic() >= wait_deadline):
                raise WaitTimeout("contention wait budget elapsed during metadata read") from exc
            raise BackendUnavailable("namespace read failed") from exc
        except RedisError as exc:
            raise BackendUnavailable("namespace read failed") from exc
        try:
            if raw is None or len(raw) > 8192:
                raise ValueError()
            meta = parse_unique_top_level_object(raw)
            expected = {
                "protocol_major": 1, "identity_scheme": "sha256-framed-utf8-v1",
                "profile": "redis-single-authority-v1", "coordination_epoch": self.epoch,
                "fixed_lease_ms": None,
            }
            if not isinstance(meta, dict) or type(meta.get("protocol_major")) is not int:
                raise ValueError()
            if any(k not in meta or meta[k] != v for k, v in expected.items()):
                raise ValueError()
        except (ValueError, TypeError, UnicodeError) as exc:
            raise ProtocolMismatch("missing or incompatible namespace metadata") from exc

    async def _cleanup_owner(self, key: str, owner: bytes, budget: float) -> ReleaseOutcome:
        async def execute() -> ReleaseOutcome:
            try:
                async with asyncio.timeout(budget):
                    result = await self._client.eval(_RELEASE, 1, key, owner)
                if result not in (0, 1):
                    return ReleaseOutcome.UNKNOWN
                return ReleaseOutcome.RELEASED if result == 1 else ReleaseOutcome.ALREADY_NOT_OWNED
            except (RedisError, TimeoutError):
                return ReleaseOutcome.UNKNOWN
        task = asyncio.create_task(execute())
        self._cleanup.add(task)
        task.add_done_callback(self._cleanup.discard)
        # Shield keeps the cleanup alive but the provider retains its ownership.
        return await asyncio.shield(task)

    async def aclose(self) -> None:
        """Stop permission and bound cleanup of all owned leases, then close sockets."""
        self._closed = True
        for lease in tuple(self._leases):
            await lease.release()
        if self._cleanup:
            await asyncio.gather(*tuple(self._cleanup), return_exceptions=True)
        await self._client.aclose()

    def _observe_cleanup(self, outcome: ReleaseOutcome, reason: str) -> None:
        report_cleanup(self._cleanup_observer, "redis", outcome, reason)

    def _track_cleanup(self, task: asyncio.Task[Any]) -> None:
        self._cleanup.add(task)
        task.add_done_callback(self._cleanup.discard)

    async def __aenter__(self) -> RedisProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class RedisHandle:
    """Loop-bound reusable resource. Busy is the only None result."""
    def __init__(self, provider: RedisProvider, key: str) -> None:
        self._provider, self._key = provider, key

    async def try_acquire(self, options: Options | None = None) -> RedisLease | None:
        """One bounded SET. Unknown transport outcome never becomes contention."""
        if options is None:
            options = self._provider.default_acquisition.lease
        return await self._try_acquire(options, wait_deadline=None)

    async def _try_acquire(
        self, options: Options, wait_deadline: float | None,
    ) -> RedisLease | None:
        if options.require_fencing:
            raise UnsupportedCapability("Redis does not provide strong fencing")
        await self._provider._metadata(options.rpc, wait_deadline)
        if wait_deadline is not None and time.monotonic() >= wait_deadline:
            raise WaitTimeout("contention wait budget elapsed before acquisition dispatch")
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError()
        owner = b"dilock/1/" + secrets.token_hex(32).encode("ascii")
        sent = time.monotonic()
        try:
            async with asyncio.timeout(options.rpc):
                result = await self._provider._client.set(
                    self._key, owner, nx=True, px=options.ttl_ms,
                )
        except ResponseError as exc:
            # A server error reply is known rejection; preserve its category.
            raise BackendUnavailable("acquisition rejected by Redis") from exc
        except (RedisError, TimeoutError, asyncio.CancelledError) as exc:
            try:
                cleanup = await self._provider._cleanup_owner(
                    self._key, owner, options.cleanup,
                )
            except asyncio.CancelledError as cleanup_exc:
                # The shielded cleanup remains provider-owned, but this caller
                # cannot claim it was acknowledged before returning.
                raise AcquisitionUnknown(
                    "dispatched SET cleanup acknowledgement is unknown",
                ) from cleanup_exc
            if cleanup == ReleaseOutcome.UNKNOWN:
                raise AcquisitionUnknown(
                    "dispatched SET has no ownership acknowledgement and cleanup is unknown",
                ) from exc
            if isinstance(exc, asyncio.CancelledError):
                raise
            # An acknowledged cleanup cannot prove that a timed-out or lost SET
            # will not execute later on Redis. Never report a known backend or
            # wait outcome after SET dispatch; the caller must not enter work.
            raise AcquisitionUnknown(
                "dispatched SET outcome remains unknown after owner-checked cleanup",
            ) from exc
        deadline = sent + options.ttl_ms / 1000 - options.margin
        if result is None:
            # SET NX is an atomic contention observation. TYPE only classifies
            # that existing key; it never retries acquisition or changes it.
            timeout = asyncio.timeout(options.rpc)
            try:
                async with timeout:
                    key_type = await self._provider._client.type(self._key)
            except TimeoutError as exc:
                if (timeout.expired() and wait_deadline is not None
                        and time.monotonic() >= wait_deadline):
                    raise WaitTimeout("contention wait budget elapsed during key classification") from exc
                raise BackendUnavailable("Redis contention key type could not be read") from exc
            except RedisError as exc:
                raise BackendUnavailable("Redis contention key type could not be read") from exc
            if key_type not in (b"string", b"none", "string", "none"):
                raise ProtocolMismatch("Redis lock key has an unexpected type")
            return None
        if result is not True or time.monotonic() >= deadline or self._provider._closed:
            await self._provider._cleanup_owner(self._key, owner, options.cleanup)
            raise AcquisitionUnknown("ownership acknowledgement is invalid or too late")
        lease = RedisLease(self._provider, self._key, owner, options, deadline)
        self._provider._leases.add(lease)
        return lease

    async def acquire(
        self, wait: float | AcquireOptions | None = None, options: Options | None = None,
    ) -> RedisLease:
        """Wait budget includes metadata/RPC; only acknowledged busy is retried."""
        if isinstance(wait, AcquireOptions):
            if options is not None:
                raise ValueError("pass either AcquireOptions or wait/options")
            options = wait.lease
            wait = wait.wait
        elif wait is None:
            defaults = self._provider.default_acquisition
            wait = defaults.wait
            if options is None:
                options = defaults.lease
        if options is None:
            options = self._provider.default_acquisition.lease
        if not math.isfinite(wait) or wait <= 0:
            raise ValueError("wait must be finite and positive")
        end = time.monotonic() + wait
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise WaitTimeout("contention wait budget exhausted")
            # The per-attempt deadline is limited by the remaining wait budget.
            max_rpc = options.lease - options.margin - 0.001
            if max_rpc <= 0:
                raise ValueError("lease budget leaves no bounded RPC interval")
            bounded = Options(options.lease, min(options.rpc, remaining, max_rpc), options.cleanup,
                              options.auto_renew, options.require_fencing)
            lease = await self._try_acquire(bounded, wait_deadline=end)
            if lease is not None:
                if time.monotonic() >= end:
                    try:
                        outcome = await lease.release()
                    except asyncio.CancelledError as exc:
                        # The positive SET is known, but interrupted cleanup is
                        # not proof that its remote ownership has ended.
                        raise AcquisitionUnknown("late acquisition cleanup was cancelled") from exc
                    if outcome is ReleaseOutcome.UNKNOWN:
                        raise AcquisitionUnknown("late acquisition cleanup was not acknowledged")
                    raise WaitTimeout("acquisition arrived after wait deadline")
                return lease
            await asyncio.sleep(min(remaining, 0.02 + secrets.randbelow(31) / 1000))

    @asynccontextmanager
    async def hold(
        self, options: AcquireOptions | None = None,
    ) -> AsyncIterator[RedisLease]:
        """Acquire and release through the shared provider in an async scope."""
        lease = await self.acquire(options) if options is not None else await self.acquire()
        try:
            yield lease
        except BaseException:
            await self._scope_release(lease, "body_failed", fail_closed=False)
            raise
        else:
            await self._scope_release(lease, "scope_exit", fail_closed=True)

    async def _scope_release(self, lease: RedisLease, reason: str, *, fail_closed: bool) -> None:
        async def cleanup() -> ReleaseOutcome:
            return await lease.release()

        task = asyncio.create_task(cleanup())
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


class RedisLease:
    """One acquisition, serialized renewal/release; no copy/reacquisition semantics.

    state is a local observation. lost is set once when reliance ends. Explicit
    release reports acknowledgement and uses an independent cleanup budget.
    Context exit performs bounded cleanup without masking a body exception.
    Process death bypasses cleanup; server TTL is the fallback.
    """
    def __init__(self, provider: RedisProvider, key: str, owner: bytes,
                 options: Options, deadline: float) -> None:
        self._provider, self._key, self._owner = provider, key, owner
        self.options, self._deadline = options, deadline
        self._state = State.HELD
        self._gate = asyncio.Lock()
        self.lost = asyncio.Event()
        self.loss_reason: str | None = None
        self._outcome: ReleaseOutcome | None = None
        self._expiry = asyncio.get_running_loop().call_at(deadline, self._expire)
        self._renewal = asyncio.create_task(self._auto()) if options.auto_renew else None

    @property
    def effective_lease_ms(self) -> int:
        """Effective server TTL; not a promise of current remaining validity."""
        return self.options.ttl_ms

    def _expire(self) -> None:
        if self._state == State.HELD and time.monotonic() >= self._deadline:
            self._lose("conservative_deadline_elapsed")

    def _lose(self, reason: str) -> None:
        if self._state == State.HELD:
            self._state = State.LOST
            self.loss_reason = reason
            self.lost.set()

    @property
    def state(self) -> State:
        """Local permission, checked against a monotonic deadline."""
        self._expire()
        return self._state

    def ensure_held(self) -> None:
        """Raise LeaseLost if permission ended; cannot authorize external effects alone."""
        if self.state != State.HELD:
            raise LeaseLost("lease no longer locally held")

    async def renew(self) -> bool:
        """Owner-checked renewal. Failure withdraws local permission and never retries."""
        async with self._gate:
            if self.state != State.HELD:
                return False
            sent = time.monotonic()
            try:
                async with asyncio.timeout(min(self.options.rpc, self._deadline - sent)):
                    result = await self._provider._client.eval(
                        _RENEW, 1, self._key, self._owner, self.options.ttl_ms,
                    )
            except (RedisError, TimeoutError, asyncio.CancelledError):
                self._lose("renewal_unconfirmed")
                raise
            deadline = sent + self.options.ttl_ms / 1000 - self.options.margin
            # Check the OLD deadline too: a late reply cannot revive local permission.
            if self.state != State.HELD or result != 1 or time.monotonic() >= deadline:
                self._lose("renewal_not_owned_or_late")
                return False
            self._deadline = deadline
            self._expiry.cancel()
            self._expiry = asyncio.get_running_loop().call_at(deadline, self._expire)
            return True

    async def _auto(self) -> None:
        try:
            while self.state == State.HELD:
                await asyncio.sleep(self.options.lease / 3)
                if not await self.renew():
                    break
        except (RedisError, TimeoutError, asyncio.CancelledError):
            self._lose("renewal_unconfirmed")

    async def release(self) -> ReleaseOutcome:
        """Withdraw permission before cleanup. Repeat release returns cached observation."""
        # Do not cancel a renewal midway through a mutating RPC; gate waits within its budget.
        if self._renewal is not None:
            self._renewal.cancel()
            await asyncio.gather(self._renewal, return_exceptions=True)
        async with self._gate:
            if self._outcome is not None:
                return self._outcome
            self._state = State.RELEASING
            self._expiry.cancel()
            try:
                self._outcome = await self._provider._cleanup_owner(
                    self._key, self._owner, self.options.cleanup,
                )
            except asyncio.CancelledError:
                # The provider shields dispatched cleanup, so cancellation cannot
                # tell us whether Redis applied the release. Cache that ambiguity
                # before propagating cancellation; a later call must not EVAL again.
                self._outcome = ReleaseOutcome.UNKNOWN
                self._state = State.RELEASED_UNKNOWN
                self._provider._leases.discard(self)
                raise
            self._state = (State.RELEASED_UNKNOWN if self._outcome == ReleaseOutcome.UNKNOWN
                           else State.RELEASED)
            self._provider._leases.discard(self)
            return self._outcome

    async def __aenter__(self) -> RedisLease:
        self.ensure_held()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, tb: TracebackType | None) -> None:
        reason = "body_failed" if exc_type is not None else "scope_exit"
        task = asyncio.create_task(self.release())
        self._provider._track_cleanup(task)
        try:
            outcome = await asyncio.shield(task)
        except BaseException:
            self._provider._observe_cleanup(ReleaseOutcome.UNKNOWN, reason)
            if exc_type is None:
                raise ScopeReleaseError(ReleaseOutcome.UNKNOWN)
            return
        if outcome != ReleaseOutcome.RELEASED:
            self._provider._observe_cleanup(outcome, reason)
            if exc_type is None:
                raise ScopeReleaseError(outcome)
