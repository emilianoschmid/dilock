"""etcd native Lock profile using the official v3 gRPC wire services."""
from __future__ import annotations

import asyncio
import math
import time
from contextlib import asynccontextmanager
from types import TracebackType
from typing import AsyncIterator, Callable
from urllib.parse import urlsplit

import grpc

from .core import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, CleanupObservation,
    LeaseLost, Options, ProtocolMismatch, ScopeReleaseError,
    ReleaseOutcome, State, UnsupportedCapability, WaitTimeout, identity, report_cleanup,
)
from ._metadata import parse_unique_top_level_object
from ._etcd_proto import rpc_pb2, rpc_pb2_grpc, v3lock_pb2, v3lock_pb2_grpc

_PROFILE = "etcd-native-lock-v1"
_SCHEME = "sha256-framed-utf8-v1"


class EtcdProvider:
    """Owns an asyncio gRPC channel and leases. Endpoint must name one etcd cluster.

    The epoch is operator-approved and provisioned in namespace metadata. This
    single-member functional profile makes no quorum or failover claim. An HTTPS
    endpoint uses gRPC's system trust roots; custom roots and client certificates
    are not configurable in this profile.
    """

    def __init__(
        self, endpoint: str, namespace: str = "default", epoch: str = "", *,
        default_acquisition: AcquireOptions = AcquireOptions(),
        cleanup_observer: Callable[[CleanupObservation], None] | None = None,
    ) -> None:
        identity(namespace, "validation")
        if not epoch or len(epoch.encode("utf-8", "strict")) > 1024 or "\0" in epoch:
            raise ValueError("epoch must be nonempty, strict UTF-8 and at most 1024 bytes")
        target, secure = _parse_endpoint(endpoint)
        if not isinstance(default_acquisition, AcquireOptions):
            raise ValueError("default_acquisition must be AcquireOptions")
        self.namespace, self.epoch = namespace, epoch
        self._default_acquisition = default_acquisition
        self._cleanup_observer = cleanup_observer
        self._cleanup: set[asyncio.Task[object]] = set()
        if secure:
            # Use gRPC's default system roots. Custom roots/client certificates are not
            # currently configurable by this profile.
            self._channel = grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
        else:
            self._channel = grpc.aio.insecure_channel(target)
        self._kv = rpc_pb2_grpc.KVStub(self._channel)
        self._leases_rpc = rpc_pb2_grpc.LeaseStub(self._channel)
        self._locks = v3lock_pb2_grpc.LockStub(self._channel)
        self._leases: set[EtcdLease] = set()
        self._closed = False

    def lock(self, resource: str) -> EtcdHandle:
        """Validate a resource and return a reusable, non-reentrant handle."""
        if self._closed:
            raise RuntimeError("provider closed")
        return EtcdHandle(self, b"/dilock/locks/" + identity(self.namespace, resource).encode("ascii"))

    @property
    def default_acquisition(self) -> AcquireOptions:
        """Immutable acquisition defaults captured at provider creation."""
        return self._default_acquisition

    async def _metadata(self, timeout: float) -> None:
        key = f"/dilock/namespaces/{self.namespace}/meta".encode()
        try:
            response = await self._kv.Range(rpc_pb2.RangeRequest(key=key), timeout=timeout)
        except grpc.aio.AioRpcError as exc:
            raise BackendUnavailable("etcd namespace metadata read failed") from exc
        if response.count != 1 or len(response.kvs) != 1 or response.kvs[0].key != key:
            raise ProtocolMismatch("missing etcd namespace metadata")
        try:
            raw = response.kvs[0].value
            if not raw or len(raw) > 8192:
                raise ValueError
            data = parse_unique_top_level_object(raw)
            expected = {"protocol_major": 1, "identity_scheme": _SCHEME,
                        "profile": _PROFILE, "coordination_epoch": self.epoch,
                        "fixed_lease_ms": None}
            if not isinstance(data, dict) or any(data.get(k) != v for k, v in expected.items()):
                raise ValueError
        except (ValueError, TypeError, UnicodeError) as exc:
            raise ProtocolMismatch("incompatible etcd namespace metadata") from exc

    async def aclose(self) -> None:
        """Release owned leases with independent bounded cleanup, then close the channel."""
        self._closed = True
        await asyncio.gather(*(lease.release() for lease in tuple(self._leases)), return_exceptions=True)
        if self._cleanup:
            await asyncio.gather(*tuple(self._cleanup), return_exceptions=True)
        await self._channel.close(grace=1.0)

    def _observe_cleanup(self, outcome: ReleaseOutcome, reason: str) -> None:
        report_cleanup(self._cleanup_observer, "etcd", outcome, reason)

    def _track_cleanup(self, task: asyncio.Task[object]) -> None:
        self._cleanup.add(task)
        task.add_done_callback(self._cleanup.discard)

    async def __aenter__(self) -> EtcdProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class EtcdHandle:
    """Reusable etcd native lock name; each attempt gets a fresh server lease."""

    def __init__(self, provider: EtcdProvider, name: bytes) -> None:
        self._provider, self._name = provider, name

    async def try_acquire(self, options: Options | None = None) -> EtcdLease | None:
        """Try once; only confirmed queued contention returns None."""
        if options is None:
            options = self._provider.default_acquisition.lease
        return await self._acquire(options, options.rpc, True)

    async def acquire(
        self, wait: float | AcquireOptions | None = None, options: Options | None = None,
    ) -> EtcdLease:
        """Queue until acquired or the overall wait budget expires."""
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
        lease = await self._acquire(options, wait, False)
        if lease is None:
            raise WaitTimeout("etcd contention wait budget exhausted")
        return lease

    @asynccontextmanager
    async def hold(
        self, options: AcquireOptions | None = None,
    ) -> AsyncIterator[EtcdLease]:
        """Acquire and release through the shared provider in an async scope."""
        lease = await self.acquire(options) if options is not None else await self.acquire()
        try:
            yield lease
        except BaseException:
            await self._scope_release(lease, "body_failed", fail_closed=False)
            raise
        else:
            await self._scope_release(lease, "scope_exit", fail_closed=True)

    async def _scope_release(self, lease: EtcdLease, reason: str, *, fail_closed: bool) -> None:
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

    async def _acquire(self, options: Options, timeout: float, trying: bool) -> EtcdLease | None:
        p = self._provider
        if p._closed:
            raise RuntimeError("provider closed")
        if options.require_fencing is False:
            pass
        else:
            # etcd create_revision is returned as an epoch-scoped fence by this profile.
            pass
        start = time.monotonic()
        deadline = start + timeout
        await p._metadata(min(options.rpc, max(0.001, deadline - time.monotonic())))
        if options.require_fencing and not p.epoch:
            raise UnsupportedCapability("etcd fencing requires an approved epoch")
        ttl = math.ceil(options.lease)
        if ttl <= 0:
            raise ValueError("etcd lease must be at least one second")
        try:
            grant_budget = min(options.rpc, deadline - time.monotonic())
            if grant_budget <= 0:
                raise WaitTimeout("etcd acquisition budget expired before lease grant")
            grant = await p._leases_rpc.LeaseGrant(rpc_pb2.LeaseGrantRequest(TTL=ttl), timeout=grant_budget)
        except asyncio.CancelledError as exc:
            # LeaseGrant may have committed before cancellation suppressed its
            # response. There is no lease ID available for owner-specific revoke,
            # so do not expose ordinary cancellation as proof that nothing exists.
            raise AcquisitionUnknown(
                "cancelled etcd lease grant has an unknown outcome; an orphan may remain until TTL expiry",
            ) from exc
        except grpc.aio.AioRpcError as exc:
            raise AcquisitionUnknown("etcd lease grant acknowledgement is unknown") from exc
        if grant.ID == 0 or grant.TTL <= 0:
            raise ProtocolMismatch("etcd returned an invalid lease grant")
        lease = EtcdLease(p, self._name, grant.ID, grant.TTL, options, start)
        p._leases.add(lease)
        lease._keeper = asyncio.create_task(lease._keepalive())
        lock_budget = deadline - time.monotonic()
        if lock_budget <= 0:
            await _abandon_uncancellable(lease)
            raise WaitTimeout("etcd acquisition budget expired before Lock")
        try:
            locked = await p._locks.Lock(v3lock_pb2.LockRequest(name=self._name, lease=grant.ID), timeout=lock_budget)
        except grpc.aio.AioRpcError as exc:
            await _abandon_uncancellable(lease)
            if trying and exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                try:
                    queue = await p._kv.Range(rpc_pb2.RangeRequest(key=self._name + b"/", range_end=self._name + b"0"), timeout=options.cleanup)
                except grpc.aio.AioRpcError as probe:
                    raise AcquisitionUnknown("etcd try result and contention probe are unknown") from probe
                if queue.count:
                    return None
                raise AcquisitionUnknown("etcd Lock timed out without confirmed contention") from exc
            if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED and not trying:
                raise WaitTimeout("etcd contention wait budget exhausted") from exc
            # CANCELLED is a transport/backend outcome, not evidence that another
            # owner holds the lock. The attempt lease was revoked above.
            raise BackendUnavailable("etcd Lock RPC failed") from exc
        except asyncio.CancelledError as exc:
            try:
                await _abandon_uncancellable(lease)
            except AcquisitionUnknown as cleanup:
                raise AcquisitionUnknown("cancelled etcd Lock waiter cleanup is unknown") from cleanup
            raise AcquisitionUnknown("cancelled etcd Lock waiter was revoked") from exc
        try:
            if not locked.key:
                raise ProtocolMismatch("etcd Lock returned an empty key")
            lease._key = locked.key
            verification_budget = min(options.rpc, deadline - time.monotonic())
            if verification_budget <= 0:
                raise WaitTimeout("etcd acquisition budget expired before ownership verification")
            verified = await p._kv.Range(
                rpc_pb2.RangeRequest(key=locked.key), timeout=verification_budget,
            )
            if verified.count != 1 or len(verified.kvs) != 1 or verified.kvs[0].lease != grant.ID:
                raise ProtocolMismatch("etcd Lock key is missing or attached to a different lease")
            lease.fence_revision = verified.kvs[0].create_revision
            if lease.state != State.HELD:
                raise AcquisitionUnknown("etcd lock verification exceeded the conservative lease budget")
            if not options.auto_renew:
                lease._stop_keepalive = True
                if lease._keeper is not None:
                    lease._keeper.cancel()
                    await asyncio.gather(lease._keeper, return_exceptions=True)
                    lease._keeper = None
            # Verification and keepalive shutdown are part of the acquisition
            # budget. A positive late reply must never authorize user work.
            if time.monotonic() >= deadline:
                raise WaitTimeout("etcd acquisition arrived after wait deadline")
            if lease.state != State.HELD:
                raise AcquisitionUnknown("etcd acquisition exceeded the conservative lease budget")
            return lease
        except grpc.aio.AioRpcError as exc:
            await _abandon_uncancellable(lease)
            if time.monotonic() >= deadline:
                raise WaitTimeout("etcd acquisition budget expired during ownership verification") from exc
            raise AcquisitionUnknown("etcd lock verification was not acknowledged") from exc
        except BaseException:
            # Lock has already returned an opaque key, so every exit (including
            # caller cancellation) must revoke this unreturned acquisition.
            await _abandon_uncancellable(lease)
            raise


class EtcdLease:
    """One non-copyable logical acquisition; loss withdraws local permission only."""

    def __init__(self, provider: EtcdProvider, name: bytes, lease_id: int,
                 ttl: int, options: Options, sent: float) -> None:
        self._provider, self._name, self._id = provider, name, lease_id
        self._ttl, self.options = ttl, options
        self._deadline = sent + ttl - max(0.01, ttl * 0.01)
        self._key: bytes | None = None
        self.fence_revision = 0
        self._state = State.HELD
        self._gate = asyncio.Lock()
        self._keeper: asyncio.Task[None] | None = None
        self._stop_keepalive = False
        self._outcome: ReleaseOutcome | None = None
        self.lost = asyncio.Event()

    @property
    def effective_lease_ms(self) -> int:
        return self._ttl * 1000

    @property
    def fence(self) -> tuple[str, int]:
        """Epoch-scoped etcd create_revision; resource-side atomic checks are required."""
        return self._provider.epoch, self.fence_revision

    @property
    def state(self) -> State:
        if self._state == State.HELD and time.monotonic() >= self._deadline:
            self._state = State.LOST
            self.lost.set()
        return self._state

    async def _keepalive(self) -> None:
        call = self._provider._leases_rpc.LeaseKeepAlive()
        try:
            while self._state == State.HELD:
                previous_deadline = self._deadline
                sent = time.monotonic()
                await call.write(rpc_pb2.LeaseKeepAliveRequest(ID=self._id))
                reply = await asyncio.wait_for(call.read(), max(0.05, min(self.options.rpc, self._ttl / 3)))
                if reply is grpc.aio.EOF or reply.ID != self._id or reply.TTL <= 0:
                    raise BackendUnavailable("etcd lease keepalive ended")
                deadline = sent + reply.TTL - max(0.01, reply.TTL * 0.01)
                if time.monotonic() >= previous_deadline:
                    raise BackendUnavailable("etcd lease keepalive arrived after the prior confirmed validity budget")
                if time.monotonic() >= deadline:
                    raise BackendUnavailable("etcd lease keepalive reply arrived too late")
                self._ttl, self._deadline = reply.TTL, deadline
                await asyncio.sleep(max(0.05, reply.TTL / 3))
        except asyncio.CancelledError:
            if self._state == State.HELD and not self._stop_keepalive:
                self._state = State.LOST
                self.lost.set()
        except (grpc.aio.AioRpcError, TimeoutError, BackendUnavailable):
            if self._state == State.HELD:
                self._state = State.LOST
                self.lost.set()
        finally:
            call.cancel()

    async def renew(self) -> bool:
        """Extend this lease with a bounded native keepalive acknowledgement."""
        async with self._gate:
            if self.state != State.HELD or self._key is None:
                return False
            previous_deadline = self._deadline
            sent = time.monotonic()
            call = self._provider._leases_rpc.LeaseKeepAlive()
            try:
                await call.write(rpc_pb2.LeaseKeepAliveRequest(ID=self._id))
                result = await asyncio.wait_for(call.read(), self.options.rpc)
            except (grpc.aio.AioRpcError, TimeoutError, asyncio.CancelledError) as exc:
                self._state = State.LOST
                self.lost.set()
                raise BackendUnavailable("etcd lease ownership could not be confirmed") from exc
            finally:
                call.cancel()
            if result is grpc.aio.EOF or result.ID != self._id or result.TTL <= 0:
                self._state = State.LOST
                self.lost.set()
                return False
            # A keepalive acknowledgement cannot restore a lease after the last
            # locally confirmed validity budget has already elapsed.
            if time.monotonic() >= previous_deadline:
                self._state = State.LOST
                self.lost.set()
                return False
            new_deadline = sent + result.TTL - max(0.01, result.TTL * 0.01)
            if time.monotonic() >= new_deadline:
                self._state = State.LOST
                self.lost.set()
                return False
            self._ttl, self._deadline = result.TTL, new_deadline
            return self.state == State.HELD

    async def _abandon(self) -> None:
        """Revoke this attempt within one total cleanup budget."""
        cleanup_deadline = time.monotonic() + self.options.cleanup
        if self._state == State.HELD:
            self._state = State.RELEASING
        if self._keeper is not None:
            self._keeper.cancel()
            # A cancelled gRPC stream normally exits immediately, but cleanup
            # must remain bounded even if its task is slow to unwind.
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._keeper),
                    timeout=_remaining_cleanup(cleanup_deadline),
                )
            except (asyncio.CancelledError, TimeoutError):
                pass
        revoke_budget = _remaining_cleanup(cleanup_deadline)
        if revoke_budget <= 0:
            self._state = State.LOST
            self.lost.set()
            self._provider._leases.discard(self)
            raise AcquisitionUnknown("etcd lease cleanup budget expired before revoke acknowledgement")
        try:
            await self._provider._leases_rpc.LeaseRevoke(
                rpc_pb2.LeaseRevokeRequest(ID=self._id), timeout=revoke_budget)
        except (grpc.aio.AioRpcError, TimeoutError) as exc:
            self._state = State.LOST
            self.lost.set()
            self._provider._leases.discard(self)
            raise AcquisitionUnknown("etcd lease cleanup was not acknowledged") from exc
        self._state = State.RELEASED
        self._provider._leases.discard(self)

    async def release(self) -> ReleaseOutcome:
        """Unlock and revoke under one total monotonic cleanup budget."""
        async with self._gate:
            if self._outcome is not None:
                return self._outcome
            self._state = State.RELEASING
            cleanup_deadline = time.monotonic() + self.options.cleanup
            if self._keeper is not None:
                self._keeper.cancel()
                # Keepalive shutdown is part of release cleanup too. A stream
                # that is slow to observe cancellation must not silently add an
                # unbounded phase before Unlock and LeaseRevoke.
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._keeper),
                        timeout=_remaining_cleanup(cleanup_deadline),
                    )
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    if (not self._keeper.cancelled() or
                            (current is not None and current.cancelling())):
                        self._outcome = ReleaseOutcome.UNKNOWN
                        self._state = State.RELEASED_UNKNOWN
                        self._provider._leases.discard(self)
                        raise
                except TimeoutError:
                    self._outcome = ReleaseOutcome.UNKNOWN
                    self._state = State.RELEASED_UNKNOWN
                    self._provider._leases.discard(self)
                    return self._outcome
                except Exception:
                    # Like gather(return_exceptions=True), a failed stream
                    # shutdown must not prevent owner-specific lock cleanup.
                    pass

            def remaining_cleanup() -> float:
                return max(0.0, cleanup_deadline - time.monotonic())

            ownership: bool | None = None
            range_budget = remaining_cleanup()
            if self._key and range_budget > 0:
                try:
                    observed = await self._provider._kv.Range(
                        rpc_pb2.RangeRequest(key=self._key), timeout=range_budget,
                    )
                    ownership = any(
                        item.key == self._key and item.lease == self._id
                        for item in observed.kvs
                    )
                except asyncio.CancelledError:
                    self._outcome = ReleaseOutcome.UNKNOWN
                    self._state = State.RELEASED_UNKNOWN
                    self._provider._leases.discard(self)
                    raise
                except (grpc.aio.AioRpcError, TimeoutError):
                    pass

            unlock_acknowledged = False
            unlock_budget = remaining_cleanup()
            if self._key and ownership is not False and unlock_budget > 0:
                try:
                    await self._provider._locks.Unlock(
                        v3lock_pb2.UnlockRequest(key=self._key), timeout=unlock_budget)
                    unlock_acknowledged = True
                except asyncio.CancelledError:
                    self._outcome = ReleaseOutcome.UNKNOWN
                    self._state = State.RELEASED_UNKNOWN
                    self._provider._leases.discard(self)
                    raise
                except (grpc.aio.AioRpcError, TimeoutError):
                    pass
            revoke_acknowledged = False
            revoke_not_found = False
            revoke_budget = remaining_cleanup()
            if revoke_budget > 0:
                try:
                    await self._provider._leases_rpc.LeaseRevoke(
                        rpc_pb2.LeaseRevokeRequest(ID=self._id), timeout=revoke_budget)
                    revoke_acknowledged = True
                except asyncio.CancelledError:
                    self._outcome = ReleaseOutcome.UNKNOWN
                    self._state = State.RELEASED_UNKNOWN
                    self._provider._leases.discard(self)
                    raise
                except grpc.aio.AioRpcError as exc:
                    code = getattr(exc, "code", None)
                    revoke_not_found = code is not None and code() is grpc.StatusCode.NOT_FOUND
                except TimeoutError:
                    pass
            # etcd Unlock can acknowledge a no-op after lease expiry. Require
            # exact-key ownership evidence plus both cleanup acknowledgements
            # before reporting Released. A confirmed absence or unique-lease
            # revoke means AlreadyNotOwned; uncertain ownership stays Unknown.
            self._outcome = (
                ReleaseOutcome.ALREADY_NOT_OWNED
                if revoke_not_found or ownership is False else
                ReleaseOutcome.RELEASED
                if ownership is True and unlock_acknowledged and revoke_acknowledged else
                ReleaseOutcome.ALREADY_NOT_OWNED
                if ownership is True and revoke_acknowledged else
                ReleaseOutcome.UNKNOWN
            )
            self._state = (
                State.RELEASED if self._outcome is not ReleaseOutcome.UNKNOWN
                else State.RELEASED_UNKNOWN
            )
            self._provider._leases.discard(self)
            return self._outcome

    async def __aenter__(self) -> EtcdLease:
        if self.state != State.HELD:
            raise LeaseLost("etcd lease no longer locally held")
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


def _parse_endpoint(endpoint: str) -> tuple[str, bool]:
    """Return a validated gRPC target and whether TLS is required by the URI."""
    if not isinstance(endpoint, str) or not endpoint:
        raise ValueError("endpoint must be a gRPC host:port or http(s) URI")
    candidate = endpoint if "://" in endpoint else f"http://{endpoint}"
    try:
        parsed = urlsplit(candidate)
        # Accessing .port validates malformed and out-of-range ports.
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("endpoint must be a valid gRPC host:port") from exc
    if (parsed.scheme not in ("http", "https") or not parsed.hostname or
            parsed.username is not None or parsed.password is not None or
            parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError("endpoint must be a gRPC host:port without credentials or path")
    target = parsed.netloc if parsed.port is not None else f"{parsed.netloc}:2379"
    return target, parsed.scheme == "https"


async def _abandon_uncancellable(lease: EtcdLease) -> None:
    """Finish bounded lease cleanup even when the acquisition task is cancelled."""
    cleanup = asyncio.create_task(lease._abandon())
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        # The cancellation which entered the surrounding handler has been
        # delivered; shielding lets its independent cleanup finish before rethrow.
        await asyncio.shield(cleanup)


def _remaining_cleanup(deadline: float, now: float | None = None) -> float:
    """Return a non-negative remainder for a monotonic cleanup deadline."""
    return max(0.0, deadline - (time.monotonic() if now is None else now))
