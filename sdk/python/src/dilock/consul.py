"""Consul HTTP/KV session profile. See spec/backends/CONSUL.md."""
from __future__ import annotations

import asyncio
import base64
import json
import math
import time
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, AsyncIterator, Callable
from urllib.parse import urlsplit

import httpx

from ._metadata import parse_unique_top_level_object
from .core import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, CleanupObservation,
    LeaseLost, Options, ProtocolMismatch, ReleaseOutcome, ScopeReleaseError, State,
    UnsupportedCapability, WaitTimeout, identity, report_cleanup,
)

_PROFILE = "consul-kv-session-v1"
_SCHEME = "sha256-framed-utf8-v1"
_MIN_TTL_SECONDS = 10
_MAX_TTL_SECONDS = 86400
_METADATA_LIMIT = 8192


def _is_exact_metadata(meta: Any, epoch: str) -> bool:
    expected = {
        "protocol_major": 1, "identity_scheme": _SCHEME,
        "profile": _PROFILE, "coordination_epoch": epoch,
        "fixed_lease_ms": None,
    }
    return (
        isinstance(meta, dict)
        and set(meta) == set(expected)
        and type(meta.get("protocol_major")) is int
        and all(meta.get(key) == value for key, value in expected.items())
    )


class ConsulProvider:
    """Owns one HTTP client and attempt sessions against one Consul agent.

    Use ``async with`` or ``await aclose()``. ACL credentials are sent only in
    the request header; this profile has no hidden retries or redirect handling.
    """

    def __init__(
        self, endpoint: str, namespace: str = "default", epoch: str = "", *,
        token: str | None = None,
        default_acquisition: AcquireOptions = AcquireOptions(),
        cleanup_observer: Callable[[CleanupObservation], None] | None = None,
        _transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        identity(namespace, "validation")
        if not isinstance(epoch, str) or not epoch or len(epoch.encode("utf-8", "strict")) > 1024 or "\0" in epoch:
            raise ValueError("epoch must be nonempty strict UTF-8 and at most 1024 bytes")
        base_url = _parse_endpoint(endpoint)
        if token is not None:
            if not isinstance(token, str) or not token or any(
                ord(char) < 0x20 or ord(char) == 0x7F for char in token
            ):
                raise ValueError("token must be a nonempty header-safe string")
            try:
                token.encode("ascii", "strict")
            except UnicodeError as exc:
                raise ValueError("token must contain only ASCII header characters") from exc
        if not isinstance(default_acquisition, AcquireOptions):
            raise ValueError("default_acquisition must be AcquireOptions")
        headers = {"X-Consul-Token": token} if token is not None else {}
        self.namespace, self.epoch = namespace, epoch
        self._default_acquisition = default_acquisition
        self._cleanup_observer = cleanup_observer
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base, headers=headers, follow_redirects=False,
            timeout=None, transport=_transport,
        )
        self._leases: set[ConsulLease] = set()
        self._cleanup: set[asyncio.Task[Any]] = set()
        self._closed = False

    def lock(self, resource: str) -> ConsulHandle:
        if self._closed:
            raise RuntimeError("provider closed")
        return ConsulHandle(self, f"dilock/locks/{identity(self.namespace, resource)}")

    @property
    def default_acquisition(self) -> AcquireOptions:
        return self._default_acquisition

    async def _request(
        self, method: str, path: str, *, timeout: float,
        params: dict[str, str] | None = None, json_body: Any = None,
        content: bytes | None = None, mutation: bool = False,
        allow_not_found: bool = False,
    ) -> httpx.Response:
        """Send one request. Dispatched mutation timeouts remain ambiguous."""
        try:
            async with asyncio.timeout(timeout):
                response = await self._client.request(
                    method, path, params=params, json=json_body, content=content,
                    timeout=httpx.Timeout(timeout),
                )
        except asyncio.CancelledError as exc:
            if mutation:
                raise AcquisitionUnknown("Consul mutation was cancelled after dispatch") from exc
            raise
        except (TimeoutError, httpx.TimeoutException, httpx.RequestError) as exc:
            if mutation:
                raise AcquisitionUnknown("Consul mutation acknowledgement is unknown") from exc
            raise BackendUnavailable("Consul request failed") from exc
        if response.status_code == 404 and allow_not_found:
            return response
        if response.status_code < 200 or response.status_code >= 300:
            if mutation and response.status_code >= 500:
                raise AcquisitionUnknown("Consul mutation response is ambiguous")
            raise BackendUnavailable(f"Consul HTTP request rejected ({response.status_code})")
        return response

    async def _metadata(self, timeout: float) -> None:
        key = f"dilock/namespaces/{self.namespace}/meta"
        response = await self._request("GET", "/v1/kv/" + key.lstrip("/"),
                                       params={"consistent": "true"}, timeout=timeout,
                                       allow_not_found=True)
        if response.status_code == 404:
            raise ProtocolMismatch("missing Consul namespace metadata")
        try:
            rows = response.json()
            if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
                raise ValueError
            row = rows[0]
            if row.get("Key") != key or not isinstance(row.get("Value"), str):
                raise ValueError
            raw = base64.b64decode(row["Value"], validate=True)
            if not raw or len(raw) > _METADATA_LIMIT:
                raise ValueError
            meta = parse_unique_top_level_object(raw)
            if not _is_exact_metadata(meta, self.epoch):
                raise ValueError
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ProtocolMismatch("missing or incompatible Consul namespace metadata") from exc

    async def _session_create(self, ttl_seconds: int, timeout: float) -> tuple[str, float]:
        body = {
            "Name": "dilock lease",
            "Behavior": "release",
            "LockDelay": "0s",
            "TTL": f"{ttl_seconds}s",
            "Checks": [],
        }
        sent = time.monotonic()
        response = await self._request(
            "PUT", "/v1/session/create", json_body=body, timeout=timeout, mutation=True,
        )
        try:
            payload = response.json()
            session_id = payload.get("ID") if isinstance(payload, dict) else None
            if not isinstance(session_id, str) or not session_id or len(session_id) > 128:
                raise ValueError
            return session_id, sent
        except (ValueError, TypeError, AttributeError, json.JSONDecodeError) as exc:
            raise AcquisitionUnknown("Consul session creation response was invalid") from exc

    async def _session_destroy(self, session_id: str, timeout: float) -> bool:
        response = await self._request(
            "PUT", f"/v1/session/destroy/{session_id}", timeout=timeout, mutation=True,
        )
        try:
            result = response.json()
            return isinstance(result, bool)
        except (ValueError, json.JSONDecodeError):
            return False

    async def _session_renew(self, session_id: str, timeout: float) -> float | None:
        sent = time.monotonic()
        response = await self._request(
            "PUT", f"/v1/session/renew/{session_id}", timeout=timeout, mutation=True,
        )
        try:
            rows = response.json()
            return (sent if isinstance(rows, list) and len(rows) == 1
                    and isinstance(rows[0], dict) and rows[0].get("ID") == session_id else None)
        except (ValueError, TypeError, AttributeError, json.JSONDecodeError):
            raise ProtocolMismatch("Consul session renewal response was invalid")

    async def _kv_acquire(self, key: str, session_id: str, timeout: float) -> bool:
        response = await self._request(
            "PUT", "/v1/kv/" + key.lstrip("/"), params={"acquire": session_id},
            content=b"", timeout=timeout, mutation=True,
        )
        try:
            result = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise AcquisitionUnknown("Consul lock acquisition response was invalid") from exc
        if not isinstance(result, bool):
            raise AcquisitionUnknown("Consul lock acquisition response was not boolean")
        return result

    async def _kv_release(self, key: str, session_id: str, timeout: float) -> bool:
        response = await self._request(
            "PUT", "/v1/kv/" + key.lstrip("/"), params={"release": session_id},
            content=b"", timeout=timeout, mutation=True,
        )
        try:
            result = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise AcquisitionUnknown("Consul lock release response was invalid") from exc
        if not isinstance(result, bool):
            raise AcquisitionUnknown("Consul lock release response was not boolean")
        return result

    async def _kv_owner(self, key: str, timeout: float) -> tuple[str, int] | None:
        response = await self._request(
            "GET", "/v1/kv/" + key.lstrip("/"), params={"consistent": "true"},
            timeout=timeout, allow_not_found=True,
        )
        if response.status_code == 404:
            return None
        try:
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                return None
            if len(rows) != 1 or not isinstance(rows[0], dict) or rows[0].get("Key") != key:
                raise ValueError
            session = rows[0].get("Session")
            lock_index = rows[0].get("LockIndex")
            if (session is not None and not isinstance(session, str)) or type(lock_index) is not int or lock_index < 0:
                raise ValueError
            return session or "", lock_index
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ProtocolMismatch("Consul lock observation was malformed") from exc

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.gather(*(lease.release() for lease in tuple(self._leases)), return_exceptions=True)
        if self._cleanup:
            await asyncio.gather(*tuple(self._cleanup), return_exceptions=True)
        await self._client.aclose()

    def _observe_cleanup(self, outcome: ReleaseOutcome, reason: str) -> None:
        report_cleanup(self._cleanup_observer, "consul", outcome, reason)

    def _track_cleanup(self, task: asyncio.Task[Any]) -> None:
        self._cleanup.add(task)
        task.add_done_callback(self._cleanup.discard)

    async def __aenter__(self) -> ConsulProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class ConsulHandle:
    """Reusable key; each acquisition owns a fresh Consul session."""

    def __init__(self, provider: ConsulProvider, key: str) -> None:
        self._provider, self._key = provider, key

    async def try_acquire(self, options: Options | None = None) -> ConsulLease | None:
        return await self._acquire(options or self._provider.default_acquisition.lease,
                                   time.monotonic() + (options or self._provider.default_acquisition.lease).rpc,
                                   trying=True)

    async def acquire(
        self, wait: float | AcquireOptions | None = None, options: Options | None = None,
    ) -> ConsulLease:
        if self._provider._closed:
            raise RuntimeError("provider closed")
        defaults = self._provider.default_acquisition
        if isinstance(wait, AcquireOptions):
            if options is not None:
                raise ValueError("pass either AcquireOptions or wait/options")
            options, wait = wait.lease, wait.wait
        elif wait is None:
            wait = defaults.wait
            options = options or defaults.lease
        options = options or defaults.lease
        if not isinstance(options, Options):
            raise ValueError("options must be Options")
        if not math.isfinite(wait) or wait <= 0:
            raise ValueError("wait must be finite and positive")
        self._validate_options(options)
        deadline = time.monotonic() + wait
        await self._provider._metadata(min(options.rpc, max(.001, deadline - time.monotonic())))
        session_id, session_sent = await self._provider._session_create(
            _ttl_seconds(options), min(options.rpc, max(.001, deadline - time.monotonic())),
        )
        return await self._attempt(session_id, session_sent, options, deadline, trying=False)

    @staticmethod
    def _validate_options(options: Options) -> None:
        ttl = _ttl_seconds(options)
        if not _MIN_TTL_SECONDS <= ttl <= _MAX_TTL_SECONDS:
            raise UnsupportedCapability("Consul sessions support lease TTLs from 10s to 24h")
        if options.require_fencing is False:
            return
        # LockIndex is the Consul profile's supported per-key fence. Its safety
        # still depends on the operator retaining the key and its epoch.

    async def _acquire(
        self, options: Options, deadline: float, *, trying: bool,
    ) -> ConsulLease | None:
        self._validate_options(options)
        p = self._provider
        if p._closed:
            raise RuntimeError("provider closed")
        await p._metadata(min(options.rpc, max(.001, deadline - time.monotonic())))
        session_id, session_sent = await p._session_create(
            _ttl_seconds(options), min(options.rpc, max(.001, deadline - time.monotonic())),
        )
        return await self._attempt(session_id, session_sent, options, deadline, trying=trying)

    async def _attempt(
        self, session_id: str, session_sent: float, options: Options,
        deadline: float, *, trying: bool,
    ) -> ConsulLease | None:
        p = self._provider
        ttl_seconds = _ttl_seconds(options)
        next_renew = time.monotonic() + ttl_seconds / 3
        session_deadline = session_sent + ttl_seconds - max(.01, ttl_seconds * .01)
        try:
            while True:
                now = time.monotonic()
                remaining = min(deadline - now, session_deadline - now)
                if deadline <= now:
                    raise WaitTimeout("Consul acquisition wait budget exhausted")
                if session_deadline <= now:
                    raise BackendUnavailable("Consul acquisition session expired before lock acquisition")
                acquired = await p._kv_acquire(self._key, session_id, min(options.rpc, remaining))
                if acquired:
                    observed = await p._kv_owner(self._key, min(options.rpc, max(.001, deadline - time.monotonic())))
                    if observed is None or observed[0] != session_id or observed[1] <= 0:
                        raise AcquisitionUnknown("Consul could not confirm lock ownership")
                    ttl = ttl_seconds
                    local_deadline = session_deadline
                    if time.monotonic() >= min(local_deadline, deadline):
                        raise AcquisitionUnknown("Consul acquisition arrived beyond its conservative budget")
                    lease = ConsulLease(p, self._key, session_id, observed[1], options, ttl, local_deadline)
                    p._leases.add(lease)
                    if options.auto_renew:
                        lease._keeper = asyncio.create_task(lease._keepalive())
                    return lease
                if trying:
                    await _cleanup_session(p, session_id, options.cleanup)
                    return None
                now = time.monotonic()
                if now >= deadline:
                    raise WaitTimeout("Consul contention wait budget exhausted")
                if now >= next_renew:
                    renewed_at = await p._session_renew(session_id, min(options.rpc, deadline - now))
                    if renewed_at is None:
                        raise BackendUnavailable("Consul waiting session expired before acquisition")
                    session_deadline = renewed_at + ttl_seconds - max(.01, ttl_seconds * .01)
                    next_renew = time.monotonic() + ttl_seconds / 3
                await asyncio.sleep(min(deadline - time.monotonic(), max(.02, min(.25, next_renew - time.monotonic()))))
        except BaseException as exc:
            cleanup = asyncio.create_task(_cleanup_session(p, session_id, options.cleanup))
            p._track_cleanup(cleanup)
            try:
                await asyncio.shield(cleanup)
            except BaseException:
                if isinstance(exc, (asyncio.CancelledError, WaitTimeout)):
                    raise AcquisitionUnknown("Consul acquisition cleanup acknowledgement is unknown") from exc
            raise

    @asynccontextmanager
    async def hold(self, options: AcquireOptions | None = None) -> AsyncIterator[ConsulLease]:
        lease = await self.acquire(options) if options is not None else await self.acquire()
        try:
            yield lease
        except BaseException:
            await self._scope_release(lease, "body_failed", fail_closed=False)
            raise
        else:
            await self._scope_release(lease, "scope_exit", fail_closed=True)

    async def _scope_release(self, lease: ConsulLease, reason: str, *, fail_closed: bool) -> None:
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


class ConsulLease:
    """One locally bounded lease backed by a private Consul TTL session."""

    def __init__(
        self, provider: ConsulProvider, key: str, session_id: str, lock_index: int,
        options: Options, ttl_seconds: int, deadline: float,
    ) -> None:
        self._provider, self._key, self._session_id = provider, key, session_id
        self._lock_index, self.options = lock_index, options
        self.effective_lease_ms = ttl_seconds * 1000
        self._ttl, self._deadline = float(ttl_seconds), deadline
        self._state = State.HELD
        self._outcome: ReleaseOutcome | None = None
        self._gate = asyncio.Lock()
        self._keeper: asyncio.Task[None] | None = None
        self.lost = asyncio.Event()

    @property
    def state(self) -> State:
        if self._state == State.HELD and time.monotonic() >= self._deadline:
            self._state = State.LOST
            self.lost.set()
        return self._state

    @property
    def fence(self) -> tuple[str, int]:
        if self.state != State.HELD:
            raise LeaseLost("Consul lease is no longer locally held")
        return self._provider.epoch, self._lock_index

    async def _verify(self, timeout: float) -> bool:
        owner = await self._provider._kv_owner(self._key, timeout)
        return owner == (self._session_id, self._lock_index)

    async def renew(self) -> bool:
        async with self._gate:
            if self.state != State.HELD:
                return False
            prior_deadline = self._deadline
            sent = time.monotonic()
            try:
                renewed = await self._provider._session_renew(self._session_id, self.options.rpc)
                if not renewed or not await self._verify(self.options.rpc):
                    self._state = State.LOST
                    self.lost.set()
                    return False
            except (BackendUnavailable, ProtocolMismatch):
                if time.monotonic() >= prior_deadline:
                    self._state = State.LOST
                    self.lost.set()
                    return False
                raise
            if time.monotonic() >= prior_deadline:
                self._state = State.LOST
                self.lost.set()
                return False
            deadline = sent + self._ttl - max(.01, self._ttl * .01)
            if time.monotonic() >= deadline:
                self._state = State.LOST
                self.lost.set()
                return False
            self._deadline = deadline
            return self.state == State.HELD

    async def _keepalive(self) -> None:
        try:
            while self.state == State.HELD:
                await asyncio.sleep(max(.05, self._ttl / 3))
                if self.state != State.HELD:
                    break
                try:
                    await self.renew()
                except (BackendUnavailable, ProtocolMismatch):
                    if time.monotonic() >= self._deadline:
                        self._state = State.LOST
                        self.lost.set()
                        break
        except asyncio.CancelledError:
            pass

    async def release(self) -> ReleaseOutcome:
        async with self._gate:
            if self._outcome is not None:
                return self._outcome
            self._state = State.RELEASING
            deadline = time.monotonic() + self.options.cleanup
            if self._keeper is not None and self._keeper is not asyncio.current_task():
                self._keeper.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(self._keeper), _remaining(deadline))
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    if current is not None and current.cancelling():
                        self._outcome = ReleaseOutcome.UNKNOWN
                        self._state = State.RELEASED_UNKNOWN
                        self._provider._leases.discard(self)
                        raise
                except TimeoutError:
                    pass
            outcome = ReleaseOutcome.UNKNOWN
            try:
                released = await self._provider._kv_release(
                    self._key, self._session_id, _remaining(deadline),
                )
                if released:
                    outcome = ReleaseOutcome.RELEASED
                else:
                    owner = await self._provider._kv_owner(self._key, _remaining(deadline))
                    if owner is None or owner[0] != self._session_id:
                        outcome = ReleaseOutcome.ALREADY_NOT_OWNED
            except (AcquisitionUnknown, BackendUnavailable, ProtocolMismatch, TimeoutError,
                    asyncio.CancelledError):
                outcome = ReleaseOutcome.UNKNOWN
            destroy_deadline = time.monotonic() + self.options.cleanup
            try:
                destroyed = await self._provider._session_destroy(
                    self._session_id, _remaining(destroy_deadline),
                )
                if not destroyed:
                    outcome = ReleaseOutcome.UNKNOWN
            except (AcquisitionUnknown, BackendUnavailable, ProtocolMismatch, TimeoutError,
                    asyncio.CancelledError):
                outcome = ReleaseOutcome.UNKNOWN
            self._outcome = outcome
            self._state = State.RELEASED if outcome != ReleaseOutcome.UNKNOWN else State.RELEASED_UNKNOWN
            self._provider._leases.discard(self)
            return outcome

    async def __aenter__(self) -> ConsulLease:
        if self.state != State.HELD:
            raise LeaseLost("Consul lease no longer locally held")
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


async def _cleanup_session(provider: ConsulProvider, session_id: str, budget: float) -> None:
    deadline = time.monotonic() + budget
    try:
        await provider._session_destroy(session_id, _remaining(deadline))
    except (AcquisitionUnknown, BackendUnavailable, TimeoutError):
        # The server TTL bounds an orphan session; callers retain unknown status
        # after dispatched, unacknowledged acquisition mutations.
        pass


def _ttl_seconds(options: Options) -> int:
    if not math.isfinite(options.lease):
        raise ValueError("lease must be finite")
    if options.lease < _MIN_TTL_SECONDS:
        raise UnsupportedCapability("Consul sessions support lease TTLs from 10s to 24h")
    seconds = math.ceil(options.lease)
    if not _MIN_TTL_SECONDS <= seconds <= _MAX_TTL_SECONDS:
        raise UnsupportedCapability("Consul sessions support lease TTLs from 10s to 24h")
    return seconds


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Consul cleanup budget exhausted")
    return remaining


def _parse_endpoint(endpoint: str) -> str:
    if not isinstance(endpoint, str) or not endpoint:
        raise ValueError("endpoint must be an HTTP(S) Consul agent URL")
    try:
        parsed = urlsplit(endpoint)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("endpoint must be a valid HTTP(S) URL") from exc
    if (parsed.scheme.lower() not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError("endpoint must be an HTTP(S) URL without credentials, query, or fragment")
    return f"{parsed.scheme.lower()}://{parsed.netloc}"
