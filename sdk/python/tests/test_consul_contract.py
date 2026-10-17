"""Consul HTTP contract tests using a deterministic in-process wire peer."""
from __future__ import annotations

import asyncio
import base64
import json
import time
from typing import Any

import httpx
import pytest

from dilock import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, Options,
    ProtocolMismatch, ReleaseOutcome, UnsupportedCapability, WaitTimeout,
)
from dilock.consul import ConsulProvider, _is_exact_metadata


class ConsulPeer:
    def __init__(self, namespace: str = "test", epoch: str = "epoch") -> None:
        self.meta_key = f"dilock/namespaces/{namespace}/meta"
        self.meta = {
            "protocol_major": 1,
            "identity_scheme": "sha256-framed-utf8-v1",
            "profile": "consul-kv-session-v1",
            "coordination_epoch": epoch,
            "fixed_lease_ms": None,
        }
        self.sessions: dict[str, dict[str, Any]] = {}
        self.locks: dict[str, dict[str, Any]] = {}
        self.calls: list[httpx.Request] = []
        self.fail_acquire = False
        self.fail_destroy = False
        self.next_session = 0
        self.create_response_delay = 0.0
        self.session_created_at: dict[str, float] = {}

    def _row(self, key: str, value: dict[str, Any]) -> dict[str, Any]:
        stored = value.get("Value", value)
        if isinstance(stored, bytes):
            raw = stored
        elif isinstance(stored, str):
            raw = stored.encode()
        else:
            raw = json.dumps(stored, separators=(",", ":")).encode()
        return {
            "Key": key,
            "Value": base64.b64encode(raw).decode(),
            "Session": value.get("Session", ""),
            "LockIndex": value.get("LockIndex", 0),
        }

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        path, query = request.url.path, request.url.params
        if path == "/v1/kv/" + self.meta_key and request.method == "GET":
            return httpx.Response(200, json=[self._row(self.meta_key, {"Value": self.meta})])
        if path == "/v1/session/create" and request.method == "PUT":
            body = json.loads(request.content)
            assert body["Behavior"] == "release" and body["LockDelay"] == "0s"
            assert body["TTL"].endswith("s") and body["Checks"] == []
            self.next_session += 1
            session_id = f"session-{self.next_session}"
            self.sessions[session_id] = {"ttl": body["TTL"], "renewals": 0}
            self.session_created_at[session_id] = time.monotonic()
            if self.create_response_delay:
                await asyncio.sleep(self.create_response_delay)
            return httpx.Response(200, json={"ID": session_id})
        if path.startswith("/v1/session/renew/") and request.method == "PUT":
            session_id = path.rsplit("/", 1)[-1]
            session = self.sessions.get(session_id)
            if session is None:
                return httpx.Response(200, json=[])
            session["renewals"] += 1
            return httpx.Response(200, json=[{"ID": session_id}])
        if path.startswith("/v1/session/destroy/") and request.method == "PUT":
            session_id = path.rsplit("/", 1)[-1]
            if self.fail_destroy:
                self.fail_destroy = False
                return httpx.Response(500)
            self.expire(session_id)
            return httpx.Response(200, json=True)
        if path.startswith("/v1/kv/"):
            key = path.removeprefix("/v1/kv/")
            if request.method == "GET":
                lock = self.locks.get(key)
                return httpx.Response(200, json=[self._row(key, lock)]) if lock else httpx.Response(404)
            if "acquire" in query:
                if self.fail_acquire:
                    raise httpx.ReadTimeout("injected lost response", request=request)
                session_id = query["acquire"]
                if session_id not in self.sessions:
                    return httpx.Response(200, json=False)
                lock = self.locks.setdefault(key, {"Session": "", "LockIndex": 0})
                if not lock["Session"]:
                    lock["LockIndex"] += 1
                    lock["Session"] = session_id
                    lock["Value"] = b""
                    return httpx.Response(200, json=True)
                return httpx.Response(200, json=lock["Session"] == session_id)
            if "release" in query:
                session_id = query["release"]
                lock = self.locks.get(key)
                if lock and lock["Session"] == session_id:
                    lock["Session"] = ""
                    return httpx.Response(200, json=True)
                return httpx.Response(200, json=False)
        return httpx.Response(404)

    def expire(self, session_id: str) -> None:
        self.sessions.pop(session_id, None)
        for lock in self.locks.values():
            if lock["Session"] == session_id:
                lock["Session"] = ""


def make_provider(peer: ConsulPeer, *, namespace: str = "test", epoch: str = "epoch",
                  token: str | None = None) -> ConsulProvider:
    transport = httpx.MockTransport(peer)
    return ConsulProvider("http://consul.test:8500", namespace, epoch, token=token,
                          _transport=transport)


@pytest.mark.asyncio
async def test_ttl_validation_precedes_all_network_calls() -> None:
    peer = ConsulPeer()
    async with make_provider(peer) as provider:
        with pytest.raises(UnsupportedCapability, match="10s"):
            await provider.lock("short").try_acquire(Options(lease=9.9, rpc=.5))
    assert peer.calls == []


@pytest.mark.asyncio
async def test_contention_release_and_retained_key_fencing() -> None:
    peer = ConsulPeer()
    async with make_provider(peer, token="secret-token") as provider:
        handle = provider.lock("shared")
        options = Options(lease=10.1, rpc=1, cleanup=1, require_fencing=True)
        first = await handle.try_acquire(options)
        assert first is not None
        assert first.effective_lease_ms == 11000
        assert first.fence == ("epoch", 1)
        assert await handle.try_acquire(options) is None
        assert await first.release() is ReleaseOutcome.RELEASED
        second = await handle.try_acquire(options)
        assert second is not None
        assert second.fence == ("epoch", 2)
        assert await second.release() is ReleaseOutcome.RELEASED
        assert all(request.headers.get("X-Consul-Token") == "secret-token"
                   for request in peer.calls)
        assert all("secret-token" not in str(request.url) for request in peer.calls)


@pytest.mark.asyncio
async def test_unconfirmed_session_destroy_makes_release_unknown() -> None:
    peer = ConsulPeer()
    async with make_provider(peer) as provider:
        lease = await provider.lock("destroy-failure").try_acquire(
            Options(lease=10, rpc=1, cleanup=1),
        )
        assert lease is not None
        peer.fail_destroy = True
        assert await lease.release() is ReleaseOutcome.UNKNOWN
        assert lease._session_id in peer.sessions


@pytest.mark.asyncio
async def test_expired_owner_cannot_release_successor() -> None:
    peer = ConsulPeer()
    async with make_provider(peer) as provider:
        handle = provider.lock("stale")
        old = await handle.try_acquire(Options(lease=10, rpc=1, cleanup=1))
        assert old is not None
        peer.expire(old._session_id)
        new = await handle.try_acquire(Options(lease=10, rpc=1, cleanup=1))
        assert new is not None
        old_outcome = await old.release()
        assert old_outcome is ReleaseOutcome.ALREADY_NOT_OWNED, (
            f"outcome={old_outcome}, lock={peer.locks[new._key]}"
        )
        assert peer.locks[new._key]["Session"] == new._session_id
        assert await new.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_renew_keeps_fence_and_confirms_same_session() -> None:
    peer = ConsulPeer()
    async with make_provider(peer) as provider:
        lease = await provider.lock("renew").try_acquire(
            Options(lease=10, rpc=1, cleanup=1),
        )
        assert lease is not None
        assert await lease.renew()
        assert peer.sessions[lease._session_id]["renewals"] == 1
        assert lease.fence == ("epoch", 1)
        assert await lease.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_acquisition_deadline_starts_at_session_creation() -> None:
    peer = ConsulPeer()
    peer.create_response_delay = .25
    async with make_provider(peer) as provider:
        lease = await provider.lock("delayed-create").try_acquire(
            Options(lease=10, rpc=1, cleanup=1),
        )
        assert lease is not None
        created = peer.session_created_at[lease._session_id]
        assert lease._deadline <= created + 10 - .1
        assert await lease.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_wait_timeout_cleans_its_distinct_session() -> None:
    peer = ConsulPeer()
    async with make_provider(peer) as provider:
        handle = provider.lock("wait")
        options = Options(lease=10, rpc=.1, cleanup=1)
        owner = await handle.try_acquire(options)
        assert owner is not None
        with pytest.raises(WaitTimeout):
            await handle.acquire(AcquireOptions(wait=.08, lease=options))
        assert list(peer.sessions) == [owner._session_id]
        assert await owner.release() is ReleaseOutcome.RELEASED


@pytest.mark.asyncio
async def test_unknown_acquire_ack_is_never_busy() -> None:
    peer = ConsulPeer()
    peer.fail_acquire = True
    async with make_provider(peer) as provider:
        with pytest.raises(AcquisitionUnknown):
            await provider.lock("unknown").try_acquire(Options(lease=10, rpc=.5, cleanup=.5))
        assert peer.locks == {}
        assert peer.sessions == {}


@pytest.mark.asyncio
async def test_metadata_mismatch_fails_before_session_creation() -> None:
    peer = ConsulPeer(epoch="different")
    async with make_provider(peer, epoch="expected") as provider:
        with pytest.raises(ProtocolMismatch):
            await provider.lock("bad-metadata").try_acquire(Options(lease=10, rpc=.5))
    assert not any(request.url.path == "/v1/session/create" for request in peer.calls)


@pytest.mark.asyncio
async def test_extra_metadata_field_fails_before_session_creation() -> None:
    peer = ConsulPeer()
    peer.meta["future_field"] = "unsupported"
    async with make_provider(peer) as provider:
        with pytest.raises(ProtocolMismatch):
            await provider.lock("extra-metadata").try_acquire(Options(lease=10, rpc=.5))
    assert not any(request.url.path == "/v1/session/create" for request in peer.calls)


@pytest.mark.asyncio
async def test_backend_http_rejection_is_not_contention() -> None:
    async def reject(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/meta"):
            return httpx.Response(403)
        return httpx.Response(404)

    provider = ConsulProvider("http://consul.test:8500", "test", "epoch",
                              _transport=httpx.MockTransport(reject))
    try:
        with pytest.raises(BackendUnavailable, match="403"):
            await provider.lock("forbidden").try_acquire(Options(lease=10, rpc=.5))
    finally:
        await provider.aclose()


def test_consul_endpoint_rejects_embedded_credentials_and_paths() -> None:
    for endpoint in ("http://user:pass@consul.test:8500", "http://consul.test:8500/prefix",
                     "http://consul.test:8500?token=secret", "ftp://consul.test:8500"):
        with pytest.raises(ValueError):
            ConsulProvider(endpoint, "test", "epoch")


def test_consul_metadata_shape_rejects_unknown_fields() -> None:
    exact = ConsulPeer().meta
    assert _is_exact_metadata(exact, "epoch")
    assert not _is_exact_metadata({**exact, "future_field": "unsupported"}, "epoch")
