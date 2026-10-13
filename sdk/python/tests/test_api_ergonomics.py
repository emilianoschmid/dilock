from __future__ import annotations

import asyncio
import time

import pytest

from dilock import (
    AcquireOptions, CleanupObservation, Options, ReleaseOutcome, ScopeReleaseError,
)
from dilock.etcd import EtcdLease, EtcdProvider
from dilock.redis import RedisLease, RedisProvider


class OutcomeLease:
    def __init__(self, outcome: ReleaseOutcome):
        self.outcome = outcome

    async def release(self) -> ReleaseOutcome:
        return self.outcome


@pytest.mark.parametrize(
    "provider_factory",
    [
        lambda observer: RedisProvider(
            "redis://127.0.0.1:1", epoch="test", cleanup_observer=observer,
        ),
        lambda observer: EtcdProvider(
            "http://127.0.0.1:1", epoch="test", cleanup_observer=observer,
        ),
    ],
)
async def test_hold_preserves_body_error_and_observes_cleanup(provider_factory, monkeypatch):
    observations: list[CleanupObservation] = []
    provider = provider_factory(observations.append)
    handle = provider.lock("scope/error")
    lease = OutcomeLease(ReleaseOutcome.UNKNOWN)

    async def fake_acquire(*_args, **_kwargs):
        return lease

    monkeypatch.setattr(handle, "acquire", fake_acquire)
    try:
        with pytest.raises(ValueError, match="primary work failure"):
            async with handle.hold(AcquireOptions(lease=Options())):
                raise ValueError("primary work failure")
        assert len(observations) == 1
        assert observations[0].outcome is ReleaseOutcome.UNKNOWN
        assert observations[0].backend in {"redis", "etcd"}
    finally:
        await provider.aclose()


@pytest.mark.parametrize("backend", ["redis", "etcd"])
@pytest.mark.parametrize("outcome", [ReleaseOutcome.ALREADY_NOT_OWNED, ReleaseOutcome.UNKNOWN])
async def test_hold_fails_closed_after_successful_body(backend, outcome, monkeypatch):
    observations: list[CleanupObservation] = []
    provider = (
        RedisProvider("redis://127.0.0.1:1", epoch="test", cleanup_observer=observations.append)
        if backend == "redis" else
        EtcdProvider("http://127.0.0.1:1", epoch="test", cleanup_observer=observations.append)
    )
    handle = provider.lock("scope/release-outcome")
    lease = OutcomeLease(outcome)

    async def fake_acquire(*_args, **_kwargs):
        return lease

    monkeypatch.setattr(handle, "acquire", fake_acquire)
    try:
        with pytest.raises(ScopeReleaseError) as caught:
            async with handle.hold(AcquireOptions(lease=Options())):
                pass
        assert caught.value.outcome is outcome
        assert len(observations) == 1
        assert observations[0].outcome is outcome
        assert observations[0].reason == "scope_exit"
        assert observations[0].backend == backend
    finally:
        await provider.aclose()


@pytest.mark.parametrize("backend", ["redis", "etcd"])
@pytest.mark.parametrize("body_fails", [False, True])
@pytest.mark.parametrize("outcome", [ReleaseOutcome.ALREADY_NOT_OWNED, ReleaseOutcome.UNKNOWN])
async def test_direct_lease_scope_surfaces_nonreleased_outcome_without_masking_body_error(
    backend, body_fails, outcome, monkeypatch,
):
    observations: list[CleanupObservation] = []
    if backend == "redis":
        provider = RedisProvider(
            "redis://127.0.0.1:1", epoch="test", cleanup_observer=observations.append,
        )
        lease = RedisLease(provider, "key", b"owner", Options(), time.monotonic() + 3600)
    else:
        provider = EtcdProvider(
            "http://127.0.0.1:1", epoch="test", cleanup_observer=observations.append,
        )
        lease = EtcdLease(provider, b"name", 1, 30, Options(), time.monotonic())

    release_started = asyncio.Event()
    finish_release = asyncio.Event()

    async def unknown_release():
        release_started.set()
        await finish_release.wait()
        return outcome

    monkeypatch.setattr(lease, "release", unknown_release)
    try:
        async def run_scope():
            async with lease:
                if body_fails:
                    raise ValueError("primary work failure")

        scope = asyncio.create_task(run_scope())
        await release_started.wait()
        assert len(provider._cleanup) == 1
        finish_release.set()
        if body_fails:
            with pytest.raises(ValueError, match="primary work failure"):
                await scope
        else:
            with pytest.raises(ScopeReleaseError) as caught:
                await scope
            assert caught.value.outcome is outcome
        assert len(observations) == 1
        assert observations[0].outcome is outcome
        assert observations[0].reason == ("body_failed" if body_fails else "scope_exit")
        assert observations[0].backend == backend
    finally:
        finish_release.set()
        if backend == "redis":
            lease._expiry.cancel()
        await provider.aclose()
