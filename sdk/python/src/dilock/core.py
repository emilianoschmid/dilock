"""Wire identity and public values; all durations are seconds unless noted."""
from __future__ import annotations

import hashlib
import logging
import math
import re
import struct
from dataclasses import dataclass
from enum import Enum
from typing import AsyncContextManager, Callable, Protocol


class AcquisitionUnknown(RuntimeError):
    """A dispatched acquisition may exist remotely; never enter protected work."""


class BackendUnavailable(RuntimeError):
    """The backend could not be reached or rejected a non-acquisition operation."""


class ProtocolMismatch(RuntimeError):
    """Namespace metadata or backend replies violate the selected profile."""


class UnsupportedCapability(RuntimeError):
    """A requested guarantee is unsupported by this backend."""


class WaitTimeout(TimeoutError):
    """Ordinary contention exhausted the acquisition wait budget."""


class LeaseLost(RuntimeError):
    """Local permission expired or ownership was no longer confirmed."""


class ScopeReleaseError(RuntimeError):
    """A protected scope completed, but release was not acknowledged."""

    def __init__(self, outcome: ReleaseOutcome):
        self.outcome = outcome
        super().__init__(f"scope release was not acknowledged: {outcome.value}")


class State(str, Enum):
    """Terminal observations never transition back to held."""
    HELD = "held"
    RELEASING = "releasing"
    RELEASED = "released"
    RELEASED_UNKNOWN = "released_unknown"
    LOST = "lost"


class ReleaseOutcome(str, Enum):
    """Remote acknowledgement, distinct from local state."""
    RELEASED = "released"
    ALREADY_NOT_OWNED = "already_not_owned"
    UNKNOWN = "release_unknown"


@dataclass(frozen=True)
class CleanupObservation:
    """Safe scope-cleanup diagnostic; never contains owner or credential bytes."""
    backend: str
    outcome: ReleaseOutcome
    reason: str


def report_cleanup(
    observer: Callable[[CleanupObservation], None] | None,
    backend: str, outcome: ReleaseOutcome, reason: str,
) -> None:
    """Report bounded scope cleanup without allowing diagnostics to escape."""
    observation = CleanupObservation(backend, outcome, reason)
    try:
        if observer is not None:
            observer(observation)
        else:
            logging.getLogger("dilock.cleanup").warning(
                "%s scope cleanup outcome=%s reason=%s", backend, outcome.value, reason,
            )
    except Exception:
        pass


@dataclass(frozen=True)
class Options:
    """Immutable budgets. Redis TTL: 0.1s..1 day; RPC must fit conservative TTL."""
    lease: float = 30.0
    rpc: float = 2.0
    cleanup: float = 2.0
    auto_renew: bool = False
    require_fencing: bool = False

    def __post_init__(self) -> None:
        for value in (self.lease, self.rpc, self.cleanup):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("budgets must be finite and positive")
        if not 0.1 <= self.lease <= 86400 or self.rpc >= self.lease - self.margin:
            raise ValueError("RPC must fit within a supported conservative TTL")

    @property
    def ttl_ms(self) -> int:
        """Effective integer server TTL, rounded down."""
        return int(self.lease * 1000)

    @property
    def margin(self) -> float:
        """Conservative drift/scheduling allowance, not protection from suspension."""
        return max(0.01, self.lease * 0.01)


@dataclass(frozen=True)
class AcquireOptions:
    """Complete immutable acquisition policy, including total wait budget."""
    wait: float = 5.0
    lease: Options = Options()

    def __post_init__(self) -> None:
        if not math.isfinite(self.wait) or self.wait <= 0:
            raise ValueError("wait must be finite and positive")
        if not isinstance(self.lease, Options):
            raise ValueError("lease must be Options")


def identity(namespace: str, resource: str) -> str:
    """Draft 1 SHA256 identity. Reject invalid scalars; never normalize Unicode."""
    if not isinstance(namespace, str) or not isinstance(resource, str):
        raise ValueError("namespace and resource must be strings")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", namespace, flags=re.ASCII):
        raise ValueError("invalid namespace")
    if "\0" in resource:
        raise ValueError("NUL in resource")
    try:
        data = resource.encode("utf-8", "strict")
    except UnicodeError as exc:
        raise ValueError("resource must contain Unicode scalars") from exc
    if not 1 <= len(data) <= 1024:
        raise ValueError("resource must have 1..1024 UTF-8 bytes")
    ns = namespace.encode("ascii")
    return hashlib.sha256(
        b"dilock.lock.identity\0" + struct.pack(">I", len(ns)) + ns
        + struct.pack(">I", len(data)) + data
    ).hexdigest()


class Lease(Protocol):
    """Loop-bound, non-transferable acquisition; release explicitly or async with."""
    @property
    def state(self) -> State: ...
    async def release(self) -> ReleaseOutcome: ...
    async def renew(self) -> bool: ...


class Handle(Protocol):
    """Reusable resource; independent attempts are non-reentrant."""
    async def try_acquire(self, options: Options | None = None) -> Lease | None: ...
    async def acquire(
        self, wait: float | AcquireOptions | None = None, options: Options | None = None,
    ) -> Lease: ...
    def hold(self, options: AcquireOptions | None = None) -> AsyncContextManager[Lease]: ...
