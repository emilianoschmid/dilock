"""Explicit, bounded distributed leases. Backend adapters are optional imports."""
from .core import (
    AcquireOptions, AcquisitionUnknown, BackendUnavailable, CleanupObservation,
    LeaseLost, Options, ProtocolMismatch, ScopeReleaseError,
    ReleaseOutcome, State, UnsupportedCapability, WaitTimeout, identity,
)

__all__ = [
    "AcquireOptions", "AcquisitionUnknown", "BackendUnavailable", "CleanupObservation",
    "LeaseLost", "Options", "ProtocolMismatch", "ScopeReleaseError",
    "ReleaseOutcome", "State", "UnsupportedCapability", "WaitTimeout", "identity",
]
