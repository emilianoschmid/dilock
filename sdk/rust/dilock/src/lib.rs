//! Shared types and draft 1 identity encoding for Dilock.
//!
//! This crate does not contact a coordinator. Backend crates provide the
//! distributed operations and their specific guarantees.
//!
//! ```
//! use dilock::{Options, resource_id};
//! use std::time::Duration;
//!
//! let id = resource_id("default", "reports/daily")?;
//! assert_eq!(id.len(), 64);
//! assert!(Options::default().validate().is_ok());
//! assert_eq!(
//!     Options { lease: Duration::ZERO, ..Options::default() }.validate(),
//!     Err(dilock::OptionError::InvalidLease),
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Default server TTL requested by a new acquisition.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// Default total acquisition wait budget.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(5);
/// Default budget for one coordinator round trip.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// Independent budget reserved for owner-checked cleanup.
pub const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Validates a caller-supplied total acquisition wait budget.
///
/// A zero wait is invalid input, not evidence of contention. Backend adapters
/// must also reject a budget whose monotonic deadline cannot be represented.
pub fn validate_wait(wait: Duration) -> Result<(), OptionError> {
    if wait.is_zero() {
        Err(OptionError::InvalidWait)
    } else {
        Ok(())
    }
}

/// Errors in the canonical draft 1 namespace/resource identity.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentityError {
    /// Namespace does not match the draft 1 ASCII grammar.
    #[error("namespace must match [a-z0-9][a-z0-9_.-]{{0,63}}")]
    InvalidNamespace,
    /// Resource is empty, contains NUL, or exceeds the UTF-8 byte limit.
    #[error("resource must contain 1..=1024 UTF-8 bytes and no NUL")]
    InvalidResource,
}

/// Computes the shared draft 1 resource identifier.
///
/// Namespace matching is ASCII-only. Resource bytes use strict UTF-8 without
/// Unicode normalization, trimming, or case folding.
pub fn resource_id(namespace: &str, resource: &str) -> Result<String, IdentityError> {
    let ns = namespace.as_bytes();
    if ns.is_empty()
        || ns.len() > 64
        || !(ns[0].is_ascii_lowercase() || ns[0].is_ascii_digit())
        || !ns
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(b))
    {
        return Err(IdentityError::InvalidNamespace);
    }
    let value = resource.as_bytes();
    if value.is_empty() || value.len() > 1024 || value.contains(&0) {
        return Err(IdentityError::InvalidResource);
    }
    let mut input = Vec::with_capacity(24 + ns.len() + value.len());
    input.extend_from_slice(b"dilock.lock.identity\0");
    input.extend_from_slice(&(ns.len() as u32).to_be_bytes());
    input.extend_from_slice(ns);
    input.extend_from_slice(&(value.len() as u32).to_be_bytes());
    input.extend_from_slice(value);
    Ok(hex(&Sha256::digest(input)))
}

/// Returns the Redis lease key for a canonical namespace/resource pair.
pub fn redis_key(namespace: &str, resource: &str) -> Result<String, IdentityError> {
    Ok(format!(
        "dilock:{{{}}}:lease",
        resource_id(namespace, resource)?
    ))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

/// Limits and requested capabilities for one acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Requested server TTL. Redis draft 1 supports 100 ms through one day.
    pub lease: Duration,
    /// One command's maximum response budget.
    pub rpc_timeout: Duration,
    /// Separate timeout for owner-checked cleanup.
    pub cleanup_timeout: Duration,
    /// Renew automatically while held when the selected backend supports it.
    pub auto_renew: bool,
    /// Require monotonic resource-side fencing, unsupported by Redis draft 1.
    pub require_fencing: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            lease: DEFAULT_LEASE,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            cleanup_timeout: DEFAULT_CLEANUP_TIMEOUT,
            auto_renew: false,
            require_fencing: false,
        }
    }
}

impl Options {
    /// Validates TTL granularity and conservative local validity constraints.
    pub fn validate(self) -> Result<Self, OptionError> {
        let ttl_ms = self.lease.as_millis();
        if !(100..=86_400_000).contains(&ttl_ms) {
            return Err(OptionError::InvalidLease);
        }
        if self.rpc_timeout.is_zero()
            || self.cleanup_timeout.is_zero()
            || self.rpc_timeout.saturating_add(self.margin()) >= self.lease
            || Instant::now().checked_add(self.rpc_timeout).is_none()
            || Instant::now().checked_add(self.cleanup_timeout).is_none()
        {
            return Err(OptionError::InvalidBudget);
        }
        Ok(self)
    }

    /// TTL represented as integer milliseconds, rounded down.
    pub fn ttl_ms(self) -> u64 {
        self.lease.as_millis() as u64
    }

    /// Scheduling and drift allowance applied to local lease validity.
    pub fn margin(self) -> Duration {
        std::cmp::max(Duration::from_millis(10), self.lease / 100)
    }
}

/// Invalid acquisition timing or unsupported option combinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OptionError {
    /// Lease must be 100 ms through 86,400,000 ms.
    #[error("lease TTL must be between 100 ms and one day")]
    InvalidLease,
    /// Total acquisition wait must be positive and representable as a deadline.
    #[error("acquisition wait budget must be positive and representable")]
    InvalidWait,
    /// RPC/cleanup budgets must be positive and RPC plus margin must fit TTL.
    #[error("RPC and cleanup budgets must be positive and RPC must fit the lease")]
    InvalidBudget,
}

/// Local lease state; an observation cannot stop a paused process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// Ownership was acknowledged and conservative validity remains.
    Held,
    /// Explicit release has started; protected work must stop.
    Releasing,
    /// Release was acknowledged or the owner was already absent.
    Released,
    /// Release was sent but its acknowledgement was not confirmed.
    ReleasedUnknown,
    /// Local permission ended due to expiry or failed renewal.
    Lost,
}

/// Result of an explicit release for one backend ownership generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The backend acknowledged release of this ownership generation.
    Released,
    /// The backend confirmed nothing belonging to this generation remains.
    /// An absent etcd lock key alone cannot confirm that its unique lease ended.
    AlreadyNotOwned,
    /// Release may have executed, but the backend did not confirm its outcome.
    Unknown,
}

/// Value returned by an awaited scoped operation.
///
/// `release` is the acknowledgement observed by explicit asynchronous
/// cleanup. `Unknown` means the operation may have completed remotely without
/// an acknowledgement; lease expiry remains the fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithLeaseOutput<T> {
    /// Value produced by the protected operation.
    pub value: T,
    /// Explicit release observation made before returning from the scope.
    pub release: ReleaseOutcome,
}

/// Failure from an awaited scoped operation.
///
/// The `Work` variant retains the original callback error and also reports the
/// independent release observation. A callback failure is never replaced by
/// cleanup uncertainty.
#[derive(Debug)]
pub enum WithLeaseError<E> {
    /// Acquisition failed, so the callback did not run.
    Acquire(Error),
    /// The callback failed; `release` is its secondary cleanup observation.
    Work {
        /// Original error returned by the protected operation.
        source: E,
        /// Result of the awaited cleanup attempt.
        release: ReleaseOutcome,
    },
    /// The callback succeeded, but owner release was not acknowledged as Released.
    Release {
        /// Exact result of the awaited cleanup attempt.
        outcome: ReleaseOutcome,
    },
}

impl<E> std::fmt::Display for WithLeaseError<E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Acquire(error) => write!(formatter, "lease acquisition failed: {error}"),
            Self::Work { source, release } => {
                write!(
                    formatter,
                    "protected operation failed: {source}; release: {release:?}"
                )
            }
            Self::Release { outcome } => {
                write!(
                    formatter,
                    "protected operation completed but release was {outcome:?}"
                )
            }
        }
    }
}

impl<E> std::error::Error for WithLeaseError<E> where E: std::error::Error + 'static {}

/// Errors shared by backend adapters; only confirmed contention is absence.
#[derive(Debug, Error)]
pub enum Error {
    /// Coordinator connection, timeout, or known server rejection.
    #[error("backend operation failed: {0}")]
    BackendUnavailable(String),
    /// A dispatched acquisition did not receive a trustworthy ownership acknowledgement.
    #[error("acquisition outcome is unknown")]
    AcquisitionUnknown,
    /// Namespace metadata is missing or incompatible.
    #[error("namespace metadata is missing or incompatible")]
    ProtocolMismatch,
    /// A required provider setting is invalid.
    #[error("invalid provider configuration")]
    InvalidConfiguration,
    /// Backend cannot satisfy a requested guarantee.
    #[error("requested capability is unsupported")]
    UnsupportedCapability,
    /// Local validity ended, so the lease may not guard new work.
    #[error("lease is no longer locally held")]
    LeaseLost,
    /// Wait budget expired after ordinary contention.
    #[error("acquisition wait budget elapsed")]
    WaitTimeout,
    /// Local arguments/options violate the wire profile.
    #[error(transparent)]
    InvalidArgument(#[from] OptionError),
    /// Identity input was invalid.
    #[error(transparent)]
    InvalidIdentity(#[from] IdentityError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_shared_identity_vector() {
        assert_eq!(
            resource_id("default", "camera/123/reindex").unwrap(),
            "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6"
        );
    }

    #[test]
    fn keeps_unicode_forms_distinct_and_validates_limits() {
        assert_ne!(
            resource_id("default", "café"),
            resource_id("default", "cafe\u{301}")
        );
        assert!(resource_id("Prod", "r").is_err());
        assert!(resource_id("default", "a\0b").is_err());
        assert!(resource_id("default", &"x".repeat(1025)).is_err());
    }

    #[test]
    fn accepts_valid_f1_to_f3_four_byte_scalars() {
        assert!(resource_id("default", "\u{40000}").is_ok());
        assert!(resource_id("default", "\u{fffff}").is_ok());
    }

    #[test]
    fn defaults_pass_profile_validation() {
        assert!(Options::default().validate().is_ok());
        assert_eq!(DEFAULT_WAIT, Duration::from_secs(5));
        assert_eq!(DEFAULT_LEASE, Duration::from_secs(30));
        assert_eq!(
            Options {
                lease: Duration::from_millis(99),
                ..Options::default()
            }
            .validate(),
            Err(OptionError::InvalidLease)
        );
    }

    #[test]
    fn cleanup_budget_must_have_a_representable_monotonic_deadline() {
        assert_eq!(
            Options {
                cleanup_timeout: Duration::MAX,
                ..Options::default()
            }
            .validate(),
            Err(OptionError::InvalidBudget)
        );
    }

    #[test]
    fn acquisition_wait_must_be_positive() {
        assert_eq!(validate_wait(Duration::ZERO), Err(OptionError::InvalidWait));
        assert_eq!(validate_wait(Duration::from_nanos(1)), Ok(()));
    }
}
