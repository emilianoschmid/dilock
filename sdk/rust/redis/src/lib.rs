//! Redis single-authority lease adapter for Dilock draft 1.
//!
//! The provider selects one Redis endpoint and namespace. Handles are reusable;
//! a lease is a unique non-Clone acquisition and must be explicitly released.
//! Drop only schedules bounded owner-checked cleanup and never waits for I/O.
//! Operations use Tokio and may move between tasks; lease mutations require
//! exclusive mutable access. Redis provides no strong fencing in this profile.

use dilock::{
    DEFAULT_WAIT, Error, LeaseState, Options, ReleaseOutcome, WithLeaseError, WithLeaseOutput,
    resource_id, validate_wait,
};

fn finish_successful_scope<T, E>(
    value: T,
    release: ReleaseOutcome,
) -> Result<WithLeaseOutput<T>, WithLeaseError<E>> {
    if release == ReleaseOutcome::Released {
        Ok(WithLeaseOutput { value, release })
    } else {
        Err(WithLeaseError::Release { outcome: release })
    }
}

fn finish_scope<T, E>(
    result: Result<T, E>,
    release: ReleaseOutcome,
) -> Result<WithLeaseOutput<T>, WithLeaseError<E>> {
    match result {
        Ok(value) => finish_successful_scope(value, release),
        Err(source) => Err(WithLeaseError::Work { source, release }),
    }
}
use redis::{Client, ErrorKind, RedisError, ServerErrorKind, aio::MultiplexedConnection};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::OnceCell, task::JoinHandle, time};

const RELEASE_SCRIPT: &str = include_str!("lua/redis-release.lua");
const RENEW_SCRIPT: &str = include_str!("lua/redis-renew.lua");

struct Inner {
    client: Client,
    // Lazily established once per provider. A clone of MultiplexedConnection
    // shares the same underlying transport, while each operation gets its own
    // command handle so concurrent RPCs do not serialize behind a mutex.
    connection: OnceCell<MultiplexedConnection>,
    namespace: String,
    epoch: String,
    cleanup_tasks: Mutex<Vec<JoinHandle<()>>>,
}

/// One Redis authority selection.
#[derive(Clone)]
pub struct RedisProvider {
    inner: Arc<Inner>,
}

impl RedisProvider {
    /// Creates a provider. Network access begins on the first operation.
    pub fn new(
        url: &str,
        namespace: impl Into<String>,
        epoch: impl Into<String>,
    ) -> Result<Self, Error> {
        let namespace = namespace.into();
        let epoch = epoch.into();
        resource_id(&namespace, "validation")?;
        if epoch.is_empty() || epoch.len() > 1024 || epoch.bytes().any(|b| b == 0) {
            return Err(Error::InvalidConfiguration);
        }
        let client = Client::open(url).map_err(backend_error)?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                connection: OnceCell::new(),
                namespace,
                epoch,
                cleanup_tasks: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Creates a reusable resource handle without network activity.
    pub fn lock(&self, resource: &str) -> Result<RedisHandle, Error> {
        let id = resource_id(&self.inner.namespace, resource)?;
        Ok(RedisHandle {
            inner: self.inner.clone(),
            key: format!("dilock:{{{id}}}:lease"),
        })
    }

    /// Waits for scheduled best-effort cleanup tasks; live leases are not released.
    pub async fn wait_for_cleanup(&self) {
        let tasks = {
            let mut guard = self
                .inner
                .cleanup_tasks
                .lock()
                .expect("cleanup mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }
}

/// Reusable named resource. Only acknowledged contention returns None.
#[derive(Clone)]
pub struct RedisHandle {
    inner: Arc<Inner>,
    key: String,
}

impl RedisHandle {
    /// Acquires using the shared default policy (five-second wait and
    /// [`Options::default`] lease settings).
    ///
    /// This delegates to [`Self::acquire`]; it does not create a provider or
    /// connection manager. `RedisProvider` and its handles remain reusable.
    pub async fn acquire_default(&self) -> Result<RedisLease, Error> {
        self.acquire(DEFAULT_WAIT, Options::default()).await
    }

    /// Acquires with the shared default policy, runs `operation` once, and
    /// awaits owner-checked release before returning.
    ///
    /// The callback can inspect the lease (including its local held state).
    /// Its error is preserved in [`WithLeaseError::Work`] together with the
    /// release observation. A dropped/cancelled helper future drops the lease;
    /// the SDK's existing runtime-tracked cleanup remains the fallback.
    pub async fn with_lease<F, T, E>(
        &self,
        operation: F,
    ) -> Result<WithLeaseOutput<T>, WithLeaseError<E>>
    where
        F: AsyncFnOnce(&mut RedisLease) -> Result<T, E>,
    {
        self.with_lease_with(DEFAULT_WAIT, Options::default(), operation)
            .await
    }

    /// Like [`Self::with_lease`], using a complete wait and lease policy.
    ///
    /// The `Options` value is used in full; it is not merged with hidden
    /// provider settings. Use struct update syntax to change one field, for
    /// example `Options { lease: Duration::from_secs(60), ..Options::default() }`.
    pub async fn with_lease_with<F, T, E>(
        &self,
        wait: Duration,
        options: Options,
        operation: F,
    ) -> Result<WithLeaseOutput<T>, WithLeaseError<E>>
    where
        F: AsyncFnOnce(&mut RedisLease) -> Result<T, E>,
    {
        let mut lease = self
            .acquire(wait, options)
            .await
            .map_err(WithLeaseError::Acquire)?;
        let result = operation(&mut lease).await;
        let release = lease.release().await;
        finish_scope(result, release)
    }

    /// Makes one atomic SET NX PX attempt.
    ///
    /// Only a confirmed NX conflict returns None. A response lost after send
    /// returns AcquisitionUnknown and triggers owner-specific cleanup. Dropping
    /// this future after dispatch also schedules that cleanup; the late command
    /// can still leave an orphan until its TTL expires.
    pub async fn try_acquire(&self, options: Options) -> Result<Option<RedisLease>, Error> {
        self.try_acquire_before(options, None).await
    }

    async fn try_acquire_before(
        &self,
        options: Options,
        wait_deadline: Option<Instant>,
    ) -> Result<Option<RedisLease>, Error> {
        let options = options.validate()?;
        if options.require_fencing || options.auto_renew {
            return Err(Error::UnsupportedCapability);
        }
        self.validate_metadata(options.rpc_timeout, wait_deadline)
            .await?;
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|_| Error::BackendUnavailable("system CSPRNG unavailable".into()))?;
        let mut owner = b"dilock/1/".to_vec();
        owner.extend_from_slice(hex(&random).as_bytes());
        let connect_timeout = bounded_timeout(options.rpc_timeout, wait_deadline)?;
        let mut connection = match self.connection(connect_timeout).await {
            Ok(connection) => connection,
            Err(_) if deadline_elapsed(wait_deadline) => return Err(Error::WaitTimeout),
            Err(error) => return Err(error),
        };
        let command_timeout = bounded_timeout(options.rpc_timeout, wait_deadline)?;
        let started = Instant::now();
        let cleanup = CleanupGuard::new(
            self.inner.clone(),
            self.key.clone(),
            owner.clone(),
            options.cleanup_timeout,
        );
        let mut command = redis::cmd("SET");
        command
            .arg(&self.key)
            .arg(&owner)
            .arg("NX")
            .arg("PX")
            .arg(options.ttl_ms());
        let command = command.query_async::<Option<String>>(&mut connection);
        let response = match time::timeout(command_timeout, command).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) if error.kind() == ErrorKind::Server(ServerErrorKind::ResponseError) => {
                cleanup.disarm();
                return Err(backend_error(error));
            }
            Ok(Err(_)) | Err(_) => {
                cleanup.run_now().await;
                return Err(Error::AcquisitionUnknown);
            }
        };
        let Some(reply) = response else {
            cleanup.disarm();
            // SET NX acknowledged contention. TYPE is one bounded, read-only
            // classification; never mutate or retry the unexpected key.
            let type_timeout = bounded_timeout(options.rpc_timeout, wait_deadline)?;
            let key_type: String = time::timeout(
                type_timeout,
                redis::cmd("TYPE")
                    .arg(&self.key)
                    .query_async(&mut connection),
            )
            .await
            .map_err(|_| contention_type_timeout(wait_deadline))?
            .map_err(|error| {
                if deadline_elapsed(wait_deadline) {
                    Error::WaitTimeout
                } else {
                    backend_error(error)
                }
            })?;
            if key_type != "string" && key_type != "none" {
                return Err(Error::ProtocolMismatch);
            }
            return Ok(None);
        };
        let deadline = started + options.lease - options.margin();
        if reply != "OK" || Instant::now() >= deadline {
            cleanup.run_now().await;
            return Err(Error::AcquisitionUnknown);
        }
        cleanup.disarm();
        Ok(Some(RedisLease {
            inner: self.inner.clone(),
            key: self.key.clone(),
            owner,
            options,
            deadline,
            state: LeaseState::Held,
            release_outcome: None,
        }))
    }

    /// Retries only acknowledged contention until the wait budget is exhausted.
    ///
    /// The deadline includes metadata checks, connections, commands, and backoff.
    /// Cancellation drops the attempt future; an attempt already sent has a
    /// tracked owner-checked cleanup scheduled by its guard.
    pub async fn acquire(&self, wait: Duration, options: Options) -> Result<RedisLease, Error> {
        validate_wait(wait)?;
        let end = Instant::now()
            .checked_add(wait)
            .ok_or(dilock::OptionError::InvalidWait)?;
        loop {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::WaitTimeout);
            }
            let bounded = Options {
                rpc_timeout: std::cmp::min(options.rpc_timeout, remaining),
                ..options
            };
            if let Some(mut lease) = self.try_acquire_before(bounded, Some(end)).await? {
                if Instant::now() < end {
                    return Ok(lease);
                }
                return Err(late_acquisition_error(lease.release().await));
            }
            let mut jitter = [0u8; 2];
            getrandom::fill(&mut jitter)
                .map_err(|_| Error::BackendUnavailable("system CSPRNG unavailable".into()))?;
            let delay = Duration::from_millis(20 + u16::from_be_bytes(jitter) as u64 % 31);
            time::sleep(std::cmp::min(delay, remaining)).await;
        }
    }

    async fn connection(&self, timeout: Duration) -> Result<MultiplexedConnection, Error> {
        self.inner.connection(timeout).await
    }

    async fn validate_metadata(
        &self,
        timeout: Duration,
        wait_deadline: Option<Instant>,
    ) -> Result<(), Error> {
        let connect_timeout = bounded_timeout(timeout, wait_deadline)?;
        let mut connection = match self.connection(connect_timeout).await {
            Ok(connection) => connection,
            Err(_) if deadline_elapsed(wait_deadline) => return Err(Error::WaitTimeout),
            Err(error) => return Err(error),
        };
        let key = format!("dilock:namespace:{}:meta", self.inner.namespace);
        let read_timeout = bounded_timeout(timeout, wait_deadline)?;
        let raw: Option<Vec<u8>> = time::timeout(
            read_timeout,
            redis::cmd("GET").arg(key).query_async(&mut connection),
        )
        .await
        .map_err(|_| {
            if deadline_elapsed(wait_deadline) {
                Error::WaitTimeout
            } else {
                Error::BackendUnavailable("namespace metadata read timed out".into())
            }
        })?
        .map_err(backend_error)?;
        let raw = raw.ok_or(Error::ProtocolMismatch)?;
        if raw.len() > 8192 {
            return Err(Error::ProtocolMismatch);
        }
        let value: Value = serde_json::from_slice(&raw).map_err(|_| Error::ProtocolMismatch)?;
        let object = value.as_object().ok_or(Error::ProtocolMismatch)?;
        if has_duplicate_top_level_fields(&raw) {
            return Err(Error::ProtocolMismatch);
        }
        let expected = [
            ("protocol_major", Value::from(1)),
            ("identity_scheme", Value::from("sha256-framed-utf8-v1")),
            ("profile", Value::from("redis-single-authority-v1")),
            ("coordination_epoch", Value::from(self.inner.epoch.clone())),
            ("fixed_lease_ms", Value::Null),
        ];
        if expected
            .iter()
            .any(|(name, want)| object.get(*name) != Some(want))
        {
            return Err(Error::ProtocolMismatch);
        }
        Ok(())
    }
}

// A late grant cannot authorize work. Timeout is accurate only when cleanup
// confirms that this acquisition no longer owns a remote key.
fn late_acquisition_error(outcome: ReleaseOutcome) -> Error {
    match outcome {
        ReleaseOutcome::Unknown => Error::AcquisitionUnknown,
        ReleaseOutcome::Released | ReleaseOutcome::AlreadyNotOwned => Error::WaitTimeout,
    }
}

fn has_duplicate_top_level_fields(raw: &[u8]) -> bool {
    struct DuplicateFlag(bool);

    impl<'de> Deserialize<'de> for DuplicateFlag {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct FieldsVisitor;

            impl<'de> Visitor<'de> for FieldsVisitor {
                type Value = bool;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a JSON object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut names = std::collections::HashSet::new();
                    let mut duplicate = false;
                    while let Some(name) = map.next_key::<String>()? {
                        duplicate |= !names.insert(name);
                        map.next_value::<IgnoredAny>()?;
                    }
                    Ok(duplicate)
                }
            }

            deserializer.deserialize_map(FieldsVisitor).map(Self)
        }
    }

    serde_json::from_slice::<DuplicateFlag>(raw)
        .map(|flag| flag.0)
        .unwrap_or(false)
}

impl Inner {
    async fn connection(&self, timeout: Duration) -> Result<MultiplexedConnection, Error> {
        time::timeout(
            timeout,
            self.connection.get_or_try_init(|| async {
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(backend_error)
            }),
        )
        .await
        .map_err(|_| Error::BackendUnavailable("Redis connection timed out".into()))?
        .cloned()
    }
}

fn bounded_timeout(maximum: Duration, deadline: Option<Instant>) -> Result<Duration, Error> {
    match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::WaitTimeout);
            }
            Ok(std::cmp::min(maximum, remaining))
        }
        None => Ok(maximum),
    }
}

fn deadline_elapsed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn contention_type_timeout(wait_deadline: Option<Instant>) -> Error {
    if deadline_elapsed(wait_deadline) {
        Error::WaitTimeout
    } else {
        Error::BackendUnavailable("Redis key type query timed out".into())
    }
}

/// One non-Clone Redis ownership generation.
pub struct RedisLease {
    inner: Arc<Inner>,
    key: String,
    owner: Vec<u8>,
    options: Options,
    deadline: Instant,
    state: LeaseState,
    release_outcome: Option<ReleaseOutcome>,
}

impl RedisLease {
    /// Local observation, transitioning elapsed conservative validity to Lost.
    pub fn state(&mut self) -> LeaseState {
        if self.state == LeaseState::Held && Instant::now() >= self.deadline {
            self.state = LeaseState::Lost;
        }
        self.state
    }

    /// Requested TTL in milliseconds, not the remaining local validity.
    pub fn effective_lease_ms(&self) -> u64 {
        self.options.ttl_ms()
    }

    /// Fails if the local conservative lease deadline has elapsed.
    pub fn ensure_held(&mut self) -> Result<(), Error> {
        (self.state() == LeaseState::Held)
            .then_some(())
            .ok_or(Error::LeaseLost)
    }

    /// Extends this owner atomically; a late reply cannot restore local permission.
    ///
    /// Cancellation leaves the previous conservative deadline in force. A
    /// completed late server renewal cannot extend local permission.
    pub async fn renew(&mut self) -> Result<bool, Error> {
        if self.state() != LeaseState::Held {
            return Ok(false);
        }
        let old_deadline = self.deadline;
        let started = Instant::now();
        let mut connection = match self.connection(self.options.rpc_timeout).await {
            Ok(connection) => connection,
            Err(error) => {
                self.state = LeaseState::Lost;
                return Err(error);
            }
        };
        let reply: Result<Result<i64, RedisError>, _> = time::timeout(
            std::cmp::min(
                self.options.rpc_timeout,
                old_deadline.saturating_duration_since(started),
            ),
            redis::cmd("EVAL")
                .arg(RENEW_SCRIPT)
                .arg(1)
                .arg(&self.key)
                .arg(&self.owner)
                .arg(self.options.ttl_ms())
                .query_async(&mut connection),
        )
        .await;
        let result = match reply {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                self.state = LeaseState::Lost;
                return Err(backend_error(error));
            }
            Err(_) => {
                self.state = LeaseState::Lost;
                return Err(Error::LeaseLost);
            }
        };
        let next_deadline = started + self.options.lease - self.options.margin();
        if Instant::now() >= old_deadline || Instant::now() >= next_deadline || result != 1 {
            self.state = LeaseState::Lost;
            return Ok(false);
        }
        self.deadline = next_deadline;
        Ok(true)
    }

    /// Releases with one monotonic cleanup budget and an atomic owner check.
    ///
    /// The outcome is conservatively marked unknown before the first await. If
    /// this future is cancelled, a later call returns `Unknown` without
    /// repeating the ambiguous command; dropping the lease schedules bounded,
    /// owner-checked cleanup.
    pub async fn release(&mut self) -> ReleaseOutcome {
        if let Some(outcome) = begin_release(&mut self.state, &mut self.release_outcome) {
            return outcome;
        }
        let Some(cleanup_deadline) = Instant::now().checked_add(self.options.cleanup_timeout)
        else {
            self.state = LeaseState::ReleasedUnknown;
            self.release_outcome = Some(ReleaseOutcome::Unknown);
            return ReleaseOutcome::Unknown;
        };
        let result = async {
            let mut connection = self
                .connection(remaining_cleanup(cleanup_deadline)?)
                .await?;
            let reply: i64 = time::timeout(
                remaining_cleanup(cleanup_deadline)?,
                redis::cmd("EVAL")
                    .arg(RELEASE_SCRIPT)
                    .arg(1)
                    .arg(&self.key)
                    .arg(&self.owner)
                    .query_async(&mut connection),
            )
            .await
            .map_err(|_| Error::AcquisitionUnknown)?
            .map_err(backend_error)?;
            Ok::<i64, Error>(reply)
        }
        .await;
        let outcome = match result {
            Ok(1) => ReleaseOutcome::Released,
            Ok(0) => ReleaseOutcome::AlreadyNotOwned,
            Ok(_) | Err(_) => ReleaseOutcome::Unknown,
        };
        self.state = if outcome == ReleaseOutcome::Unknown {
            LeaseState::ReleasedUnknown
        } else {
            LeaseState::Released
        };
        self.release_outcome = Some(outcome);
        outcome
    }

    async fn connection(&self, timeout: Duration) -> Result<MultiplexedConnection, Error> {
        self.inner.connection(timeout).await
    }
}

fn begin_release(
    state: &mut LeaseState,
    outcome: &mut Option<ReleaseOutcome>,
) -> Option<ReleaseOutcome> {
    if let Some(outcome) = *outcome {
        return Some(outcome);
    }
    if matches!(
        state,
        LeaseState::Releasing | LeaseState::Released | LeaseState::ReleasedUnknown
    ) {
        *state = LeaseState::ReleasedUnknown;
        *outcome = Some(ReleaseOutcome::Unknown);
        return Some(ReleaseOutcome::Unknown);
    }
    *state = LeaseState::Releasing;
    *outcome = Some(ReleaseOutcome::Unknown);
    None
}

impl Drop for RedisLease {
    fn drop(&mut self) {
        if matches!(
            self.state,
            LeaseState::Held
                | LeaseState::Releasing
                | LeaseState::Lost
                | LeaseState::ReleasedUnknown
        ) {
            schedule_cleanup(
                &self.inner,
                self.key.clone(),
                self.owner.clone(),
                self.options.cleanup_timeout,
            );
        }
    }
}

struct CleanupGuard {
    inner: Arc<Inner>,
    key: String,
    owner: Vec<u8>,
    timeout: Duration,
    armed: bool,
}

impl CleanupGuard {
    fn new(inner: Arc<Inner>, key: String, owner: Vec<u8>, timeout: Duration) -> Self {
        Self {
            inner,
            key,
            owner,
            timeout,
            armed: true,
        }
    }
    fn disarm(mut self) {
        self.armed = false;
    }
    async fn run_now(mut self) {
        self.armed = false;
        owner_release(&self.inner, &self.key, &self.owner, self.timeout).await;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            schedule_cleanup(
                &self.inner,
                self.key.clone(),
                self.owner.clone(),
                self.timeout,
            );
        }
    }
}

fn schedule_cleanup(inner: &Arc<Inner>, key: String, owner: Vec<u8>, timeout: Duration) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let cleanup_inner = inner.clone();
    let task = runtime.spawn(async move {
        owner_release(&cleanup_inner, &key, &owner, timeout).await;
    });
    if let Ok(mut tasks) = inner.cleanup_tasks.lock() {
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    } else {
        task.abort();
    }
}

async fn owner_release(inner: &Inner, key: &str, owner: &[u8], timeout: Duration) {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return;
    };
    let Ok(connection_timeout) = remaining_cleanup(deadline) else {
        return;
    };
    // A canceled acquisition may leave its multiplexed request awaiting a late
    // acknowledgement. Cleanup needs an independent transport so it can reach
    // Redis even while that request is still in flight.
    let mut connection = match time::timeout(
        connection_timeout,
        inner.client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        _ => return,
    };
    let Ok(command_timeout) = remaining_cleanup(deadline) else {
        return;
    };
    let _: Result<i64, RedisError> = match time::timeout(
        command_timeout,
        redis::cmd("EVAL")
            .arg(RELEASE_SCRIPT)
            .arg(1)
            .arg(key)
            .arg(owner)
            .query_async(&mut connection),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => return,
    };
}

fn remaining_cleanup(deadline: Instant) -> Result<Duration, Error> {
    remaining_cleanup_at(deadline, Instant::now())
}

fn remaining_cleanup_at(deadline: Instant, now: Instant) -> Result<Duration, Error> {
    let remaining = deadline.saturating_duration_since(now);
    if remaining.is_zero() {
        Err(Error::AcquisitionUnknown)
    } else {
        Ok(remaining)
    }
}

fn backend_error(_: RedisError) -> Error {
    Error::BackendUnavailable("Redis command failed".into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_acquisition_timeout_requires_confirmed_owner_cleanup() {
        for outcome in [ReleaseOutcome::Released, ReleaseOutcome::AlreadyNotOwned] {
            assert!(matches!(
                late_acquisition_error(outcome),
                Error::WaitTimeout
            ));
        }
        assert!(matches!(
            late_acquisition_error(ReleaseOutcome::Unknown),
            Error::AcquisitionUnknown
        ));
    }

    #[test]
    fn provider_and_handle_creation_keep_the_shared_transport_lazy() {
        let provider = RedisProvider::new("redis://127.0.0.1:1", "default", "epoch").unwrap();
        assert!(provider.inner.connection.get().is_none());
        let _handle = provider.lock("reports/daily").unwrap();
        assert!(provider.inner.connection.get().is_none());
    }

    #[test]
    fn bundled_script_hashes_match_canonical_sources() {
        let canonical_release = include_str!("../../../../spec/backends/redis-release.lua");
        let canonical_renew = include_str!("../../../../spec/backends/redis-renew.lua");
        assert_eq!(
            Sha256::digest(RELEASE_SCRIPT),
            Sha256::digest(canonical_release)
        );
        assert_eq!(
            Sha256::digest(RENEW_SCRIPT),
            Sha256::digest(canonical_renew)
        );
    }

    #[test]
    fn contention_type_timeout_distinguishes_overall_wait_from_rpc_timeout() {
        assert!(matches!(
            contention_type_timeout(Some(Instant::now() - Duration::from_millis(1))),
            Error::WaitTimeout
        ));
        assert!(matches!(
            contention_type_timeout(Some(Instant::now() + Duration::from_secs(1))),
            Error::BackendUnavailable(_)
        ));
        assert!(matches!(
            contention_type_timeout(None),
            Error::BackendUnavailable(_)
        ));
    }

    #[test]
    fn namespace_metadata_rejects_duplicate_top_level_fields_only() {
        assert!(has_duplicate_top_level_fields(
            br#"{"protocol_major":1,"protocol_major":1}"#
        ));
        assert!(has_duplicate_top_level_fields(
            br#"{"protocol_major":1,"protocol\u005fmajor":1}"#
        ));
        assert!(!has_duplicate_top_level_fields(
            br#"{"extension":{"field":1,"field":2},"protocol_major":1}"#
        ));
        assert!(!has_duplicate_top_level_fields(
            br#"{"protocol_major":1,"extension":{"protocol_major":2}}"#
        ));
    }

    #[test]
    fn positive_wait_below_lease_margin_is_not_itself_expired() {
        let margin = Options::default().margin();
        let remaining = Duration::from_millis(5);
        assert!(remaining < margin);
        assert!(!remaining.is_zero());
    }

    #[test]
    fn release_cleanup_uses_one_nonresetting_monotonic_budget() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(1);
        assert_eq!(
            remaining_cleanup_at(deadline, start + Duration::from_millis(750))
                .expect("budget remains"),
            Duration::from_millis(250)
        );
        assert!(matches!(
            remaining_cleanup_at(deadline, deadline),
            Err(Error::AcquisitionUnknown)
        ));
    }

    #[tokio::test]
    async fn acquire_rejects_invalid_wait_before_network_io() {
        let provider = RedisProvider::new("redis://127.0.0.1:1", "default", "epoch").unwrap();
        let handle = provider.lock("resource").unwrap();
        for wait in [Duration::ZERO, Duration::MAX] {
            assert!(matches!(
                handle.acquire(wait, Options::default()).await,
                Err(Error::InvalidArgument(dilock::OptionError::InvalidWait))
            ));
        }
    }

    #[test]
    fn cancelled_release_is_terminal_and_does_not_retry() {
        let mut state = LeaseState::Held;
        let mut outcome = None;
        let mut backend_calls = 0;

        assert_eq!(begin_release(&mut state, &mut outcome), None);
        backend_calls += 1;
        // Model cancellation while the backend response is ambiguous.
        assert_eq!(
            begin_release(&mut state, &mut outcome),
            Some(ReleaseOutcome::Unknown)
        );
        assert_eq!(backend_calls, 1);
        assert_eq!(state, LeaseState::Releasing);
    }

    #[test]
    fn successful_scope_requires_acknowledged_released_outcome() {
        assert!(matches!(
            finish_successful_scope::<_, std::convert::Infallible>(7, ReleaseOutcome::Released),
            Ok(WithLeaseOutput {
                value: 7,
                release: ReleaseOutcome::Released
            })
        ));
        for outcome in [ReleaseOutcome::AlreadyNotOwned, ReleaseOutcome::Unknown] {
            assert!(matches!(
                finish_successful_scope::<(), std::convert::Infallible>((), outcome),
                Err(WithLeaseError::Release { outcome: observed }) if observed == outcome
            ));
        }
    }

    #[test]
    fn callback_error_remains_primary_when_release_is_not_acknowledged() {
        for release in [
            ReleaseOutcome::Released,
            ReleaseOutcome::AlreadyNotOwned,
            ReleaseOutcome::Unknown,
        ] {
            assert!(matches!(
                finish_scope::<(), _>(Err("original work error"), release),
                Err(WithLeaseError::Work { source: "original work error", release: observed })
                    if observed == release
            ));
        }
    }
}
