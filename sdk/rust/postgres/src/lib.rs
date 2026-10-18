//! Experimental `postgres-advisory-session-v1` backend.
//!
//! Every successful acquisition owns one private `tokio-postgres` session.
//! PostgreSQL advisory locks have no server TTL or generic fencing; `renew`
//! only checks session liveness.

use dilock::{resource_id, Error, Error as CoreError, LeaseState, Options, ReleaseOutcome};
use std::{future::Future, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use tokio_postgres::{Client, NoTls, Row};

const PROFILE: &str = "postgres-advisory-session-v1";
const SCHEME: &str = "sha256-framed-utf8-v1";

#[derive(Debug, Clone)]
pub struct PostgresOptions {
    pub connection_string: String,
    pub namespace: String,
    pub coordination_epoch: String,
    pub rpc_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub poll_interval: Duration,
    pub default_wait: Duration,
    pub default_options: Options,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self { connection_string: String::new(), namespace: "default".into(), coordination_epoch: String::new(), rpc_timeout: Duration::from_secs(2), cleanup_timeout: Duration::from_secs(2), poll_interval: Duration::from_millis(25), default_wait: Duration::from_secs(5), default_options: Options::default() }
    }
}

pub struct PostgresProvider { options: Arc<PostgresOptions>, closed: Arc<AtomicBool> }

impl PostgresProvider {
    pub async fn connect(options: PostgresOptions) -> Result<Self, Error> {
        validate_options(&options)?;
        let provider = Self { options: Arc::new(options), closed: Arc::new(AtomicBool::new(false)) };
        let (client, driver) = connect_session(&provider.options).await?;
        tokio::spawn(async move { let _ = driver.await; });
        validate_metadata(&client, &provider.options).await?;
        Ok(provider)
    }

    pub async fn provision_namespace(connection_string: &str, namespace: &str, epoch: &str) -> Result<(), Error> {
        if connection_string.is_empty() || epoch.is_empty() || epoch.as_bytes().len() > 1024 { return Err(Error::InvalidConfiguration); }
        resource_id(namespace, "validation")?;
        let config: tokio_postgres::Config = connection_string.parse().map_err(|_| Error::InvalidConfiguration)?;
        let provision_timeout = Duration::from_secs(2);
        let (mut client, driver) = timeout(provision_timeout, config.connect(NoTls)).await.map_err(|_| Error::BackendUnavailable("PostgreSQL connection timed out".into()))?.map_err(backend)?;
        tokio::spawn(async move { let _ = driver.await; });
        let transaction = timeout(provision_timeout, client.transaction()).await.map_err(|_| Error::BackendUnavailable("PostgreSQL namespace-provision transaction timed out".into()))?.map_err(backend)?;
        timeout(provision_timeout, transaction.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&"dilock:namespace-meta:v1"]))
            .await.map_err(|_| Error::BackendUnavailable("PostgreSQL namespace-provision serialization timed out".into()))?.map_err(backend)?;
        timeout(provision_timeout, transaction.batch_execute("CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)"))
            .await.map_err(|_| Error::BackendUnavailable("PostgreSQL metadata-table creation timed out".into()))?.map_err(backend)?;
        timeout(provision_timeout, transaction.execute("INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1,1,$2,$3,$4,NULL) ON CONFLICT (namespace) DO NOTHING", &[&namespace, &SCHEME, &PROFILE, &epoch]))
            .await.map_err(|_| Error::BackendUnavailable("PostgreSQL namespace provisioning timed out".into()))?.map_err(backend)?;
        timeout(provision_timeout, transaction.commit()).await.map_err(|_| Error::BackendUnavailable("PostgreSQL namespace-provision commit timed out".into()))?.map_err(backend)?;
        let row = timeout(provision_timeout, client.query_opt("SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace=$1", &[&namespace]))
            .await.map_err(|_| Error::BackendUnavailable("PostgreSQL namespace verification timed out".into()))?.map_err(backend)?.ok_or(Error::ProtocolMismatch)?;
        if !metadata_ok(&row, epoch) { return Err(Error::ProtocolMismatch); }
        Ok(())
    }

    pub fn lock(&self, resource: &str) -> Result<PostgresHandle, Error> {
        if self.closed.load(Ordering::Acquire) { return Err(Error::InvalidConfiguration); }
        let id = resource_id(&self.options.namespace, resource)?;
        Ok(PostgresHandle { provider: self.clone_ref(), key: advisory_key(&id)? })
    }

    pub fn close(&self) { self.closed.store(true, Ordering::Release); }
    fn clone_ref(&self) -> ProviderRef { ProviderRef { options: self.options.clone(), closed: self.closed.clone() } }
}

#[derive(Clone)] struct ProviderRef { options: Arc<PostgresOptions>, closed: Arc<AtomicBool> }
pub struct PostgresHandle { provider: ProviderRef, key: i64 }

impl PostgresHandle {
    pub async fn try_acquire(&self, options: Options) -> Result<Option<PostgresLease>, Error> {
        if options.require_fencing || options.auto_renew { return Err(Error::UnsupportedCapability); }
        let policy = options.validate()?;
        if self.provider.closed.load(Ordering::Acquire) { return Err(Error::InvalidConfiguration); }
        let (client, driver) = connect_session(&self.provider.options).await?;
        tokio::spawn(async move { let _ = driver.await; });
        let result = timeout(self.provider.options.rpc_timeout, client.query_one("SELECT pg_try_advisory_lock($1::bigint)", &[&self.key])).await;
        let row = match result { Ok(Ok(row)) => row, Ok(Err(_e)) => { drop(client); return Err(CoreError::AcquisitionUnknown); }, Err(_) => { drop(client); return Err(CoreError::AcquisitionUnknown); } };
        if !row.get::<_, bool>(0) { drop(client); return Ok(None); }
        Ok(Some(PostgresLease { client: Some(client), key: self.key, options: policy, state: LeaseState::Held }))
    }

    pub async fn acquire(&self, wait: Duration, options: Options) -> Result<PostgresLease, Error> {
        dilock::validate_wait(wait).map_err(Error::InvalidArgument)?;
        let deadline = Instant::now() + wait;
        loop {
            if let Some(lease) = self.try_acquire(options).await? { return Ok(lease); }
            if Instant::now() >= deadline { return Err(Error::WaitTimeout); }
            sleep(self.provider.options.poll_interval.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }

    pub async fn acquire_default(&self) -> Result<PostgresLease, Error> {
        self.acquire(self.provider.options.default_wait, self.provider.options.default_options).await
    }
}

pub struct PostgresLease { client: Option<Client>, key: i64, options: Options, state: LeaseState }

impl PostgresLease {
    pub fn state(&self) -> LeaseState { self.state }
    pub fn valid(&self) -> bool { self.state == LeaseState::Held }
    /// Liveness only; it never extends a server-side TTL.
    pub async fn renew(&mut self) -> Result<bool, Error> {
        if !self.valid() { return Err(Error::LeaseLost); }
        let client = self.client.as_ref().ok_or(Error::LeaseLost)?;
        match timeout(self.options.rpc_timeout, client.query_one("SELECT 1", &[])).await {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(error)) => { self.state = LeaseState::Lost; Err(backend(error)) },
            Err(_) => { self.state = LeaseState::Lost; Err(Error::BackendUnavailable("PostgreSQL liveness timed out".into())) },
        }
    }
    pub async fn release(&mut self) -> Result<ReleaseOutcome, Error> {
        if self.state == LeaseState::Released { return Ok(ReleaseOutcome::AlreadyNotOwned); }
        let client = self.client.take().ok_or(Error::AcquisitionUnknown)?;
        self.state = LeaseState::Releasing;
        let result = timeout(self.options.cleanup_timeout, client.query_one("SELECT pg_advisory_unlock($1::bigint)", &[&self.key])).await;
        drop(client);
        match result {
            Ok(Ok(row)) => { let released = row.get::<_, bool>(0); self.state = if released { LeaseState::Released } else { LeaseState::Lost }; Ok(if released { ReleaseOutcome::Released } else { ReleaseOutcome::AlreadyNotOwned }) }
            Ok(Err(_)) | Err(_) => { self.state = LeaseState::ReleasedUnknown; Ok(ReleaseOutcome::Unknown) }
        }
    }
}

async fn connect_session(options: &PostgresOptions) -> Result<(Client, impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static), Error> {
    let config: tokio_postgres::Config = options.connection_string.parse().map_err(|_| Error::InvalidConfiguration)?;
    timeout(options.rpc_timeout, config.connect(NoTls)).await.map_err(|_| Error::BackendUnavailable("PostgreSQL connection timed out".into()))?.map_err(backend)
}

async fn validate_metadata(client: &Client, options: &PostgresOptions) -> Result<(), Error> {
    let row = timeout(options.rpc_timeout, client.query_opt("SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace=$1", &[&options.namespace])).await.map_err(|_| Error::BackendUnavailable("PostgreSQL metadata timed out".into()))?.map_err(backend)?.ok_or(Error::ProtocolMismatch)?;
    if !metadata_ok(&row, &options.coordination_epoch) { return Err(Error::ProtocolMismatch); }
    Ok(())
}

fn metadata_ok(row: &Row, epoch: &str) -> bool {
    row.get::<_, i32>(0) == 1 && row.get::<_, String>(1) == SCHEME && row.get::<_, String>(2) == PROFILE && row.get::<_, String>(3) == epoch && row.get::<_, Option<i64>>(4).is_none()
}

fn validate_options(options: &PostgresOptions) -> Result<(), Error> {
    if options.connection_string.is_empty() || options.coordination_epoch.is_empty() || options.coordination_epoch.as_bytes().len() > 1024 || options.rpc_timeout.is_zero() || options.cleanup_timeout.is_zero() || options.poll_interval.is_zero() || options.default_wait.is_zero() { return Err(Error::InvalidConfiguration); }
    resource_id(&options.namespace, "validation")?;
    options.default_options.validate().map(|_| ()).map_err(Error::InvalidArgument)
}

fn advisory_key(id: &str) -> Result<i64, Error> { if id.len()!=64 { return Err(Error::InvalidConfiguration); } Ok(i64::from_be_bytes(u64::from_str_radix(&id[..16],16).map_err(|_| Error::InvalidConfiguration)?.to_be_bytes())) }
fn backend(error: tokio_postgres::Error) -> Error { Error::BackendUnavailable(error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_advisory_vectors() {
        assert_eq!(advisory_key(&("0000000000000000".to_string()+&"0".repeat(48))).unwrap(), 0);
        assert_eq!(advisory_key(&("7fffffffffffffff".to_string()+&"0".repeat(48))).unwrap(), i64::MAX);
        assert_eq!(advisory_key(&("8000000000000000".to_string()+&"0".repeat(48))).unwrap(), i64::MIN);
        assert_eq!(advisory_key(&("ffffffffffffffff".to_string()+&"0".repeat(48))).unwrap(), -1);
    }
}
