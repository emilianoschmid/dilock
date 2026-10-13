use dilock::{LeaseState, Options, ReleaseOutcome};
use dilock_etcd::{EtcdFence, EtcdHandle, EtcdLease, EtcdProvider};
use dilock_redis::{RedisLease, RedisProvider};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Mutex, oneshot},
    task::JoinSet,
};

const MAX_LINE: usize = 64 * 1024;
static NEXT_LEASE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    backend: String,
    endpoint: String,
    namespace: String,
    epoch: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: String,
    op: String,
    resource: Option<String>,
    lease_id: Option<String>,
    target_id: Option<String>,
    wait_ms: Option<u64>,
    lease_ms: Option<u64>,
    rpc_ms: Option<u64>,
    auto_renew: Option<bool>,
}

#[derive(Serialize)]
struct Response<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_lease_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_outcome_unknown: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdk_version: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_version: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capabilities: Option<Vec<&'static str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fence: Option<FenceOutput>,
}

#[derive(Serialize)]
struct FenceOutput {
    epoch: String,
    value: String,
}

#[derive(Clone)]
enum Provider {
    Redis(RedisProvider),
    Etcd(EtcdProvider),
}

#[derive(Clone)]
enum Handle {
    Redis(dilock_redis::RedisHandle),
    Etcd(EtcdHandle),
}

enum Lease {
    Redis(RedisLease),
    Etcd(EtcdLease),
}

type Leases = Arc<Mutex<HashMap<String, Lease>>>;
type Released = Arc<Mutex<HashMap<String, ReleaseOutcome>>>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;

impl Provider {
    fn lock(&self, resource: &str) -> Result<Handle, dilock::Error> {
        match self {
            Self::Redis(provider) => provider.lock(resource).map(Handle::Redis),
            Self::Etcd(provider) => provider
                .lock(resource)
                .map(Handle::Etcd)
                .map_err(dilock::Error::InvalidIdentity),
        }
    }

    async fn wait_for_cleanup(&self) {
        match self {
            Self::Redis(provider) => provider.wait_for_cleanup().await,
            Self::Etcd(provider) => provider.wait_for_cleanup().await,
        }
    }

    async fn server_version(&self) -> Result<Option<String>, dilock::Error> {
        match self {
            Self::Redis(_) => Ok(None),
            Self::Etcd(provider) => provider
                .server_version(Duration::from_secs(2))
                .await
                .map(Some),
        }
    }
}

impl Handle {
    async fn acquire(&self, wait: Duration, options: Options) -> Result<Lease, dilock::Error> {
        match self {
            Self::Redis(handle) => handle.acquire(wait, options).await.map(Lease::Redis),
            Self::Etcd(handle) => handle.acquire(wait, options).await.map(Lease::Etcd),
        }
    }

    async fn try_acquire(&self, options: Options) -> Result<Option<Lease>, dilock::Error> {
        match self {
            Self::Redis(handle) => handle
                .try_acquire(options)
                .await
                .map(|lease| lease.map(Lease::Redis)),
            Self::Etcd(handle) => handle
                .try_acquire(options)
                .await
                .map(|lease| lease.map(Lease::Etcd)),
        }
    }
}

impl Lease {
    fn effective_lease_ms(&self) -> u64 {
        match self {
            Self::Redis(lease) => lease.effective_lease_ms(),
            Self::Etcd(lease) => lease.effective_lease_ms(),
        }
    }

    fn fence(&self) -> Option<FenceOutput> {
        match self {
            Self::Redis(_) => None,
            Self::Etcd(lease) => Some(fence_output(lease.fence())),
        }
    }

    async fn release(&mut self) -> ReleaseOutcome {
        match self {
            Self::Redis(lease) => lease.release().await,
            Self::Etcd(lease) => lease.release().await,
        }
    }

    fn state(&mut self) -> LeaseState {
        match self {
            Self::Redis(lease) => lease.state(),
            Self::Etcd(lease) => lease.state(),
        }
    }
}

fn fence_output(fence: &EtcdFence) -> FenceOutput {
    FenceOutput {
        epoch: fence.epoch.clone(),
        value: fence.revision.to_string(),
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("worker startup failed: {error}");
        std::process::exit(2);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).ok_or("expected configuration path")?;
    let bytes = fs::read(path)?;
    let config: Config = serde_json::from_slice(&bytes)?;
    let provider = match config.backend.as_str() {
        "redis" => Provider::Redis(RedisProvider::new(
            &config.endpoint,
            config.namespace,
            config.epoch,
        )?),
        "etcd" => Provider::Etcd(EtcdProvider::new(
            config.endpoint,
            config.namespace,
            config.epoch,
        )?),
        _ => return Err("unsupported Rust worker backend".into()),
    };
    let leases: Leases = Arc::new(Mutex::new(HashMap::new()));
    let released: Released = Arc::new(Mutex::new(HashMap::new()));
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let output = Arc::new(Mutex::new(io::stdout()));
    let input = BufReader::new(io::stdin());
    let mut input = input;
    let mut tasks = JoinSet::new();
    loop {
        let mut line = Vec::new();
        let mut oversized = false;
        let mut saw_input = false;
        loop {
            let (count, newline, chunk) = {
                let available = input.fill_buf().await?;
                if available.is_empty() {
                    if !saw_input {
                        break;
                    }
                    (0, true, Vec::new())
                } else {
                    saw_input = true;
                    let newline = available.iter().position(|byte| *byte == b'\n');
                    let count = newline.map_or(available.len(), |index| index + 1);
                    let chunk = available[..count].to_vec();
                    (count, newline.is_some(), chunk)
                }
            };
            if count == 0 && !newline {
                break;
            }
            if !oversized && line.len() + chunk.len() <= MAX_LINE {
                line.extend_from_slice(&chunk);
            } else {
                oversized = true;
            }
            input.consume(count);
            if newline {
                break;
            }
        }
        if !saw_input {
            break;
        }
        if oversized {
            write_response(
                &output,
                Response {
                    id: None,
                    status: "invalid_argument",
                    lease_id: None,
                    effective_lease_ms: None,
                    state: None,
                    remote_outcome_unknown: None,
                    sdk_version: None,
                    protocol: None,
                    client_version: None,
                    server_version: None,
                    capabilities: None,
                    fence: None,
                },
            )
            .await?;
            continue;
        }
        let request: Request = match serde_json::from_slice::<Request>(&line) {
            Ok(request) if !request.id.is_empty() && request.id.len() <= 128 => request,
            _ => {
                write_response(
                    &output,
                    Response {
                        id: None,
                        status: "invalid_argument",
                        lease_id: None,
                        effective_lease_ms: None,
                        state: None,
                        remote_outcome_unknown: None,
                        sdk_version: None,
                        protocol: None,
                        client_version: None,
                        server_version: None,
                        capabilities: None,
                        fence: None,
                    },
                )
                .await?;
                continue;
            }
        };
        {
            let mut active = pending.lock().await;
            active.retain(|_, sender| !sender.is_closed());
            if active.contains_key(&request.id) {
                write_response(
                    &output,
                    Response {
                        id: Some(request.id),
                        status: "invalid_argument",
                        lease_id: None,
                        effective_lease_ms: None,
                        state: None,
                        remote_outcome_unknown: None,
                        sdk_version: None,
                        protocol: None,
                        client_version: None,
                        server_version: None,
                        capabilities: None,
                        fence: None,
                    },
                )
                .await?;
                continue;
            }
        }
        if request.op == "shutdown" {
            write_response(
                &output,
                Response {
                    id: Some(request.id.clone()),
                    status: "ok",
                    lease_id: None,
                    effective_lease_ms: None,
                    state: None,
                    remote_outcome_unknown: None,
                    sdk_version: None,
                    protocol: None,
                    client_version: None,
                    server_version: None,
                    capabilities: None,
                    fence: None,
                },
            )
            .await?;
            break;
        }
        if request.op == "cancel_request" {
            let target = request.target_id.as_deref().unwrap_or_default();
            let sender = pending.lock().await.remove(target);
            let status = if sender.is_some_and(|sender| sender.send(()).is_ok()) {
                "ok"
            } else {
                "invalid_argument"
            };
            write_response(
                &output,
                Response {
                    id: Some(request.id),
                    status,
                    lease_id: None,
                    effective_lease_ms: None,
                    state: None,
                    remote_outcome_unknown: None,
                    sdk_version: None,
                    protocol: None,
                    client_version: None,
                    server_version: None,
                    capabilities: None,
                    fence: None,
                },
            )
            .await?;
            continue;
        }
        let cancellation = if matches!(request.op.as_str(), "acquire" | "try_acquire") {
            let (sender, receiver) = oneshot::channel();
            pending.lock().await.insert(request.id.clone(), sender);
            Some(receiver)
        } else {
            None
        };
        let provider = provider.clone();
        let leases = leases.clone();
        let released = released.clone();
        let pending = pending.clone();
        let output = output.clone();
        tasks.spawn(async move {
            let response = handle(request, provider, leases, released, pending, cancellation).await;
            if let Err(error) = write_response(&output, response).await {
                eprintln!("worker output failed: {error}");
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    {
        let mut held = leases.lock().await;
        for lease in held.values_mut() {
            let _ = lease.release().await;
        }
        held.clear();
    }
    provider.wait_for_cleanup().await;
    Ok(())
}

async fn handle(
    request: Request,
    provider: Provider,
    leases: Leases,
    released: Released,
    pending: Pending,
    cancellation: Option<oneshot::Receiver<()>>,
) -> Response<'static> {
    let id = request.id.clone();
    let error_status = |status| Response {
        id: Some(id.clone()),
        status,
        lease_id: None,
        effective_lease_ms: None,
        state: None,
        remote_outcome_unknown: Some(status == "acquisition_unknown"),
        sdk_version: None,
        protocol: None,
        client_version: None,
        server_version: None,
        capabilities: None,
        fence: None,
    };
    match request.op.as_str() {
        "hello" => {
            let server_version = match provider.server_version().await {
                Ok(version) => version,
                Err(_) => return error_status("backend_unavailable"),
            };
            let (client_version, capabilities) = match &provider {
                Provider::Redis(_) => (
                    "redis-rs-1.3.0",
                    vec![
                        "redis",
                        "explicit_release",
                        "explicit_renew",
                        "owner_checked_cleanup",
                    ],
                ),
                Provider::Etcd(_) => (
                    "etcd-client-0.20.0",
                    vec!["etcd", "explicit_release", "managed_keepalive", "fencing"],
                ),
            };
            Response {
                id: Some(id.clone()),
                status: "ok",
                lease_id: None,
                effective_lease_ms: None,
                state: None,
                remote_outcome_unknown: None,
                sdk_version: Some(env!("CARGO_PKG_VERSION")),
                protocol: Some("draft-1"),
                client_version: Some(client_version),
                server_version,
                capabilities: Some(capabilities),
                fence: None,
            }
        }
        "try_acquire" | "acquire" => {
            let Some(resource) = request.resource.as_deref() else {
                return error_status("invalid_argument");
            };
            let options = options(&request);
            let handle = match provider.lock(resource) {
                Ok(handle) => handle,
                Err(_) => return error_status("invalid_argument"),
            };
            let operation = async {
                if request.op == "acquire" {
                    handle
                        .acquire(
                            Duration::from_millis(request.wait_ms.unwrap_or(5_000)),
                            options,
                        )
                        .await
                        .map(Some)
                } else {
                    handle.try_acquire(options).await
                }
            };
            let operation_started = AtomicBool::new(false);
            let result = if let Some(mut cancellation) = cancellation {
                tokio::select! {
                    biased;
                    _ = &mut cancellation => {
                        pending.lock().await.remove(&id);
                        return Response {
                            id: Some(id), status: "cancelled", lease_id: None,
                            effective_lease_ms: None, state: None,
                            remote_outcome_unknown: Some(operation_started.load(Ordering::Relaxed)),
                            sdk_version: None, protocol: None, client_version: None,
                            server_version: None, capabilities: None, fence: None,
                        };
                    }
                    result = async {
                        operation_started.store(true, Ordering::Relaxed);
                        operation.await
                    } => result
                }
            } else {
                operation.await
            };
            pending.lock().await.remove(&id);
            match result {
                Ok(None) => error_status("busy"),
                Ok(Some(lease)) => {
                    let effective = lease.effective_lease_ms();
                    let fence = lease.fence();
                    let lease_id = format!(
                        "rust-{}-{}",
                        id,
                        NEXT_LEASE_ID.fetch_add(1, Ordering::Relaxed)
                    );
                    leases.lock().await.insert(lease_id.clone(), lease);
                    Response {
                        id: Some(id.clone()),
                        status: "acquired",
                        lease_id: Some(lease_id),
                        effective_lease_ms: Some(effective),
                        state: None,
                        remote_outcome_unknown: None,
                        sdk_version: None,
                        protocol: None,
                        client_version: None,
                        server_version: None,
                        capabilities: None,
                        fence,
                    }
                }
                Err(error) => error_status(match error {
                    dilock::Error::AcquisitionUnknown => "acquisition_unknown",
                    dilock::Error::ProtocolMismatch => "protocol_mismatch",
                    dilock::Error::UnsupportedCapability => "unsupported_capability",
                    dilock::Error::WaitTimeout => "timeout",
                    dilock::Error::BackendUnavailable(_) => "backend_unavailable",
                    _ => "invalid_argument",
                }),
            }
        }
        "release" => {
            let Some(lease_id) = request.lease_id else {
                return error_status("invalid_argument");
            };
            if let Some(outcome) = released.lock().await.get(&lease_id).copied() {
                return release_response(&id, outcome);
            }
            let mut map = leases.lock().await;
            let Some(lease) = map.get_mut(&lease_id) else {
                return error_status("invalid_argument");
            };
            let outcome = lease.release().await;
            if outcome != ReleaseOutcome::Unknown {
                map.remove(&lease_id);
                released.lock().await.insert(lease_id, outcome);
            }
            release_response(&id, outcome)
        }
        "inspect" => {
            let Some(lease_id) = request.lease_id else {
                return error_status("invalid_argument");
            };
            let mut map = leases.lock().await;
            let Some(lease) = map.get_mut(&lease_id) else {
                return error_status("invalid_argument");
            };
            let state = match lease.state() {
                LeaseState::Held => "held",
                LeaseState::Releasing => "releasing",
                LeaseState::Released => "released",
                LeaseState::ReleasedUnknown => "released_unknown",
                LeaseState::Lost => "lost",
            };
            Response {
                id: Some(id.clone()),
                status: "ok",
                lease_id: Some(lease_id),
                effective_lease_ms: None,
                state: Some(state),
                remote_outcome_unknown: None,
                sdk_version: None,
                protocol: None,
                client_version: None,
                server_version: None,
                capabilities: None,
                fence: None,
            }
        }
        "cancel_request" => error_status("invalid_argument"),
        _ => error_status("invalid_argument"),
    }
}

fn release_response(id: &str, outcome: ReleaseOutcome) -> Response<'static> {
    let status = match outcome {
        ReleaseOutcome::Released => "released",
        ReleaseOutcome::AlreadyNotOwned => "already_not_owned",
        ReleaseOutcome::Unknown => "release_unknown",
    };
    Response {
        id: Some(id.to_owned()),
        status,
        lease_id: None,
        effective_lease_ms: None,
        state: None,
        remote_outcome_unknown: Some(outcome == ReleaseOutcome::Unknown),
        sdk_version: None,
        protocol: None,
        client_version: None,
        server_version: None,
        capabilities: None,
        fence: None,
    }
}

fn options(request: &Request) -> Options {
    Options {
        lease: Duration::from_millis(request.lease_ms.unwrap_or(30_000)),
        rpc_timeout: Duration::from_millis(request.rpc_ms.unwrap_or(2_000)),
        cleanup_timeout: Duration::from_millis(request.rpc_ms.unwrap_or(2_000)),
        auto_renew: request.auto_renew.unwrap_or(false),
        require_fencing: false,
    }
}

async fn write_response(output: &Arc<Mutex<io::Stdout>>, response: Response<'_>) -> io::Result<()> {
    let mut output = output.lock().await;
    let mut bytes = serde_json::to_vec(&response)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    bytes.push(b'\n');
    output.write_all(&bytes).await?;
    output.flush().await
}
