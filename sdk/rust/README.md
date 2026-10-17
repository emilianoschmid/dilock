# Dilock Rust SDK

This workspace contains shared Rust types (`dilock`), Redis and etcd adapters,
and `dilock-consul`, a synchronous Rust ownership facade over the optional
Consul-enabled `dilock_c` ABI. The Consul crate uses the shared C++ engine; it
is not a separate HTTP implementation. Build and install the C ABI with
`DILOCK_BUILD_CONSUL=ON` and `DILOCK_BUILD_C_API=ON`, then set
`DILOCK_C_LIB_DIR` to the directory containing `dilock_c` when building Rust.
Async applications should call the synchronous facade on their runtime's
blocking pool. C handles are thread-affine at this facade and are not `Send`.
No adapter provides exactly-once effects or stops a paused process after its
lease expires.

## Redis usage

```rust
use dilock::{Options, ReleaseOutcome};
use dilock_redis::RedisProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = RedisProvider::new(
        "redis://127.0.0.1/",
        "default",
        "deployment-epoch-1",
    )?;
    let resource = provider.lock("reports/daily")?;
    let mut lease = resource.try_acquire(Options::default()).await?
        .ok_or("resource is busy")?;
    lease.ensure_held()?;
    // Use resource-side idempotency or fencing for protected external writes.
    assert_eq!(lease.release().await, ReleaseOutcome::Released);
    Ok(())
}
```

Provision Redis namespace metadata before acquiring. Its key is
`dilock:namespace:<namespace>:meta` and its JSON must use protocol major 1,
identity scheme `sha256-framed-utf8-v1`, profile
`redis-single-authority-v1`, the deployment's stable `coordination_epoch`, and
`fixed_lease_ms` set to null. Runtime clients never create or repair metadata.

`RedisProvider` and `RedisHandle` are cloneable, reusable selections of the
same configured authority. `RedisLease` is deliberately non-Clone and is
mutated through exclusive mutable access. Use explicit async release when its
acknowledgement matters. Dropping a live lease only schedules tracked, bounded,
owner-checked best-effort cleanup; the runtime must remain alive for it to run,
and server expiry remains the crash fallback. Cancellation or timeout after a
SET may have been sent returns an unknown outcome and never returns a lease. A
late SET can leave an orphan until TTL expiry. Explicit release and fallback
cleanup share one monotonic cleanup budget across connection setup and the
owner-checked Lua command.

The adapter verifies the conservative deadline against the monotonic SET send
time and rejects invalid TTL and RPC budgets before sending. Waiting retries
only confirmed NX contention with bounded jitter. Redis errors and unknown
acquisition outcomes are distinct. Setting `auto_renew` or `require_fencing`
fails with an unsupported-capability error. Renewal and release compare the
full owner value atomically using the canonical Lua scripts.

Use a protected, non-evicting keyspace and a stable coordinator epoch. Manual
deletion, eviction, state-losing restart, and asynchronous replica promotion (including Sentinel/Cluster failover) can
invalidate assumptions. Owner values prevent stale cleanup from deleting a
successor; they are not fencing tokens. Protect external writes with
resource-side serialization, transactions, idempotency, and fencing where
available.

### Rust application quickstart

`RedisHandle::acquire_default().await` uses a five-second wait and
`Options::default()` (30-second requested lease, two-second RPC and cleanup
budgets, no auto-renewal). `with_lease` is the awaited scope form: it runs the
callback once and explicitly releases the lease. It returns `WithLeaseOutput`
only after `Released`; `AlreadyNotOwned` and `Unknown` become the typed
`WithLeaseError::Release` with the exact outcome. If the callback fails,
`WithLeaseError::Work` preserves that error alongside the cleanup observation.
The `Drop` path remains only a tracked fallback if the helper future is
cancelled.

The complete policy remains available through the existing
`acquire(wait, options)` call and `with_lease_with(wait, options, callback)`.
`Options` is a complete per-acquisition policy; struct update syntax is the
idiomatic way to change one field. One provider can create multiple handles;
two different resources can be held at once but are not acquired atomically.
See [`Redis quickstart`](../../docs/examples/rust-redis/quickstart/README.md)
and [`etcd quickstart`](../../docs/examples/rust-etcd/quickstart/README.md)
for minimal and configured programs.

## Consul C ABI facade

`dilock-consul` exposes a reusable provider and handle, one non-cloneable lease
per acquisition, cancellation (the atomic token can be shared with a cancelling
thread), try-acquire, server-granted TTL, explicit
release outcomes, and `(coordination_epoch, LockIndex)` fencing. Provision the
namespace metadata explicitly before worker startup. Keep lock KV entries
after release so their `LockIndex` cannot reset. ACL tokens are redacted from
`ConsulOptions` debug output and should be loaded from the deployment secret
manager. The Rust API preserves blocking native call semantics; use a blocking
executor pool in async services.

```rust
use dilock_consul::{ConsulOptions, ConsulProvider, ReleaseOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ConsulOptions {
        namespace: "billing".into(),
        coordination_epoch: "production-incarnation-1".into(),
        ..ConsulOptions::default()
    };
    dilock_consul::provision_namespace(&options)?;
    let provider = ConsulProvider::new(&options)?;
    let lock = provider.lock("invoice/42/reconcile")?;
    let mut lease = lock.acquire_default()?;
    let fence = lease.fence()?;
    // Atomically register and enforce (fence.coordination_epoch, fence.lock_index)
    // at the protected resource before performing external side effects.
    assert_eq!(lease.release()?, ReleaseOutcome::Released);
    Ok(())
}
```

The C ABI library is a native runtime dependency and must be discoverable by
the platform loader when the consumer runs. This facade does not provide a
Rust-only backend binary.

## Etcd native Lock usage

```rust
use dilock::{Options, ReleaseOutcome};
use dilock_etcd::EtcdProvider;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = EtcdProvider::new(
        "http://127.0.0.1:2379",
        "default",
        "production-cluster-incarnation-2026-01",
    )?;
    // Provision once through an explicit administrator action. Runtime
    // acquisition only validates this key and never creates or repairs it.
    provider.provision_namespace(Duration::from_secs(2)).await?;
    let handle = provider.lock("reports/daily")?;
    let mut lease = handle
        .acquire(Duration::from_secs(10), Options::default())
        .await?;
    lease.ensure_held()?;
    let fence = lease.fence();
    // Register (fence.epoch, fence.revision) atomically at the protected resource.
    assert_eq!(lease.release().await, ReleaseOutcome::Released);
    Ok(())
}
```

Each etcd attempt gets a distinct server lease and starts managed keepalive
before waiting in etcd's native FIFO Lock queue. After Lock returns, the SDK
retains its opaque key, performs an exact linearizable Range, verifies that
the key is attached to the expected lease, and exposes its `create_revision`
with the stable metadata epoch. That pair orders acquisitions only inside its
epoch; a restore or cluster replacement needs a coordinated fence migration.
The native etcd Lock API has no try-lock response, so `try_acquire` returns
`UnsupportedCapability`; a timed-out waiter is never misreported as ordinary
contention. Fencing is supported.
Keepalive failure or a late acknowledgement makes local state Lost; a reconnect
cannot restore permission.

The current Rust adapter does not enable `etcd-client` TLS features or expose
etcd credentials in `EtcdProvider`. Direct TLS or authenticated etcd endpoints
are therefore unsupported by this package version. A deployment that requires
those controls must provide a trusted local transport proxy; do not send etcd
traffic over an untrusted network. This limitation is separate from the
server-side lock, lease, and fencing behavior described above.

Cancellation or failure after lease grant stops the keeper and attempts a
bounded revoke. Dropping a held `EtcdLease` schedules bounded revoke cleanup if
a Tokio runtime is active; explicit async release performs Unlock on the exact
opaque key and then revokes that attempt's lease. Explicit release may be
canceled; its remote outcome is then unknown, and Drop schedules revoke.
Provider and handle clones do not share leases. Tokio tasks may move between
threads; one lease's mutations require exclusive mutable access. Configure TLS
and authentication at the transport layer in deployment infrastructure; the
SDK never changes endpoint, profile, or metadata on failure.

## JSON Lines worker

Build the worker with:

```powershell
cargo build --release -p dilock-redis-worker
```

The config's `backend` may be `redis` or `etcd`; etcd requires its namespace
metadata to be provisioned first. Run the executable with the JSON config path
as its only argument. It reads one JSON request per stdin line, writes one
response per stdout line, and sends diagnostics to stderr. It supports request
cancellation for pending acquire operations; cancellation before the operation
starts is reported as known local cancellation, while cancellation after
polling may have dispatched the command is marked outcome-unknown. Etcd replies
include the verified `{ "epoch": "...", "value": "<decimal revision>" }`
fence. Shutdown waits for in-flight requests, explicitly releases retained
leases, and awaits tracked cleanup.

## Local checks

From this directory run `cargo fmt --all -- --check`,
`cargo clippy --offline --workspace --all-targets -- -D warnings`,
`cargo test --offline --workspace`, `cargo test --offline --workspace --doc`,
and `cargo package --offline --workspace --no-verify`. etcd-client requires a
`protoc` executable at build time; set `PROTOC` to the locally installed,
version-pinned compiler. A release candidate also needs the declared MSRV,
feature matrix, a clean downstream package consumer, and directed worker runs.
Package builds are local only; this repository does not publish automatically.
