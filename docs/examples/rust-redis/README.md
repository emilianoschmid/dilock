# Rust Redis public API consumer

This standalone consumer uses the public Rust API and a real, already-provisioned Redis authority. It demonstrates acknowledged contention, a bounded wait, TTL expiry, a successor acquisition, and owner-checked stale release. It does not start Redis or create namespace metadata.

## Prerequisites and metadata

- Rust and Cargo 1.88 or newer (the SDK declares Rust 1.88 as its MSRV).
- A reachable Redis endpoint and an operator-provisioned Dilock namespace.
- Environment variables `DILOCK_REDIS_URL`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH`. `DILOCK_RESOURCE` optionally overrides the example resource name.

Provision the namespace out of band with protocol major `1`, identity scheme `sha256-framed-utf8-v1`, profile `redis-single-authority-v1`, the deployment's stable `coordination_epoch`, and `fixed_lease_ms: null`. Its key is `dilock:namespace:<namespace>:meta`. Runtime SDK calls validate but never create or repair metadata. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md) and [`spec/backends/REDIS.md`](../../../spec/backends/REDIS.md). Run this short-TTL demonstration only against a non-production namespace.

The endpoint may contain credentials. Supply it through a secret manager or environment injection, not source files or shell history. Do not paste secrets into command lines that may be logged.

## Build and run

From the repository root, build against this checkout's SDK sources. Cargo resolves the example's third-party dependencies; use `--offline` only when those dependencies are already cached:

```powershell
cargo build --locked --manifest-path docs/examples/rust-redis/Cargo.toml
```

Then supply the provisioned identity and run the consumer:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
cargo run --locked --manifest-path docs/examples/rust-redis/Cargo.toml
```

Expected output on an uncontended resource:

```text
contender: acknowledged busy
contender: bounded wait ended with timeout
contender: acquired after Redis TTL
original owner: stale release left successor untouched
successor: still owns the resource
successor: release acknowledged
```

## Ownership and limits

Each `RedisProvider` owns a client handle and each `RedisHandle` is a reusable resource selection. A `RedisLease` is a non-`Clone` ownership generation; mutable operations require exclusive `&mut` access. Tokio futures may move across runtime threads, but do not use a lease concurrently. Explicit `release().await` is used where its acknowledgement matters. Dropping a live lease never blocks: it schedules tracked, bounded, owner-checked best-effort cleanup only when a Tokio runtime is active. Keep the runtime alive and call `wait_for_cleanup().await` when waiting for scheduled cleanup matters; Redis TTL is the crash fallback.

Cancellation before a Redis command is dispatched has no remote effect. If cancellation or timeout happens after `SET NX PX` may have been sent, the SDK reports/retains an unknown acquisition outcome and schedules owner-specific cleanup; it does not turn uncertainty into busy and does not return a lease. A late command may leave an orphan until TTL expiry. Cancellation during explicit release leaves its remote outcome unknown; Drop then schedules best-effort cleanup. The example does not cancel operations, so it does not test those paths.

The adapter retries only confirmed contention in `acquire`; this example checks the explicit `WaitTimeout` category. Redis single-authority mode does not provide strong fencing or failover safety. Expiry and `LeaseState::Lost` cannot stop paused or stale application work. Protect external writes with resource-side transactions, idempotency, and fencing where available. This sample is not a production-readiness claim.
