# Rust Redis quickstart

Run without flags for one short protected operation using defaults. `--configured` runs one resource with a complete explicit wait/lease/RPC/cleanup policy. `--shared-provider` operates on two distinct resources using the same provider. Shared provider/client configuration does not make acquisitions atomic: each lease is independent and releasing one does not release the other.

Before running, provision `dilock:namespace:default:meta` with the Redis profile and the same stable, approved epoch used by every participant. The SDK validates but never creates or repairs metadata. The example uses a short callback: a lease's awaited cleanup does not extend its validity or stop work after loss.

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379/"
$env:DILOCK_EPOCH = "dev-1"
cargo run --manifest-path docs/examples/rust-redis/quickstart/Cargo.toml
cargo run --manifest-path docs/examples/rust-redis/quickstart/Cargo.toml -- --configured
cargo run --manifest-path docs/examples/rust-redis/quickstart/Cargo.toml -- --shared-provider
```

The helpers call the work closure once and await owner-checked release; `ReleaseOutcome::Unknown` remains visible, and callback errors retain the cleanup observation. The sample URI is plaintext. Use the Redis client's reviewed TLS/auth URI configuration in protected environments and keep credentials out of source control. Redis single-authority does not promise strong fencing across unsafe failover.
