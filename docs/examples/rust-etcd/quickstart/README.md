# Rust etcd quickstart

No flag runs one short protected operation with defaults; `--configured` uses a complete explicit acquisition policy for one resource; `--shared-provider` works with two independent resources through the same provider/client. They are separate leases and are not acquired atomically.

Provision namespace metadata before normal runs. It must contain the etcd native-lock profile, identity scheme, and approved epoch. The SDK validates but does not create or repair the record. `--provision` is a separate local-development/admin action that creates only absent metadata using compare-if-absent; production metadata should be provisioned administratively.

```powershell
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_EPOCH = "dev-1"
cargo run --manifest-path docs/examples/rust-etcd/quickstart/Cargo.toml -- --provision
cargo run --manifest-path docs/examples/rust-etcd/quickstart/Cargo.toml
cargo run --manifest-path docs/examples/rust-etcd/quickstart/Cargo.toml -- --configured
cargo run --manifest-path docs/examples/rust-etcd/quickstart/Cargo.toml -- --shared-provider
```

The example uses plaintext HTTP only; it does not configure TLS roots/client certificates or username/password auth. Do not expose such an endpoint to untrusted networks; use a separately reviewed authenticated TLS integration for protected deployments. It prints etcd fence revisions, which the protected resource must atomically register and enforce with the approved epoch. Lease expiry cannot stop a paused process. Helpers invoke the closure once and await native Unlock/revoke; release observations distinguish confirmed cleanup from `Unknown`, and callback errors retain cleanup observations.
