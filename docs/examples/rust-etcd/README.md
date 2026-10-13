# Rust etcd public API consumer

This Tokio consumer uses two separate `EtcdProvider` configurations to queue acquisitions against one etcd authority. It expects the contender's finite wait to return `Error::WaitTimeout` while the first lease is held, releases explicitly, then verifies that the successor has a greater etcd `create_revision` in the same epoch. The lease is non-`Clone`; `Drop` does not perform asynchronous release, so the example awaits `release` and provider cleanup.

Before running, provision `/dilock/namespaces/<namespace>/meta` out of band with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the operator-approved `coordination_epoch`, and `fixed_lease_ms: null`. The program never provisions or changes metadata. A protected resource must atomically enforce the epoch/revision pair. etcd lease expiry cannot stop stale work.

Build using only the path dependencies and cached crates in this checkout:

```powershell
cargo build --offline --manifest-path docs/examples/rust-etcd/Cargo.toml
```

Then set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH`, and
run with `cargo run --offline --manifest-path docs/examples/rust-etcd/Cargo.toml`.
The command requires a reachable etcd authority and matching pre-provisioned
metadata; it does not create or repair that metadata.
