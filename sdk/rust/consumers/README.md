# Rust package consumer

Run the exact-artifact consumer from the repository root. It packages and
consumes Core, Redis, etcd, Consul and PostgreSQL crates:

```powershell
python sdk/rust/consumers/package_consumer.py `
  --output validation/implementation/rust-package-consumer.json `
  --scratch .tools/rust-package-consumer
```

The scratch directory must be new or empty. The driver copies the workspace to
scratch, runs `cargo package --locked --offline`, extracts the exact core,
Redis, etcd, Consul and PostgreSQL `.crate` archives, and compiles, tests, and
runs a consumer that uses those extracted archives and the local offline dependency
cache. The Consul facade is type-checked against the public artifact surface;
the consumer does not invoke native Consul calls. A C ABI installation is
still required for the native link step; pass its library directory with
`--dilock-c-lib-dir` or `DILOCK_C_LIB_DIR`. Native Consul behavior is covered
by the dedicated C ABI contract suite. The driver does not contact crates.io
or a Consul backend. No dependency or toolchain is
downloaded; Rust 1.88 must be selected separately when validating the MSRV.

For a checkout that keeps its caches outside the repository, pass
`--cargo-home`, `--rustup-home`, and `--protoc`, or set
`DILOCK_CARGO_HOME`, `DILOCK_RUSTUP_HOME`, and `DILOCK_PROTOC`. If omitted, the
driver uses existing workspace-local `.tools` caches and otherwise preserves
the normal Cargo/Rustup home from the environment. Cargo offline mode is always
enabled, so a clean cache fails closed instead of contacting crates.io.

To reuse an existing Cargo build cache when disk space is constrained, pass
`--target-dir` or set `DILOCK_CARGO_TARGET_DIR`. The driver writes package and
consumer build outputs there and never cleans that directory. Without this
option, Cargo keeps build targets inside the isolated scratch workspace.

To validate already-staged archives without rebuilding them, pass
`--archives-dir <directory>`. The driver copies, hashes, extracts, and consumes
the exact `dilock`, `dilock-redis`, `dilock-etcd`, `dilock-consul` and
`dilock-postgres` archives from that
directory; `package_build` is then recorded as `not_run_prebuilt_exact_archives`.
