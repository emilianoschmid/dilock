# Rust package consumer

Run the exact-artifact consumer from the repository root:

```powershell
python sdk/rust/consumers/package_consumer.py `
  --output validation/implementation/rust-package-consumer.json `
  --scratch .tools/rust-package-consumer
```

The scratch directory must be new or empty. The driver copies the workspace to
scratch, runs `cargo package --locked --offline`, extracts the exact core,
Redis, and etcd `.crate` archives, and compiles, tests, and runs a consumer that
uses only those extracted archives and the local offline dependency cache. It
does not contact crates.io or either backend. No dependency or toolchain is
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
the exact `dilock`, `dilock-redis`, and `dilock-etcd` archives from that
directory; `package_build` is then recorded as `not_run_prebuilt_exact_archives`.
