# Zig etcd quickstart

`main.zig` demonstrates Zig `defer` and obtains an etcd fence. The distributed lock is implemented by the shared C++ etcd Lock/Lease engine via Dilock C ABI; this binding is experimental/internal, not an independent Zig gRPC backend or stable release.

Prerequisites: pinned Zig 0.17.0, shared C ABI built with etcd and its native gRPC dependencies, etcd, and pre-provisioned `billing` metadata for `etcd-native-lock-v1` and the configured epoch. Use a raw `host:port` endpoint and configure it with the epoch in the source. Metadata is provisioned separately.

After installing the C ABI, build the SDK project from the repository root as described in `sdk/zig/README.md`:

```sh
cd sdk/zig
zig build -Dc-include=../../.local/dilock-c/include -Dc-lib=../../.local/dilock-c/lib
```

This compiles, but does not execute, the SDK examples. Runtime loading must resolve `libdilock_c` and native dependencies. Fencing is effective only if the protected resource atomically rejects stale `(epoch, create_revision)` values.

The common connection path uses defaults; `EtcdProvider.connectWithOptions(EtcdProviderOptions{ .endpoint = ..., .namespace = ..., .coordination_epoch = ..., .auto_renew = true })` exposes full provider options, including TLS PEM contents. Use `lock.acquire(wait_ms, lease_ms)` for per-attempt policy. One provider may own multiple independent handles; sequential handles are not an atomic multi-lock transaction.
