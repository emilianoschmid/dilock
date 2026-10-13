# C++ etcd public API consumer

This standalone CMake consumer links the installed `Dilock::etcd` target. It creates two independently owned providers, observes a bounded native queue wait expire while the owner holds a unique resource, releases the owner, and verifies that the successor receives a larger `create_revision` in the same configured epoch.

The app only reads namespace metadata. Before connecting, an operator must provision `/dilock/namespaces/<namespace>/meta` with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The etcd fence is meaningful only when the protected resource atomically validates the epoch and rejects older revisions. Lease expiry cannot stop a stale process or make a protected side effect exactly once.

Build from an existing local SDK build/install and already installed dependencies; these commands do not use a package registry:

```powershell
cmake --install .tools/dilock-cpp-etcd --prefix .tools/cpp-etcd-consumer-install
cmake -S docs/examples/cpp-etcd -B .tools/cpp-etcd-consumer-build -DCMAKE_PREFIX_PATH="$((Resolve-Path .tools/cpp-etcd-consumer-install).Path)"
cmake --build .tools/cpp-etcd-consumer-build --config Release
```

Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to one local or approved endpoint and its existing metadata. The C++ gRPC target is `host:port` (for example `127.0.0.1:2379`, without an `http://` prefix). The example uses an insecure endpoint by default; do not expose it to an untrusted network. The app does not provision metadata or start etcd.

This consumer exercises one etcd authority. It does not cover quorum failure,
authority replacement, or enforcement of the fence by an external resource.
