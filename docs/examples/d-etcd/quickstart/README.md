# D etcd quickstart

`main.d` demonstrates D `scope(exit)` cleanup and reads the verified etcd fence. Distributed Lock/Lease operations run in the shared C++ etcd engine through Dilock C ABI; this experimental binding is not an independent D gRPC implementation or stable DUB release.

Prerequisites: DMD or compatible compiler/linker, shared C ABI with etcd and native gRPC dependencies, etcd, and pre-provisioned `billing` metadata for `etcd-native-lock-v1` and `local-etcd-epoch`. Set `DILOCK_ETCD_ENDPOINT` as raw `host:port` and set `DILOCK_EPOCH` to match. Provision metadata out of band.

After installing the C ABI following `sdk/c/README.md`, compile from the repository root (adjust paths if needed):

```sh
dmd -Isdk/d/source -L-L.local/dilock-c/lib -L-ldilock_c -of=build/d-etcd docs/examples/d-etcd/quickstart/main.d
```

The shared library and native dependencies must be loadable. Enforce the printed `(epoch, create_revision)` atomically at the protected resource. Use a disposable etcd namespace.

The short `EtcdProvider.connect(endpoint, namespace, epoch)` path keeps defaults. `EtcdProviderOptions` is the full configuration path, including TLS PEM contents and opt-in `autoRenew`; `lock.acquire(waitMs, leaseMs)` sets one attempt's policy. Several handles from one provider are independent resources, not an atomic multi-lock operation.
