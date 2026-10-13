# Nim etcd quickstart

`main.nim` calls the Nim facade over the native C++ etcd Lock/Lease engine through Dilock C ABI. It does not implement gRPC in Nim. Experimental/internal candidate; no stable Nimble release is implied.

Prerequisites: Nim 2.2+, shared C ABI built with etcd and its C++/gRPC runtime dependencies, etcd, and pre-provisioned `billing` metadata for `etcd-native-lock-v1` and `local-etcd-epoch`. Set `DILOCK_ETCD_ENDPOINT` to raw `host:port` and `DILOCK_EPOCH` to match. Metadata is provisioned separately.

After installing the C ABI as described in `sdk/c/README.md`, compile from the repository root:

```sh
nim c --path:sdk/nim/src --passC:-I.local/dilock-c/include --passL:-L.local/dilock-c/lib --passL:-ldilock_c -o:build/nim-etcd docs/examples/nim-etcd/quickstart/main.nim
```

Make the shared ABI and native dependencies visible to the runtime loader. The printed etcd fence is effective only when the protected resource atomically enforces it. Use a disposable namespace.

The common path leaves provider settings at defaults. For configured work, `connectEtcd(endpoint, namespace, epoch, autoRenew = true)` enables provider keepalive; use `lock.acquire(waitMs, leaseMs)` for a complete per-attempt wait/lease policy. Multiple handles from one provider are separate resources and do not acquire atomically. See `sdk/nim/README.md` for TLS PEM options and limits of the facade.
