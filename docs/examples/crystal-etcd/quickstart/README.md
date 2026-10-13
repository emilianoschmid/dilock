# Crystal etcd quickstart

`main.cr` uses the Crystal facade over Dilock C ABI and the shared native C++ etcd Lock/Lease engine. It is not a Crystal gRPC implementation. The facade is experimental and not ready for stable release.

Prerequisites: Crystal 1.21+, Shards, installed C ABI and C++ etcd profile with gRPC/protobuf/OpenSSL runtime dependencies, etcd, and pre-provisioned `billing` metadata using `etcd-native-lock-v1`. Set `DILOCK_ETCD_ENDPOINT` to raw gRPC `host:port`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to match. Metadata is never created by the SDK.

Install Crystal dependencies and run from the repository root (adjust the ABI library path):

```sh
cd sdk/crystal
shards install
cd ../..
crystal run --path=sdk/crystal/src --path=sdk/crystal/lib docs/examples/crystal-etcd/quickstart/main.cr --link-flags "-L.local/dilock-c/lib"
```

The runtime loader must also find the C ABI and native dependencies. The protected resource must atomically reject stale `(epoch, create_revision)` values. Use a disposable etcd namespace.

The sample reuses one provider for two independent resources and runs their scopes sequentially; separate handles do not form an atomic multi-lock transaction. Its first scope uses defaults and the second overrides wait/lease durations. `auto_renew` is an explicit provider setting, not an implicit default.
