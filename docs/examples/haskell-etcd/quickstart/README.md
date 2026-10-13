# Haskell etcd quickstart

`main.hs` is an experimental Haskell facade over Dilock C ABI v1 and the shared native C++ etcd Lock/Lease engine. It does not implement gRPC in Haskell; no stable package is implied.

Prerequisites: GHC/Cabal, C ABI and C++ etcd profile installed with shared runtime dependencies, etcd, and pre-provisioned `reports` metadata for `etcd-native-lock-v1` and `local-etcd-epoch`. Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_COORDINATION_EPOCH` to match. Metadata provisioning is outside this SDK.

Build/install the C ABI as in `sdk/c/README.md`. From `sdk/haskell`, run the packaged C-ABI quickstart (the checked-in Cabal example is the canonical source corresponding to this copy):

```sh
LD_LIBRARY_PATH="$PWD/../../.local/dilock-c/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" cabal run dilock-etcd-cabi-quickstart --flags=+etcd-c-abi --extra-include-dirs=../../.local/dilock-c/include --extra-lib-dirs=../../.local/dilock-c/lib
```

Runtime loading must resolve the C ABI's C++/gRPC dependency closure. The protected resource must atomically reject stale `(epoch, create_revision)` writes. Use a disposable etcd authority.

The quick path can use `defaultEtcdOptions epoch` and `defaultEtcdAcquireOptions`; configure endpoint, namespace, TLS PEM values and `autoRenew` on `EtcdOptions` when needed. One provider can create multiple independent handles with `withEtcdLock`; acquire them sequentially unless the application explicitly coordinates a multi-lock workflow. They are not an atomic group. The checked-in source currently opts into auto-renew because the native etcd lease may outlive its initial TTL.
