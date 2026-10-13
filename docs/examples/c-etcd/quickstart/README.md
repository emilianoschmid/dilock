# C etcd quickstart

`main.c` is a C17 consumer of the Dilock C ABI. Distributed coordination uses the existing native C++ etcd Lock/Lease engine; C is an ABI facade, not an independent etcd implementation. Experimental/internal candidate; no stable published package is implied.

Prerequisites: CMake, a C++ toolchain, etcd, C ABI with the etcd profile, and pre-provisioned `billing` metadata for `etcd-native-lock-v1` and `local-etcd-epoch`. Set `DILOCK_ETCD_ENDPOINT` to a raw gRPC target such as `127.0.0.1:2379` and set `DILOCK_EPOCH` to match. The SDK never provisions metadata.

From the repository root, build and run the SDK's etcd quickstart target (the checked-in SDK source is the canonical counterpart to this copy):

```sh
cmake -S sdk/cpp -B build/dilock-c -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DDILOCK_BUILD_TESTS=ON -DBUILD_SHARED_LIBS=ON
cmake --build build/dilock-c --target dilock_c_etcd_quickstart
build/dilock-c/dilock_c_etcd_quickstart
build/dilock-c/dilock_c_etcd_quickstart configured
```

No argument uses the C ABI defaults; `configured` selects the example's complete per-provider policy, including auto-renew. This links via `Dilock::c` and its C++/gRPC dependencies. A provider can create several independent lock handles; sequential acquisitions are not an atomic multi-lock transaction. The protected resource must atomically reject stale `(epoch, create_revision)` values.
