# C Redis quickstart

`main.c` is a C17 consumer of the Dilock C ABI. The distributed Redis engine is the existing C++ implementation; this C SDK is an ABI facade, not a second lock protocol. Experimental/internal candidate; no stable published package is implied.

Prerequisites: CMake, a C++ toolchain, Redis, C ABI with the Redis profile, and pre-provisioned `billing` metadata matching `local-redis-epoch`. Set `DILOCK_REDIS_HOST` and `DILOCK_EPOCH` to match. Metadata provisioning is an operator/deployment step; the SDK only validates it.

From the repository root, build and run the SDK's C quickstart target (the checked-in SDK source is the canonical counterpart to this copy):

```sh
cmake -S sdk/cpp -B build/dilock-c -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DDILOCK_BUILD_TESTS=ON -DBUILD_SHARED_LIBS=ON
cmake --build build/dilock-c --target dilock_c_redis_quickstart
build/dilock-c/dilock_c_redis_quickstart
build/dilock-c/dilock_c_redis_quickstart configured
```

No argument uses the C ABI defaults; `configured` selects the example's full `billing` policy. This links the C consumer through `Dilock::c` and its C++ runtime dependencies. One provider can create several independent lock handles; acquire/release them sequentially when they are not an atomic multi-lock operation.
