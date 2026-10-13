# Fortran Redis quickstart

`main.f90` demonstrates the ISO_C_BINDING facade and explicit opaque-handle cleanup. The distributed Redis engine remains in the existing C++ backend behind Dilock C ABI; this is not a second Fortran protocol. The facade is experimental/internal and has no stable package claim.

Prerequisites: CMake 3.24+, compatible C/C++ and Fortran toolchains, installed C ABI with Redis, Redis, and pre-provisioned `reports` metadata matching the epoch configured in the source. The SDK only validates metadata.

Build/install the C ABI, then build the Fortran SDK quickstart target as described in `sdk/c/README.md` and `sdk/fortran/README.md`:

```sh
cmake -S sdk/cpp -B build/dilock-c -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DBUILD_SHARED_LIBS=ON
cmake --build build/dilock-c --parallel
cmake --install build/dilock-c --prefix build/dilock-c-install
cmake -S sdk/fortran -B build/fortran -DCMAKE_PREFIX_PATH="build/dilock-c-install"
cmake --build build/fortran --target dilock_fortran_redis_example --parallel
```

Run the resulting target only against the matching disposable Redis authority. CMake may also require the native dependency prefix in `CMAKE_PREFIX_PATH`.

The source uses default acquisition policy. For the full path, `redis_open` accepts optional host, port and namespace, and `redis_acquire` accepts a complete wait/lease pair. The Fortran facade intentionally exposes fewer provider transport settings than the C ABI; see `sdk/fortran/README.md`. A provider can create multiple independent resource handles and use them sequentially; that does not make their acquisitions atomic.
