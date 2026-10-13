# Fortran etcd quickstart

`main.f90` uses the Fortran ISO_C_BINDING wrapper; distributed coordination and fencing are provided by the existing C++ etcd Lock/Lease engine through Dilock C ABI. This is an experimental/internal facade, not an independent Fortran backend or stable release.

Prerequisites: CMake 3.24+, compatible C/C++ and Fortran toolchains, installed C ABI and C++ etcd profile with runtime dependencies, etcd, and pre-provisioned `reports` metadata for `etcd-native-lock-v1` and the configured epoch. Configure endpoint and epoch in the source to match. Metadata is an operator step.

Build/install the C ABI and then build the Fortran quickstart target (see `sdk/c/README.md` and `sdk/fortran/README.md` for dependency-prefix setup):

```sh
cmake -S sdk/cpp -B build/dilock-c -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DBUILD_SHARED_LIBS=ON
cmake --build build/dilock-c --parallel
cmake --install build/dilock-c --prefix build/dilock-c-install
cmake -S sdk/fortran -B build/fortran -DCMAKE_PREFIX_PATH="build/dilock-c-install"
cmake --build build/fortran --target dilock_fortran_etcd_example --parallel
```

Run only with matching metadata on a disposable etcd authority. The protected resource must atomically reject stale epoch/revision fences.

`main.f90` shows two independent locks sequentially on one provider. The SDK also ships `etcd_configured_quickstart.f90`, which demonstrates an explicit wait/lease policy and provider auto-renew. The facade does not expose all C ABI transport/TLS options; consult `sdk/fortran/README.md`. Separate handles are not an atomic multi-lock transaction.
