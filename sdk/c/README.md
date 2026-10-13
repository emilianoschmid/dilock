# Dilock C ABI

The C SDK is a versioned ABI facade over the existing C++ Redis and etcd
implementations. It does not contain a second lock protocol. Build it with the
native C++ SDK and both backend profiles:

```sh
cmake -S sdk/cpp -B build/dilock-c \
  -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DDILOCK_BUILD_TESTS=ON
cmake --build build/dilock-c
ctest --test-dir build/dilock-c --output-on-failure
cmake --install build/dilock-c --prefix "$PWD/.local/dilock-c"
```

The CMake export provides `Dilock::c`, which links the native C++ runtime and
the Redis/etcd profiles. Applications compile their public source as C17 and
link with CMake; the final link must include a C++ runtime. No separate C
package has been published.

## Common use

Provision namespace metadata before starting application clients. Set
`DILOCK_EPOCH` to the same deployment-approved value used by the metadata; the
local fallback in these short examples is `local-dev-1` only. Metadata is never
created or repaired as an effect of provider or lock creation.

```c
#include <dilock/dilock.h>
#include <stdio.h>

int main(void) {
    dilock_error error = {0};
    dilock_redis_options options;
    dilock_redis_options_init(&options);
    options.coordination_epoch = "local-dev-1";

    dilock_redis_provider *provider = NULL;
    dilock_redis_lock *lock = NULL;
    dilock_redis_lease *lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    if (dilock_redis_provider_create(&options, &provider, &error) != DILOCK_STATUS_OK) return 1;
    if (dilock_redis_lock_create(provider, "reports/daily", &lock, &error) != DILOCK_STATUS_OK) {
        dilock_redis_provider_destroy(provider);
        return 1;
    }
    if (dilock_redis_acquire_default(lock, &lease, &error) != DILOCK_STATUS_OK) return 1;

    puts("Short protected operation.");
    dilock_status status = dilock_redis_lease_release(lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) return 1;

    dilock_redis_lease_destroy(lease);
    dilock_redis_lock_destroy(lock);
    dilock_redis_provider_destroy(provider);
    return 0;
}
```

`dilock_redis_options_init` supplies a 5-second wait, 30-second requested lease,
2-second RPC and cleanup budgets, namespace `default`, and loopback Redis
endpoint defaults. The Redis quickstart accepts `DILOCK_REDIS_HOST`; the etcd
quickstart accepts `DILOCK_ETCD_ENDPOINT` as a raw gRPC target such as
`127.0.0.1:2379`. Both accept `DILOCK_EPOCH`. Supply the expected coordination
epoch explicitly. An etcd
provider is analogous; configure `endpoint`, `name_space` and
`coordination_epoch`, then use `dilock_etcd_acquire_default`. etcd metadata must
be provisioned out of band with profile `etcd-native-lock-v1`; etcd
`try_acquire` is deliberately rejected as `DILOCK_STATUS_UNSUPPORTED_CAPABILITY`.

## Full configuration and outcomes

The `dilock_redis_options` and `dilock_etcd_options` structs expose endpoint,
namespace, expected epoch, TLS (etcd), per-RPC and cleanup limits, default wait
and lease, etcd auto-renew, and an optional cleanup callback. The callback's
user-data pointer must remain valid until every provider, resource handle, and
lease created from those options is destroyed and all pending cleanup callbacks
have returned; destroying the provider alone does not end that lifetime. The
callback is called only for unknown best-effort scope cleanup and must not
perform unbounded work. Do not free or mutate callback state concurrently with
an SDK operation. Change any desired values after calling the
initializer, then create the provider. To override one attempt, call
`dilock_*_acquire(lock, wait_ms, lease_ms, ...)`; those values are the complete
attempt policy. Every function returning `dilock_status` distinguishes busy,
timeout, cancellation, backend failure, protocol mismatch, unknown acquisition
and unsupported capability. Explicit release returns its transport status and
places `Released`, `AlreadyNotOwned` or `Unknown` in the result output. Destroying
a lease performs bounded best-effort cleanup but cannot report its remote
acknowledgement; call explicit release when that result matters.

Cancellable acquire variants accept a thread-safe cancellation object that a
different thread may request. Keep it alive until acquisition returns.
Cancellation is observed between native operations; it cannot forcibly
interrupt an already running synchronous Redis command. If a command may have
reached the server, the native adapter preserves `CANCELLED` versus
`ACQUISITION_UNKNOWN` according to its cleanup evidence. The C etcd fence
accessor returns the epoch in a caller-provided buffer and the verified
`create_revision`; the buffer needs one extra byte for the trailing NUL. If
the buffer is null or too small, the accessor returns `INVALID_ARGUMENT` while
still reporting the fence revision and required epoch byte count, so the caller
can allocate and retry. The protected resource still has to enforce that fence
atomically.

Providers own their transports. Resource handles and leases retain shared native
state, so several resources can share one provider and transport pool. They are
independent locks, not an atomic multi-resource acquisition. Destroy opaque
objects with the matching `*_destroy` function. Destroying a still-active lease
triggers the native bounded cleanup path; use explicit release if its outcome is
needed before destruction. API/ABI version is
returned by `dilock_c_abi_version`; enum numeric values and struct layout are
versioned from ABI 1 and are not promised stable across a future major ABI.

The minimal runnable source files are in `examples/`. Redis namespace metadata
can be provisioned with `dilock_redis_provision_namespace` from a separately
authorized deployment utility. etcd provisioning remains an operator step.
Real-backend conformance is separate from the C17 translation-unit smoke test.
This SDK is not ready for publication until its C17 consumer, Redis/etcd live
tests, package consumer, supported-platform builds and release gates pass.

The quickstart targets have two modes. With the CMake build configured as above,
run them without arguments for `*_options_init` defaults (5-second wait,
30-second lease, 2-second RPC/cleanup), or pass any argument for the example's
fully set `billing` policy (10-second wait, 60-second lease, 1-second
RPC/cleanup, plus Redis database/host or etcd auto-renew settings):

```sh
cmake -S sdk/cpp -B build/dilock-c \
  -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON -DDILOCK_BUILD_TESTS=ON
cmake --build build/dilock-c --target dilock_c_redis_quickstart dilock_c_etcd_quickstart
build/dilock-c/dilock_c_redis_quickstart
build/dilock-c/dilock_c_redis_quickstart configured
build/dilock-c/dilock_c_etcd_quickstart
build/dilock-c/dilock_c_etcd_quickstart configured
```

Both modes still require the matching namespace metadata and deployment epoch;
the local `local-dev-1` fallback is only for a disposable development authority.
Passing an argument changes one example invocation, not the C ABI or a published
configuration preset.
