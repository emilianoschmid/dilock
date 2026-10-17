# Dilock Fortran facade

This module uses ISO_C_BINDING to call the versioned Dilock C ABI. Redis, etcd
and Consul implementations remain in the existing native adapters; this package does
not implement another protocol. The C ABI and Redis, etcd and Consul profiles must be
built and installed first, as described in [`sdk/c/README.md`](../c/README.md).
This SDK installs a CMake package exporting `DilockFortran::fortran`; the
consumer must be built with a Fortran compiler compatible with the one that
produced the installed `.mod` file. Configure the SDK with the C ABI prefix,
then install it to a separate prefix:

```sh
cmake -S sdk/fortran -B build/fortran \
  -DCMAKE_PREFIX_PATH="<C-ABI-install-prefix>;<C-ABI-dependency-prefix>"
cmake --build build/fortran --parallel
cmake --install build/fortran --prefix build/fortran-install
cmake -S sdk/fortran/consumers/cmake -B build/fortran-consumer \
  -DCMAKE_PREFIX_PATH="build/fortran-install;<C-ABI-install-prefix>;<C-ABI-dependency-prefix>"
cmake --build build/fortran-consumer --parallel
```

The downstream consumer resolves the installed Fortran package without using
the SDK source tree. Running a lock example also needs a reachable backend and
pre-provisioned namespace metadata.

Consul metadata is provisioned explicitly before `consul_open`; normal provider
construction never creates or repairs it. Pass the agent URL, namespace and
expected epoch, with an optional ACL token. `consul_fence` returns the verified
`(epoch, LockIndex)` pair; the protected resource must enforce it atomically.
Retain Consul lock KV keys so `LockIndex` cannot reset. See
[`consul_quickstart.f90`](examples/consul_quickstart.f90) for the C ABI wrapper
flow and fence retrieval.

```fortran
program redis_example
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_null_ptr
  use dilock_fortran
  implicit none
  type(c_ptr) :: provider = c_null_ptr, resource_lock = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message
  integer(c_int) :: status, outcome

  status = redis_open('local-dev-1', provider, message)
  if (status /= dilock_ok) error stop 'provider creation failed'
  status = redis_lock(provider, 'reports/daily', resource_lock, message)
  if (status /= dilock_ok) then
    call redis_destroy_provider(provider)
    error stop 'resource handle creation failed'
  end if
  status = redis_acquire_default(resource_lock, lease, message)
  if (status /= dilock_ok) then
    call redis_destroy_lock(resource_lock)
    call redis_destroy_provider(provider)
    error stop 'acquisition failed'
  end if

  ! Keep protected work short and independently fence stale writers if needed.
  status = redis_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    call redis_destroy_lease(lease)
    call redis_destroy_lock(resource_lock)
    call redis_destroy_provider(provider)
    if (outcome == dilock_already_not_owned) error stop 'lease is no longer owned'
    error stop 'release was not confirmed'
  end if
  call redis_destroy_lease(lease)
  call redis_destroy_lock(resource_lock)
  call redis_destroy_provider(provider)
end program
```

The short initializer supplies native defaults; the coordination epoch remains
mandatory. `redis_open` also accepts optional host, port and namespace. Errors
remain distinct numeric C ABI statuses, including busy, timeout, backend error,
unknown acquisition and unsupported capability; inspect the returned message
for diagnostics. A non-OK status is never ordinary contention unless its value
is `dilock_busy`. Unknown status values from a newer or mismatched C ABI are
mapped to `dilock_internal`; unknown release outcomes become
`dilock_release_unknown`, so an unrecognized value can never appear acknowledged.
Only `dilock_released` confirms this call released the lease. Treat
`dilock_already_not_owned` as a lost/stale lease, not as successful cleanup.

To set the endpoint, namespace and complete acquisition wait/lease policy
explicitly, use the optional `redis_open` arguments and `redis_acquire`. This
convenience facade does not expose provider credentials, transport timeouts or
cleanup observers.

```fortran
program configured_redis_example
  use, intrinsic :: iso_c_binding, only: c_ptr, c_int, c_int32_t, c_int64_t, c_null_ptr
  use dilock_fortran
  implicit none
  type(c_ptr) :: provider = c_null_ptr, resource_lock = c_null_ptr, lease = c_null_ptr
  character(len=384) :: message
  integer(c_int) :: status, outcome

  status = redis_open('prod-east-1', provider, message, host='redis.internal', &
                      port=6380_c_int32_t, name_space='reports')
  if (status /= dilock_ok) error stop 'provider creation failed'
  status = redis_lock(provider, 'daily', resource_lock, message)
  if (status /= dilock_ok) then
    call redis_destroy_provider(provider)
    error stop 'resource handle creation failed'
  end if
  status = redis_acquire(resource_lock, 10000_c_int64_t, 60000_c_int64_t, lease, message)
  if (status /= dilock_ok) then
    call redis_destroy_lock(resource_lock)
    call redis_destroy_provider(provider)
    error stop 'acquisition failed'
  end if
  status = redis_release(lease, outcome, message)
  if (status /= dilock_ok .or. outcome /= dilock_released) then
    call redis_destroy_lease(lease)
    call redis_destroy_lock(resource_lock)
    call redis_destroy_provider(provider)
    if (outcome == dilock_already_not_owned) error stop 'lease is no longer owned'
    error stop 'release was not confirmed'
  end if
  call redis_destroy_lease(lease)
  call redis_destroy_lock(resource_lock)
  call redis_destroy_provider(provider)
end program
```

Character arguments must be supplied as UTF-8 bytes in default-character
storage and are passed byte-for-byte to the C ABI; trailing blanks are
significant and are not trimmed. Embedded NUL bytes are rejected before calling
the ABI because C strings would otherwise truncate. A fixed-width
`character(len=N)` variable therefore contributes all N characters. Use an
exact-length literal or an appropriately sized deferred-length variable when
padding is not part of the resource, namespace or epoch. `etcd_fence` uses the
caller's output character length as its byte capacity and fails explicitly
when the C ABI epoch will not fit. On failure, it clears the epoch and returns
revision zero; consume both outputs only when the status is `dilock_ok`.

For etcd, call `etcd_open(endpoint, epoch, ...)` with a gRPC target such as
`etcd:2379` (do not include an `http://` URL scheme), create a resource with
`etcd_lock`, acquire with `etcd_acquire`, and obtain its create-revision fence
with `etcd_fence`. The protected application resource must atomically reject
stale fences. The wrappers are synchronous and provide no thread-safety
guarantee. Fortran `c_ptr` handles are copyable at the language level: treat
them as move-only by convention, do not assign aliases, and invoke the matching
destroy function exactly once. Explicit release reports acknowledgement;
destroying an active lease can only request bounded best-effort cleanup.
Initialize every output handle to `c_null_ptr` before calling `redis_open`,
`etcd_open`, either `*_lock` function, or an acquisition function. Each wrapper
rejects a non-null output handle with `dilock_invalid_argument` and preserves
the existing handle. Release as needed, destroy it, and then reuse the output
variable. This prevents an overwritten `c_ptr` from leaking an opaque provider,
lock, or lease.

The etcd facade also has a default acquisition helper (5-second wait,
30-second requested lease). The coordination epoch and endpoint remain
explicit, and `etcd_fence` returns the configured epoch with the exact lock
revision.

The checked-in [`examples/etcd_quickstart.f90`](examples/etcd_quickstart.f90)
shows the complete lifecycle: one provider reused for two resource handles,
default acquisition, fence retrieval and explicit release. The target
`dilock_fortran_etcd_example` is built with the SDK. Run it only against a
disposable etcd endpoint with the `reports` namespace metadata pre-provisioned.


The fully configured etcd path accepts an explicit wait/lease pair and the
optional provider auto-renew flag. This facade does not expose TLS credentials,
RPC deadlines or cleanup observers.

Both backends also expose `cancellation_create`, `cancellation_request`, and
`cancellation_destroy`, plus `*_acquire_with_cancellation` and
`*_acquire_default_with_cancellation`. Cancellation is a caller-owned opaque
C ABI handle. Keep it alive until the synchronous acquire call returns; to
interrupt a blocked call, request cancellation from another thread managed by
the host application. A cancellation handle is one-shot: create a new one for
another cancellable attempt. The returned `dilock_cancelled` status remains
distinct from contention and backend failure. Cancellation only applies while
acquiring; it does not stop protected work after a lease has been returned.
Destroy the cancellation handle after the acquire call has returned.

`etcd_try_acquire(lock, lease_ms, lease, message)` is exposed for an explicit
capability result. etcd's native `Lock` RPC queues waiters and has no safe
nonblocking operation, so this call returns `dilock_unsupported` without
starting a network operation. The lease output must still be initialized to
`c_null_ptr`; the function rejects a non-null output without changing it.

For the fully configured path, see
[`examples/etcd_configured_quickstart.f90`](examples/etcd_configured_quickstart.f90)
and the `dilock_fortran_etcd_configured_example` CMake target. It uses an
explicit wait/lease pair and enables provider auto-renewal. This facade does
not expose TLS credentials, RPC deadlines or cleanup observers.

## Real backend consumer test

When `BUILD_TESTING=ON`, CMake also compiles `dilock_fortran_live_consumer`
against the installed `Dilock::c` package. It is intentionally not registered
in default CTest because it requires a disposable Redis or etcd service and
matching pre-provisioned namespace metadata. Run it explicitly as:

```sh
build/dilock_fortran_live_consumer redis redis 6379 fortran-live fortran-live-epoch
build/dilock_fortran_live_consumer etcd etcd:2379 fortran-live fortran-live-epoch
```

The consumer opens independent providers and checks contention, lease expiry,
stale-owner release, handoff, and etcd fencing. Namespace metadata is an
operator/deployment concern; the test does not create it through the Fortran
facade. Use isolated test namespaces and disposable backend instances.
