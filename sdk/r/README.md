# Dilock for R (experimental)

This experimental package borrows a `redux` hiredis connection supplied by the caller. It verifies provisioned namespace metadata, derives the shared framed identity, uses `SET NX PX`, and releases/renews only with owner-token-checked packaged Lua scripts. `with_dilock(handle, function(lease) ...)` uses `on.exit` for cleanup; unknown cleanup is an error after successful work, while known loss of ownership raises `dilock_lease_lost`; either condition is reported as a warning if the body itself fails. For Redis, a `NULL` try result means acknowledged contention after validating the key type; transport failures are errors/unknown outcomes.

The caller owns the Redis connection. Lua executes inside Redis; no infrastructure-side Lua service is installed. The package also contains an experimental native etcd gRPC adapter described below. Neither adapter stops protected code when a lease expires.

```r
provider <- dilock_provider(redis_connection, "default", "provisioned-epoch")
handle <- dilock_lock(provider, "camera/123/reindex")
with_dilock(handle, function(lease) rebuild_index())
```

The short path uses a 5-second wait and 30-second requested lease. To provide
the complete per-acquisition policy explicitly, pass both durations to
`with_dilock`. The adapter borrows the already-configured `redux` connection;
connection/TLS/timeouts are not configured by the Dilock provider.

```r
provider <- dilock_provider(redis_connection, namespace = "reports",
                            epoch = Sys.getenv("DILOCK_COORDINATION_EPOCH"))
daily <- dilock_lock(provider, "daily")
with_dilock(daily, function(lease) {
  # Run short protected work here.
}, wait_ms = 10000L, lease_ms = 60000L)
```

`dilock_renew` is an explicit owner-checked Redis operation, not automatic
renewal; lease expiry does not stop R code already running.


Executable Redis and etcd quickstarts, including both default and explicit
timing options, are in [`examples/`](examples/README.md). They use the real
SDK backends and require namespace metadata provisioned ahead of time; they do
not initialize backend metadata.

For the opt-in real-Redis test, configure `DILOCK_TEST_REDIS_HOST` and
`DILOCK_TEST_REDIS_PORT`, then run the package's `testthat` suite. The test
creates unique namespace metadata and lock keys and deletes them on exit; use
a disposable test database or server.

## Experimental etcd adapter

The etcd adapter uses native etcd v3 `Lock`, `Unlock`, `LeaseGrant`,
`LeaseKeepAlive`, `LeaseRevoke`, and `Range` RPCs. It does not use a custom
gateway. The provider owns its gRPC channel; each acquisition opens a separate
channel for its unique lease and native Lock waiter. Close the provider with
`on.exit`. Namespace metadata must already be provisioned by the operator at
`/dilock/namespaces/<namespace>/meta`; normal SDK use never creates or repairs
it. Metadata must select `etcd-native-lock-v1` and the deployment's stable
coordination epoch.

The etcd native `Lock` RPC queues requests and does not provide a
nonblocking try operation. `dilock_etcd_try_acquire()` therefore raises
`dilock_unsupported_capability` before making an RPC. Use
`dilock_etcd_acquire()` when a bounded wait is acceptable; its `wait_ms`
controls the queue wait, and timeout cleanup revokes the request's own lease.

```r
provider <- dilock_etcd_provider(
  target = "127.0.0.1:2379",
  namespace = "reports",
  epoch = Sys.getenv("DILOCK_COORDINATION_EPOCH")
)
on.exit(dilock_etcd_close(provider), add = TRUE)

daily <- dilock_etcd_lock(provider, "daily")
with_dilock_etcd(daily, function(lease) {
  message("fencing epoch/revision: ", lease$fence$epoch, "/", lease$fence$create_revision)
  rebuild_report()
}, wait_ms = 10000L, lease_ms = 60000L)
```

This adapter is experimental and does not imply production support. It pins
`rgrpc` 0.1.2 and `RProtoBuf` 0.4.28; source installation requires a usable
native gRPC C++ runtime and protobuf development toolchain. Runtime descriptors
load the packaged focused etcd v3.7.2 `.proto` definitions, with upstream
licenses included. Protocol Buffers are etcd v3's wire-message format:
`RProtoBuf` loads those descriptors, and `rgrpc` sends the messages over gRPC.
The package includes the `.proto` files; no Dilock service or script is
installed on the etcd server. For TLS, pass credentials supported by the
installed `rgrpc`, such as `rgrpc::grpc_tls(...)`; TLS configuration has not
been validated here. R execution is synchronous, so long protected work must call
`dilock_etcd_renew(lease)` explicitly and treat `FALSE` or an error as loss of
ownership. Acquisition and each RPC have bounded timeouts; one monotonic
cleanup budget is shared by `Unlock` and `LeaseRevoke`. Known loss of ownership
raises `dilock_lease_lost`, while cleanup uncertainty raises
`dilock_release_unknown`. If the body itself fails, cleanup warnings preserve
that error. `dilock_etcd_close` warns and
returns `"unknown"` if cleanup of a tracked lease is uncertain. Released leases
are removed from provider tracking so a long-lived provider does not retain
every historical gRPC stream and client. If any release returned `"unknown"`,
closing the provider preserves and reports that bounded uncertainty status.
The fencing value is the
coordination epoch plus observed key `create_revision`; downstream systems
must enforce it if stale work must be rejected.

`dilock_etcd_release()` linearly ranges the exact opaque lock key and checks
that its lease ID matches this acquisition before calling native `Unlock`.
`"released"` requires that ownership proof plus positive acknowledgements from
both `Unlock` and `LeaseRevoke`. A confirmed absent/mismatched key or an
acknowledged owner-specific revoke alone returns `"already_not_owned"`;
uncertain ownership or cleanup returns `"unknown"`. The Range, Unlock, and
LeaseRevoke calls share one monotonic cleanup budget, so an expiry between
observation and Unlock cannot be mistaken for an acknowledged release when
Revoke reports `NOT_FOUND`.

For opt-in etcd live tests, set `DILOCK_ETCD_ENDPOINT` to a disposable etcd
v3.7.2-compatible endpoint with pre-provisioned test metadata. Tests use unique
namespace/resource names and revoke their leases; they do not delete metadata or
broad key ranges.
