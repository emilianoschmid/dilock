# Dilock Zig binding

This is an experimental Zig binding over the repository's C ABI facade. Redis,
etcd and Consul operations are performed by the existing C++ engines through
`Dilock::c`; this package adds C import, typed Zig error sets and explicit
`defer`-based handle destruction. The C ABI is an internal version-1 candidate,
not a release-stable interface. The Zig binding is therefore not an
independently stable or publishable SDK.

The simple Redis example uses the C API defaults: 5 seconds of wait and a 30
second requested lease. An endpoint, namespace and operator-approved
coordination epoch remain explicit:

```zig
var provider = try dilock.RedisProvider.connect("127.0.0.1", 6379, "billing", "local-redis-epoch");
defer provider.deinit();
var lock = try provider.lock("invoice/42/reconcile");
defer lock.deinit();
var lease = try lock.acquireDefault();
defer lease.deinit();
try lease.release();
```

Call `release` when the acknowledgement matters. `deinit` delegates to the C
lease destroy function, which performs only its documented bounded best-effort
cleanup. A lease is not a local mutex; expiry cannot stop a paused process.
Redis `tryAcquire` returns `null` only for ordinary contention; backend errors
remain typed errors. The etcd native Lock profile rejects try-acquire with
`UnsupportedCapability`, because it has no safe non-blocking operation. etcd
fencing contains the namespace epoch and verified `create_revision`; a protected
resource must atomically reject stale fences. Fence epoch storage is allocated
with the caller-provided allocator and released with `EtcdFence.deinit`;
deployment epoch length is not capped by the Zig wrapper. Redis has no strong
fencing. `EtcdLease.effectiveTtlMs()` reports the server-granted TTL in
milliseconds; etcd rounds requested TTLs to whole seconds.

For a complete per-acquisition policy, call the explicit overload. For
provider-level configuration, use `connectWithOptions`; it exposes Redis
authentication/database and timing settings, and etcd TLS PEM plus timing and
auto-renew settings. Durations are integer milliseconds at the C ABI boundary.

```zig
const redis_options = dilock.RedisProviderOptions{
    .host = "redis.internal",
    .port = 6380,
    .username = "billing-service",
    .password = "", // Load credentials from the deployment's secret manager when needed.
    .database = 4,
    .namespace = "billing",
    .coordination_epoch = "prod-east-1",
    .rpc_timeout_ms = 800,
    .default_wait_ms = 10_000,
    .default_lease_ms = 60_000,
};
var provider = try dilock.RedisProvider.connectWithOptions(redis_options);
defer provider.deinit();
```

The existing positional `connect` overload remains the short form for common
defaults. TLS option values are PEM contents, not file paths; keep credentials
in the deployment secret manager.

```zig
var provider = try dilock.RedisProvider.connect("127.0.0.1", 6379, "reports", "prod-east-1");
defer provider.deinit();
var daily = try provider.lock("daily");
defer daily.deinit();
var lease = try daily.acquire(10_000, 60_000); // 10 s wait, 60 s requested lease.
defer lease.deinit();
// Run a short protected operation here.
const outcome = try lease.release();
if (outcome != .released) return error.ReleaseNotConfirmed;
```

The same per-attempt overload is available for etcd. Its native `Lock` path
does not provide non-blocking try-acquire; etcd fencing must be checked
atomically by the protected resource.

Consul uses `ConsulProvider.connectWithOptions` for endpoint, ACL token and
timing settings. Provision the namespace metadata explicitly with
`provisionConsulNamespace` before workers start. The ACL token is passed as
NUL-terminated text and should come from the deployment secret manager. Consul
fencing returns the provisioned epoch and KV `LockIndex`; protected writes must
compare that fence atomically. Keep lock KV entries in place after release so
`LockIndex` cannot reset. `ConsulLock.tryAcquire` returns `null` only for busy
contention, and `ConsulLease.renew()` returns false when ownership is no longer
confirmed.

For default wait and lease durations with cancellation, use
`acquireDefaultCancellable(&cancellation)` on either lock type. The explicit
duration overload remains available as `acquireCancellable(waitMs, leaseMs,
&cancellation)`. Cancellation stops the acquisition attempt according to the C
ABI contract; it does not cancel work after a lease has been returned.

```zig
var provider = try dilock.EtcdProvider.connect("127.0.0.1:2379", "reports", "prod-east-1");
defer provider.deinit();
var daily = try provider.lock("daily");
defer daily.deinit();
var lease = try daily.acquire(10_000, 60_000);
defer lease.deinit();
var fence = try lease.fence(std.heap.page_allocator);
defer fence.deinit();
// Pass the fence to the protected resource and compare it atomically there.
const outcome = try lease.release();
if (outcome != .released) return error.ReleaseNotConfirmed;
```

This binding checks the linked ABI version at provider/cancellation creation
and maps C status values to Zig errors. Calling `Cancellation.request()` after
`deinit()` returns `error.InvalidArgument` rather than silently ignoring the
request. Do not copy an owning Zig value: providers, locks, leases and
cancellation tokens own C handles, while `EtcdFence` owns allocator storage.
Call `deinit()` exactly once for each sole owner.

## Build and test

First build/install the CMake SDK with Redis, etcd and Consul enabled,
(`DILOCK_BUILD_ETCD=ON`, `DILOCK_BUILD_CONSUL=ON`),
`BUILD_SHARED_LIBS=ON` and
`DILOCK_BUILD_C_API=ON` as described in [`../c/README.md`](../c/README.md). A
shared C ABI carries the selected platform's C++ runtime dependencies. Then,
with Zig 0.17.0, point
`zig build` at the installed header and library directories using
`-Dc-include=<prefix>/include -Dc-lib=<prefix>/lib`. Run `zig build test` and
`zig build` compiles all three examples as installed artifacts (it does not run them
against a backend). The ABI currently exports all enabled backend profiles in
one library, so this binding has a
combined native dependency and inherits that engine's platform requirements.

The build pins Zig 0.17.0 in `build.zig.zon`. Run `zig build test` with the
pinned toolchain to execute the package tests.

The PostgreSQL route has an isolated test root so it can link against the
PostgreSQL-only C ABI without requiring unrelated etcd or Consul symbols:

```sh
zig build test-postgres \
  -Dc-include=/path/to/cabi/include \
  -Dc-lib=/path/to/cabi/lib
```

Set `DILOCK_POSTGRES_DSN` to run the live contention, liveness, release and
handoff assertion against a disposable PostgreSQL database. This remains an
experimental draft C ABI route.
