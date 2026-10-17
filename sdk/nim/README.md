# Dilock Nim binding

This is a thin Nim binding to the local C ABI over the existing C++ Redis,
etcd and Consul engines. It is a source candidate, not a standalone protocol
implementation. C ABI version 1 is still an internal candidate, so this
binding is experimental and must not be treated as a stable Nimble package.

The quick path uses the C defaults (5 seconds of wait and a 30 second requested
lease) while requiring the deployment's namespace and epoch:

```nim
let provider = connectRedis("127.0.0.1", "billing", "local-redis-epoch")
defer: provider.destroy()
let lock = provider.lock("invoice/42/reconcile")
defer: lock.destroy()
withLease lease, lock.acquireDefault():
  if not lease.isValid():
    raise newException(IOError, "Redis lease is no longer valid")
```

Consul uses an HTTP agent endpoint, explicit metadata provisioning, and an
optional ACL token. Its lease TTL defaults to 30 seconds. The token is read
from deployment configuration and never belongs in source control:

```nim
import std/os

let endpoint = getEnv("DILOCK_CONSUL_ENDPOINT", "http://127.0.0.1:8500")
let provider = connectConsul(endpoint, "billing", "local-consul-epoch",
                             aclToken = getEnv("DILOCK_CONSUL_TOKEN"))
defer: provider.destroy()
let lock = provider.lock("invoice/42/reconcile")
defer: lock.destroy()
withLease lease, lock.acquireDefault():
  let fence = lease.fence()
  echo fence.epoch, ":", fence.lockIndex
  if lease.release() != ReleaseOutcome.released:
    raise newException(IOError, "Consul release was not confirmed")
```

Provision matching metadata before connecting with
`provisionConsulNamespace(endpoint, namespace, epoch, aclToken)`, or use a
separate deployment tool. The API uses KV CAS=0 and verifies an existing value;
normal `connectConsul` never provisions or repairs metadata. Every attempt owns
a separate Consul session. Retain lock KV keys because deleting a key can reset
its `LockIndex` fence. HTTPS uses system certificate verification.

Use `lock.acquire(waitMs, leaseMs)` for a per-attempt policy. `defer` / the
`withLease` template invoke C handle destruction on normal and exceptional
exits. Explicit `release` is required when its acknowledgement matters; destroy
is bounded best-effort cleanup. The quick path above relies on scoped cleanup;
call `release()` and inspect its outcome when the application needs the
acknowledgement. The wrappers use Nim `ref object` handles, so
aliases share one wrapper and destruction is idempotent. A lock lease is not a
local mutex and expiry cannot stop a paused process.

Passing a nil or destroyed provider, lock, lease, or cancellation handle to an
operation raises `DilockError` with `dsInvalidArgument` before entering the C
ABI. Destroying a nil handle is a no-op. These checks do not make concurrent
mutation/destruction of the same wrapper safe; callers must serialize its
lifecycle.

Pass a `Cancellation` handle as the second argument to `lock.acquireDefault`
to use the provider defaults with cancellation. The explicit-duration form is
`lock.acquire(waitMs, leaseMs, cancellation)`. Cancellation applies to waiting
for acquisition; it does not stop work after a lease has been returned.

Redis `tryAcquire` returns `Option.none` only for ordinary contention. The etcd
native Lock `tryAcquire` returns a typed unsupported-capability exception. etcd
fences expose the configured epoch and verified `create_revision`; the
protected resource must atomically reject stale values. Redis does not provide
strong fencing. `lease.renew(leaseMs)` is available for Redis: it returns true
when the owner token was renewed and raises `DilockError` for lost ownership or
backend uncertainty. The C ABI has no synchronous etcd lease-renewal function.
It does support provider-level automatic etcd keepalive after acquisition;
enable that behavior with `connectEtcd(..., autoRenew = true)`. Keepalive also
runs while waiting for the native Lock RPC. The default remains `false`, so an
acquired etcd lease expires at its server-granted TTL unless it is released
first. Do not interpret automatic keepalive as a fencing guarantee or as a
manual `lease.renew` acknowledgement.

The full policy currently configurable through Nim is the acquisition wait and
requested lease duration. Redis connection configuration accepts an optional
username, password and database. Etcd accepts a raw gRPC target, namespace,
expected epoch and optional TLS PEM values. Consul accepts an agent URL,
namespace, expected epoch and optional ACL token. TTLs must be 10 seconds to 24
hours; the default lease remains 30 seconds. The TLS values are PEM contents,
not filenames; load them from the deployment's secret/configuration provider
and avoid logging them. This facade still does not expose Redis TLS.

```nim
let provider = connectRedis("redis.internal", "reports", "prod-east-1", port = 6380)
defer: provider.destroy()
let daily = provider.lock("daily")
defer: daily.destroy()

withLease lease, daily.acquire(10_000, 60_000):
  if not lease.isValid():
    raise newException(IOError, "Redis lease is no longer valid")
  let outcome = lease.release()
  if outcome != ReleaseOutcome.released:
    raise newException(IOError, "Redis release was not confirmed as released")
```

For an authenticated Redis deployment or a non-default database, pass the
credentials and database explicitly. These strings are sent to the local C ABI
to configure its Redis client; do not log or embed production secrets.

```nim
import std/os

let provider = connectRedis(
  "redis.internal",
  "billing",
  "billing-redis-incarnation-7",
  port = 6380,
  username = "dilock",
  password = getEnv("DILOCK_REDIS_PASSWORD"),
  database = 4,
)
defer: provider.destroy()
```

The opt-in authenticated-Redis integration regression is
`tests/test_redis_auth_database.nim`. It requires a disposable authenticated
Redis and Dilock namespace metadata provisioned in the selected database. Set
`DILOCK_TEST_REDIS_PASSWORD`, and optionally `DILOCK_TEST_REDIS_USERNAME`,
`DILOCK_TEST_REDIS_DATABASE`, `DILOCK_REDIS_HOST`, and
`DILOCK_REDIS_PORT` before compiling/running that test.

For etcd, the same explicit durations are accepted by its native Lock path.
The C ABI etcd endpoint is `host:port`; etcd try-acquire is unsupported.

```nim
let provider = connectEtcd("etcd.internal:2379", "reports", "prod-east-1")
defer: provider.destroy()
let daily = provider.lock("daily")
defer: daily.destroy()

withLease lease, daily.acquire(10_000, 60_000):
  if not lease.isValid():
    raise newException(IOError, "etcd lease is no longer valid")
  let fence = lease.fence()
  echo "Fence: ", fence.epoch, ":", fence.createRevision
  let outcome = lease.release()
  if outcome != ReleaseOutcome.released:
    raise newException(IOError, "etcd release was not confirmed as released")
```

When work may outlast the initial etcd TTL, opt into automatic keepalive at
provider creation. This setting applies to leases acquired through that
provider; callers still need to check lease validity and enforce the etcd
fence atomically at the protected resource.

```nim
let provider = connectEtcd(
  "etcd.internal:2379", "reports", "prod-east-1", autoRenew = true,
  tlsRootCertificates = getEnv("DILOCK_ETCD_ROOTS_PEM"),
  tlsClientCertificate = getEnv("DILOCK_ETCD_CLIENT_CERT_PEM"),
  tlsClientPrivateKey = getEnv("DILOCK_ETCD_CLIENT_KEY_PEM"))
defer: provider.destroy()
let daily = provider.lock("daily")
defer: daily.destroy()

withLease lease, daily.acquire(10_000, 3_000):
  if not lease.isValid():
    raise newException(IOError, "etcd lease is no longer valid")
  # The C ABI keeps this lease alive automatically while this scope runs.
```

The C ABI etcd endpoint is a raw gRPC target such as `127.0.0.1:2379`; do not
prefix it with `http://`.

## Build and test

Build and install the combined C ABI with both backend profiles and
`BUILD_SHARED_LIBS=ON` using [`sdk/c/README.md`](../c/README.md). Install it
under `.local/dilock-c` or adjust the Nimble task's header and library paths.
Then run `nimble test` from this directory. The examples require explicitly
provisioned Redis/etcd metadata and should be run only against authorized local
test authorities. `DILOCK_REDIS_HOST` and `DILOCK_ETCD_ENDPOINT` override the
example connection targets; both default to loopback.

The Consul live consumer is `tests/test_live_consul.nim`; it expects a
pre-provisioned disposable namespace and validates contention, renewal, release,
handoff, and fencing:

```sh
nim c -r --path:src --passC:-I<abi-prefix>/include \
  --passL:-L<abi-prefix>/lib --passL:-ldilock_c tests/test_live_consul.nim
```

The C ABI and this facade are experimental and are not presented as a
production-ready package.

The opt-in Redis renewal integration test is `tests/test_redis_renewal.nim`.
Run it only against a disposable Redis whose `nim-renewal` namespace metadata
has been provisioned with the matching epoch. Compile it with the same C ABI
include/library flags as the unit test; set `DILOCK_REDIS_HOST`,
`DILOCK_REDIS_PORT`, `DILOCK_TEST_NAMESPACE`, and `DILOCK_TEST_EPOCH` for the
test authority. It sleeps beyond the initial lease period to verify renewal
with an independent provider.

The opt-in etcd automatic-keepalive integration test is
`tests/test_etcd_auto_renew.nim`. It uses two providers against a disposable
etcd authority, waits beyond the initial server TTL, verifies that a successor
still times out, then checks release and handoff. Set `DILOCK_ETCD_ENDPOINT`,
`DILOCK_TEST_NAMESPACE`, and `DILOCK_TEST_EPOCH` to provisioned test values.
