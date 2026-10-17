# Dilock D binding

This D source candidate binds the repository's C ABI over the existing C++
Redis, etcd and Consul engines. It adds typed D status exceptions, non-copyable handle
wrappers and `scope(exit)` examples; it contains no duplicate backend or lock
protocol. The C ABI is an internal version-1 candidate rather than a
release-stable contract. This D binding is research-stage and not publishable
as a stable DUB package.

The quick example uses C defaults (5 seconds of wait, 30 seconds of requested
lease) and keeps namespace/epoch configuration explicit:

```d
auto provider = RedisProvider.connect("127.0.0.1", "billing", "redis-epoch-1");
scope(exit) provider.destroy();
auto lock = provider.lock("invoice/42/reconcile");
scope(exit) lock.destroy();
auto lease = lock.acquireDefault();
scope(exit) lease.destroy();
```

Consul uses an HTTP agent URL and optional ACL token. Provision matching
metadata separately with `provisionConsulNamespace(options)` before connecting;
normal provider construction only validates it. The example
[`consul_quickstart.d`](examples/consul_quickstart.d) reads a verified fence
from the acquired lease. HTTPS uses the system trust store, and the Consul lock
KV entry must be retained so its `LockIndex` cannot reset.

The etcd endpoint is the raw gRPC target such as `127.0.0.1:2379`; do not
prefix it with `http://`. The Redis and etcd quickstarts accept endpoint
overrides through `DILOCK_REDIS_HOST` and `DILOCK_ETCD_ENDPOINT`; their defaults
are `127.0.0.1` and `127.0.0.1:2379`. Provision the namespace metadata before
creating a provider. The examples use namespace `billing` and coordination
epochs `local-redis-epoch` and `local-etcd-epoch`, respectively; provision
matching metadata before running them.
provider construction only validates existing metadata and never creates or
repairs it.

Use `acquire(waitMs, leaseMs)` for a per-attempt policy. Call `release` when
the acknowledgement matters; `destroy` performs bounded best-effort cleanup.
Redis leases also expose `renew(leaseMs)`: it returns `true` only when the
backend confirms the current acquisition was extended, and `false` when that
acquisition is no longer owned. etcd's native lease renewal remains managed by
the provider's keepalive policy rather than a per-lease renew method.
`EtcdLease.effectiveTtlMs()` reports the server-granted TTL in milliseconds;
etcd rounds requested TTLs to whole seconds.
Wrapper structs disable copying; keep exactly one owner and destroy it once.
Redis `tryAcquire(leaseMs, lease)` returns `false` only for ordinary contention
and writes the acquired lease to an empty `ref` argument on success. An empty
lease is required so an existing native acquisition cannot be overwritten and
leaked; a non-empty output raises `invalidArgument` without changing it. This
keeps the move-only lease value on the caller's stack without an extra heap
allocation; the caller must still call `destroy`, normally with `scope(exit)`.
Backend and protocol failures remain typed exceptions. etcd native Lock
try-acquire is explicitly unsupported. etcd fences carry epoch and verified
`create_revision`, which the protected resource must atomically enforce. Redis
does not provide strong fencing. A lease does not stop a paused process after
expiry.

Destroy cancellation handles after their acquisition has completed. Calling
`Cancellation.request()` on a default-initialized or destroyed handle raises a
typed `invalidArgument` error instead of silently dropping the request.
Both lock types also support `acquireDefault(cancellation)` when the configured
provider wait and lease durations should be used with cancellation. All
cancellation overloads reject a default-initialized or destroyed cancellation
handle; the C ABI treats a null cancellation pointer as an uncancellable call.
Cancellation handles are non-copyable and these overloads take them by `ref`;
pass a live local variable and keep it alive until the synchronous acquire call
returns.

The executable [`redis_try_quickstart.d`](examples/redis_try_quickstart.d)
shows the immediate-acquisition form while reusing one provider and one lock
handle. A false result is ordinary contention; exceptions still represent
backend, protocol, or cancellation failures.

`RedisProviderOptions`, `EtcdProviderOptions` and `ConsulProviderOptions` expose the provider settings
available in the C ABI while preserving its defaults. Durations use
`core.time.Duration` and are converted to whole milliseconds. Redis options
include username/password, database, RPC and cleanup timeouts, safety margin,
poll interval, and default wait/lease durations. etcd options include RPC and
cleanup timeouts, safety margin, default wait/lease durations, auto-renew, and
optional TLS root/client certificate and private-key PEM contents. TLS strings
are PEM contents, not file paths. A client certificate and its private key
must be provided together; a custom root certificate can be used on its own.
Keep etcd auto-renew disabled unless the application explicitly wants
keepalive to continue after acquisition.

Consul options include agent endpoint, namespace, expected epoch, optional ACL
token, RPC and cleanup timeouts, poll interval, and default wait/lease durations.
TTL requests must be between 10 seconds and 24 hours. The returned fence is
`(epoch, LockIndex)` and must be enforced atomically by the protected resource.

Keep the short `connect(host, namespace, epoch)` overload for common defaults.
Use the options overload when deployment configuration needs explicit
transport or timing settings:

```d
import core.time : dur;

auto redisOptions = RedisProviderOptions.init;
redisOptions.host = "redis.internal";
redisOptions.port = 6380;
redisOptions.username = "billing-service";
redisOptions.password = passwordFromSecretStore;
redisOptions.database = 4;
redisOptions.nameSpace = "billing";
redisOptions.coordinationEpoch = "prod-east-1";
redisOptions.rpcTimeout = dur!"msecs"(800);
redisOptions.cleanupTimeout = dur!"msecs"(700);
redisOptions.defaultWait = dur!"seconds"(10);
redisOptions.defaultLease = dur!"seconds"(60);
auto redisProvider = RedisProvider.connect(redisOptions);
scope(exit) redisProvider.destroy();

auto etcdOptions = EtcdProviderOptions.init;
etcdOptions.endpoint = "etcd.internal:2379";
etcdOptions.nameSpace = "billing";
etcdOptions.coordinationEpoch = "prod-east-1";
etcdOptions.tlsRootCertificates = rootCertificatesPem;
etcdOptions.tlsClientCertificate = clientCertificatePem;
etcdOptions.tlsClientPrivateKey = clientPrivateKeyPem;
etcdOptions.autoRenew = true;
auto etcdProvider = EtcdProvider.connect(etcdOptions);
scope(exit) etcdProvider.destroy();
```

Store credentials and private-key PEM values in the deployment's secret
manager; do not commit them into source or configuration examples. Embedded
U+0000 is rejected before any string reaches the C ABI. Provider construction
still validates existing namespace metadata only; it never provisions or
repairs it.

```d
auto provider = RedisProvider.connect("redis.internal", "reports", "prod-east-1", 6380);
scope(exit) provider.destroy();
auto daily = provider.lock("daily");
scope(exit) daily.destroy();
auto lease = daily.acquire(10_000, 60_000);
scope(exit) lease.destroy();

enforce(lease.isValid(), "lease expired before protected work");
const outcome = lease.release();
enforce(outcome == ReleaseOutcome.released, "Redis release was not confirmed as released");
```

For etcd, use the native Lock path and pass a raw `host:port` target. The
returned fence must be atomically checked by the protected resource.

```d
auto provider = EtcdProvider.connect("etcd.internal:2379", "reports", "prod-east-1");
scope(exit) provider.destroy();
auto daily = provider.lock("daily");
scope(exit) daily.destroy();
auto lease = daily.acquire(10_000, 60_000);
scope(exit) lease.destroy();
const fence = lease.fence();
// Pass this fence to the protected resource and compare it atomically there.
const outcome = lease.release();
enforce(outcome == ReleaseOutcome.released, "etcd release was not confirmed as released");
```

## Build and test

Build/install both C++ backend profiles and a shared C ABI as described in
[`sdk/c/README.md`](../c/README.md), then configure DUB's library search path to
the resulting `dilock_c` library. DMD is the reference compiler; the module also needs a C++ linker runtime
and the transitive native dependencies of the C++ package. Run `dub test`, then
build/run `examples/redis_quickstart.d` and `examples/etcd_quickstart.d` against
explicitly provisioned local authorities.

The opt-in Consul lifecycle consumer is `tests/consul_live.d`. After provisioning
the namespace metadata, compile it against the installed C ABI and run it with
the same endpoint, namespace, and epoch:

```sh
ldc2 -Isource -L-L"$DILOCK_C_LIB_DIR" \
  -L-Wl,-rpath-link,"$DILOCK_C_LIB_DIR" -L-ldilock_c \
  source/dilock.d tests/consul_live.d -of=/tmp/dilock-d-consul-live
DILOCK_CONSUL_ENDPOINT=http://127.0.0.1:8500 \
DILOCK_CONSUL_NAMESPACE=consul-validation DILOCK_CONSUL_EPOCH=local-validation-1 \
LD_LIBRARY_PATH="$DILOCK_C_LIB_DIR:${LD_LIBRARY_PATH:-}" /tmp/dilock-d-consul-live
```
