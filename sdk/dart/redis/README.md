# Dilock Redis for Dart

This package provides the Redis single-authority draft 1 profile for trusted
Dart server and CLI applications. It is not intended for browser or untrusted
mobile clients. Redis deployments must use a protected, non-evicting keyspace
and coordinate one stable epoch with the protected resource.

```dart
import 'package:dilock/dilock.dart';
import 'package:dilock_redis/dilock_redis.dart';

Future<void> main() async {
  final provider = await RedisProvider.connect(
    'redis://127.0.0.1:6379/0',
    options: const RedisProviderOptions(coordinationEpoch: 'dev-1'),
  );
  try {
    final daily = provider.lock('reports/daily');
    await daily.withLease<void>((lease) async {
      lease.ensureHeld();
      await runShortJob();
    });
  } finally {
    await provider.close();
  }
}

Future<void> runShortJob() async {}
```

This is the default mode: `default` namespace, 5-second wait, 30-second lease,
2-second RPC and cleanup budgets, and no background renewal. The example is
also available as `dart run example/minimal.dart` from this package directory.

For an explicitly configured provider and complete per-acquisition policy, see
[`example/configured.dart`](example/configured.dart). It sets connection
timeout, pool size, cleanup observer, wait and lease budgets, and uses
`withLeaseOptions` so owner-checked release is awaited on every exit path:

```sh
dart run example/configured.dart
```

The endpoint and epoch in that example are illustrative. Run it only after
provisioning matching namespace metadata in a disposable Redis authority.

The namespace defaults to `default`; its metadata must be provisioned before
the SDK connects. Metadata is checked during initialization and never created
or repaired by runtime code. The `rediss://` scheme uses normal TLS certificate
verification. Credentials can be supplied through URL user info; do not log
the URL. The provider owns its bounded connection pool and active leases.
Handles share that provider and do not create a connection on construction.

`withLease` acquires with the provider's finite 5-second wait and default
30-second lease (2-second RPC and cleanup budgets, auto-renew off). Its `finally`
path awaits owner-checked cleanup and observes `Unknown` through the optional
cleanup callback without replacing a body exception, even if cleanup or the
observer throws. Cleanup errors after successful work are surfaced to the
caller. `withLeaseOptions` takes
a complete `AcquireOptions` value; it does not silently merge omitted fields.
`tryAcquire` returns null only after Redis acknowledged `SET NX` contention.
Backend errors, cancellation after dispatch and missing acknowledgements are
typed failures. Dart Futures do not support preemptive cancellation; a
`CancellationToken` closes a dispatched socket and triggers tracked owner-only
cleanup, but the remote command may already have committed. If cancellation races
with a positive `SET NX` acknowledgement, acquisition is reported as unknown
and tracked owner-only cleanup runs; cancellation after acknowledged contention
remains a known cancellation.

The configured cleanup observer also receives `unknown` when tracked
owner-specific cleanup after an ambiguous acquisition, cancellation, late
acknowledgement, renewal loss, or conservative lease expiry is not confirmed.
The callback is best-effort and cannot turn an uncertain outcome into success.

`RedisLease.release()` is the acknowledgement-sensitive operation and returns
`released`, `alreadyNotOwned`, or `unknown`. `autoRenew` compares the full owner
value in a bundled Lua script; no Lua service or manual script upload is
required. Unknown/lost local state cannot stop a paused process. Owner values
prevent stale cleanup, but they are not fencing tokens; Redis draft 1 does not
promise a strong monotonic fence through failover.

The implementation currently speaks RESP2 over `dart:io` sockets and uses a
small provider-owned connection pool without client-side command retries. ACL
authentication, logical database selection, standalone URLs and TLS URLs are
supported. Cluster/Sentinel discovery and custom certificate bypass are not.
