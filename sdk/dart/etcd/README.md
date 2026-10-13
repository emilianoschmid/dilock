# Dilock etcd for Dart

This package implements the native etcd v3 `Lease`, `Lock`, and `KV.Range`
gRPC profile. It uses one unique etcd lease per acquisition, keeps queued
waiters alive, verifies the exact opaque Lock key with a linearizable Range,
and exposes `(coordination epoch, create revision)` as an etcd fence.

The protocol inputs are the focused etcd v3.7.2 proto definitions under
`proto/etcd-v3.7.2`. They retain the upstream module licenses and source
references. Generated gRPC classes are included in `lib/src/generated/`.
Regenerate them after resolving the declared `grpc`, `protobuf`, and
`protoc_plugin` dependencies by running `tool/generate_protos.ps1`; the pinned
local `protoc` is 31.1.

```dart
import 'package:dilock/dilock.dart';
import 'package:dilock_etcd/dilock_etcd.dart';

Future<void> main() async {
  final provider = await EtcdProvider.connect(
    'http://127.0.0.1:2379',
    options: const EtcdProviderOptions(coordinationEpoch: 'deployment-2026-01'),
  );
  try {
    final reports = provider.lock('reports/daily');
    await reports.withLease<void>((lease) async {
      lease.ensureHeld();
      // Protect one bounded unit of work. The lease does not stop stale code.
    });
  } finally {
    await provider.close();
  }
}
```

This default mode uses a 5-second acquisition wait, 30-second requested lease,
and 2-second RPC and cleanup budgets. Run the checked-in consumer from this
package directory with `dart run example/minimal.dart` after an operator has
provisioned the matching etcd namespace metadata.

For full provider and acquisition settings, use
[`example/configured.dart`](example/configured.dart) and run
`dart run example/configured.dart` from this package directory. That example
sets the namespace, epoch, RPC and cleanup budgets, wait and lease durations,
renewal, fencing requirement and cleanup observer. It uses the scoped helper
so release is awaited on normal and exceptional exits.

`withLease` uses the provider's complete default policy. For explicit timing,
pass a complete `AcquireOptions` through `withLeaseOptions`. `autoRenew` defaults
to false after ownership is confirmed, but keepalive remains active while a
native Lock call is queued. Call `renew()` or set `autoRenew: true` for longer
work. Closing the provider cancels in-flight calls, releases active leases
within cleanup budgets, and closes its owned gRPC channel.

Scope cleanup catches transport and client exceptions, attempts both exact-key
Unlock and unique-lease revoke within one shared budget, and reports an
unconfirmed outcome through the configured cleanup observer. A cleanup error
cannot replace an exception from scoped work; after successful work, cleanup
errors remain visible. An acknowledged revoke is terminal cleanup, but an
`alreadyNotOwned` result still reports lease loss because cleanup cannot prove
that ownership held throughout the callback. It never treats a thrown revoke
as acknowledged release.

Cancellation after dispatch is reported as an unknown acquisition outcome
unless lease revocation is confirmed. A finite Lock deadline is ordinary
contention only for `tryAcquire` when a Range confirms a competing native lock;
other transport errors stay errors. The fence is useful only if the protected
resource atomically rejects stale `(epoch, revision)` values. It does not stop
an old process. etcd's linearizable revision provides fencing only within the
operator-approved coordination epoch; restoring an older snapshot requires a
new epoch.

The acquisition wait budget includes lease grant, keepalive readiness, Lock,
and exact-key Range verification. A late positive Range or cancellation after
dispatch never returns a lease: the SDK attempts unique-lease cleanup and
reports timeout/cancellation only when cleanup is acknowledged; otherwise the
outcome remains unknown.

The adapter, proto inputs, generator script, and checked-in generated stubs are
included. Run `dart analyze`, `dart format`, and `dart test` before using a
build. This package remains experimental; backend behavior, cross-language
conformance, and installed-package consumption must be verified for each
release profile.
