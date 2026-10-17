# Dilock Consul for Dart

This optional adapter uses Consul's HTTP KV and session APIs through Dart's
`dart:io` client. It creates a separate TTL session for each acquisition,
confirms the owner with a consistent KV read, and returns `(coordination epoch,
LockIndex)` as the fence. Session TTLs are 10 seconds through 24 hours and are
rounded upward to whole seconds.

Provision metadata explicitly before opening the provider. Provisioning uses
KV `cas=0`; an existing entry must match exactly. Retain every
`dilock/locks/<resource-id>` key: deleting one can reset its LockIndex.

```dart
import 'package:dilock_consul/dilock_consul.dart';

Future<void> main() async {
  const options = ConsulProviderOptions(
    namespace: 'orders',
    coordinationEpoch: 'deployment-2026-01',
  );
  await ConsulProvider.provisionNamespace(
    'http://127.0.0.1:8500',
    options: options,
  );
  final provider = await ConsulProvider.connect(
    'http://127.0.0.1:8500',
    options: options,
  );
  try {
    final lock = provider.lock('invoice/123');
    final lease = await lock.acquire(
      options: const AcquireOptions(
        wait: Duration(seconds: 5),
        lease: LeaseOptions(leaseDuration: Duration(seconds: 10)),
      ),
    );
    try {
      print('enforce fence atomically: ${lease.fence.lockIndex}');
    } finally {
      await lease.release();
    }
  } finally {
    await provider.close();
  }
}
```

The package's local HTTP contract test exercises metadata provisioning,
acquisition, owner verification, fence extraction and release without a Consul
daemon. A disposable real Consul agent is still required for live behavior and
cross-language contention evidence. No package has been published.
