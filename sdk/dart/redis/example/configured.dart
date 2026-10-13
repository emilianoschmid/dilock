import 'package:dilock/dilock.dart';
import 'package:dilock_redis/dilock_redis.dart';

Future<void> main() async {
  const policy = AcquireOptions(
    wait: Duration(seconds: 3),
    lease: LeaseOptions(
      leaseDuration: Duration(seconds: 20),
      rpcTimeout: Duration(seconds: 1),
      cleanupTimeout: Duration(seconds: 1),
      autoRenew: true,
      requireFencing: false,
    ),
  );
  final provider = await RedisProvider.connect(
    'rediss://cache.example:6380/0',
    options: const RedisProviderOptions(
      namespace: 'billing',
      coordinationEpoch: 'billing-redis-incarnation-7',
      connectTimeout: Duration(seconds: 3),
      defaultAcquisition: policy,
      maxConnections: 12,
      cleanupObserver: _reportCleanup,
    ),
  );
  try {
    await provider.lock('invoice/8391').withLeaseOptions<void>(
      (lease) async {
        lease.ensureHeld();
        await persistIdempotently();
      },
      policy,
    );
  } finally {
    await provider.close();
  }
}

Future<void> persistIdempotently() async {}

void _reportCleanup(CleanupObservation observation) {
  print(
      '${observation.backend}: ${observation.outcome.name}: ${observation.reason}');
}
