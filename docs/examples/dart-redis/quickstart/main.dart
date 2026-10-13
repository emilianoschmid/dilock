import 'package:dilock_redis/dilock_redis.dart';
import 'package:dilock/dilock.dart';

Future<void> main() async {
  final provider = await RedisProvider.connect(
    'redis://127.0.0.1:6379/0',
    options: const RedisProviderOptions(coordinationEpoch: 'local-redis-epoch'),
  );
  try {
    // Two resource handles reuse this provider's existing connections.
    await provider.lock('reports/daily').withLease<void>((lease) async {
      lease.ensureHeld();
      await runShortJob();
    });
    const explicit = AcquireOptions(
      wait: Duration(seconds: 3),
      lease: LeaseOptions(
        leaseDuration: Duration(seconds: 20),
        rpcTimeout: Duration(seconds: 1),
        cleanupTimeout: Duration(seconds: 1),
      ),
    );
    await provider.lock('reports/weekly').withLeaseOptions<void>((lease) async {
      lease.ensureHeld();
      await runShortJob();
    }, explicit);
  } finally {
    await provider.close();
  }
}

Future<void> runShortJob() async {}
