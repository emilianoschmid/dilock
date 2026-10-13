import 'package:dilock_redis/dilock_redis.dart';

Future<void> main() async {
  final provider = await RedisProvider.connect(
    'redis://127.0.0.1:6379/0',
    options: const RedisProviderOptions(coordinationEpoch: 'local-redis-epoch'),
  );
  try {
    await provider.lock('reports/daily').withLease<void>((lease) async {
      lease.ensureHeld();
      await runShortJob();
    });
  } finally {
    await provider.close();
  }
}

Future<void> runShortJob() async {}
