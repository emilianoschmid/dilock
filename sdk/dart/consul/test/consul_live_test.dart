import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_consul/dilock_consul.dart';
import 'package:test/test.dart';

void main() {
  test('Consul contention, renewal and monotonic LockIndex lifecycle', () async {
    final endpoint = Platform.environment['DILOCK_CONSUL_ENDPOINT'];
    final namespace = Platform.environment['DILOCK_CONSUL_NAMESPACE'];
    final epoch = Platform.environment['DILOCK_CONSUL_EPOCH'];
    if (endpoint == null || namespace == null || epoch == null) {
      markTestSkipped('set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH');
      return;
    }

    final options = ConsulProviderOptions(
      namespace: namespace,
      coordinationEpoch: epoch,
      token: Platform.environment['CONSUL_HTTP_TOKEN'],
    );
    await ConsulProvider.provisionNamespace(endpoint, options: options);
    final provider = await ConsulProvider.connect(endpoint, options: options);
    try {
      final lock = provider.lock('dart/live/${DateTime.now().microsecondsSinceEpoch}');
      const policy = AcquireOptions(
        wait: Duration(seconds: 1),
        lease: LeaseOptions(
          leaseDuration: Duration(seconds: 10),
          requireFencing: true,
        ),
      );
      final first = await lock.tryAcquire(options: policy);
      expect(first, isNotNull);
      expect(first!.fence.coordinationEpoch, epoch);
      expect(first.fence.lockIndex, greaterThan(0));
      expect(await lock.tryAcquire(options: policy), isNull);
      expect(await first.renew(), isTrue);
      expect(await first.release(), ReleaseOutcome.released);

      final next = await lock.tryAcquire(options: policy);
      expect(next, isNotNull);
      expect(next!.fence.lockIndex, greaterThan(first.fence.lockIndex));
      expect(await next.release(), ReleaseOutcome.released);
    } finally {
      await provider.close();
    }
  });
}
