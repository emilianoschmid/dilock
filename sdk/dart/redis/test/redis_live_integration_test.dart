import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:dilock/dilock.dart';
import 'package:dilock_redis/dilock_redis.dart';
import 'package:test/test.dart';

const _rpcTimeout = Duration(milliseconds: 400);
const _cleanupTimeout = Duration(milliseconds: 400);

void main() {
  final endpoint = _nonEmptyEnvironment('DILOCK_TEST_REDIS_ENDPOINT');
  final namespace = _nonEmptyEnvironment('DILOCK_TEST_REDIS_NAMESPACE');
  final epoch = _nonEmptyEnvironment('DILOCK_TEST_REDIS_EPOCH');
  final missingConfiguration =
      endpoint == null || namespace == null || epoch == null;

  test(
    'two independent providers interoperate with live Redis lifecycle operations',
    () async {
      final suffix = '${DateTime.now().microsecondsSinceEpoch}-'
          '${Random.secure().nextInt(1 << 32)}';
      final options = RedisProviderOptions(
        namespace: namespace!,
        coordinationEpoch: epoch!,
        maxConnections: 2,
      );
      final firstProvider =
          await RedisProvider.connect(endpoint!, options: options);
      RedisProvider? secondProvider;
      try {
        // These are distinct SDK providers and therefore own separate pools.
        secondProvider =
            await RedisProvider.connect(endpoint, options: options);
        final first = firstProvider.lock('live-$suffix-handoff');
        final second = secondProvider.lock('live-$suffix-handoff');
        const regularLease = LeaseOptions(
          leaseDuration: Duration(seconds: 3),
          rpcTimeout: _rpcTimeout,
          cleanupTimeout: _cleanupTimeout,
        );

        // Contention and release-to-successor handoff across independent pools.
        final initialOwner = await first.tryAcquire(options: regularLease);
        expect(initialOwner, isNotNull);
        expect(await second.tryAcquire(options: regularLease), isNull);
        expect(await initialOwner!.release(), ReleaseOutcome.released);
        final handoffOwner = await second.tryAcquire(options: regularLease);
        expect(handoffOwner, isNotNull);
        expect(await handoffOwner!.release(), ReleaseOutcome.released);

        // The server expires the first generation; its delayed cleanup cannot
        // remove the successor because Redis compares the complete owner value.
        final expiring = firstProvider.lock('live-$suffix-stale-release');
        final successorHandle =
            secondProvider.lock('live-$suffix-stale-release');
        const shortLease = LeaseOptions(
          leaseDuration: Duration(milliseconds: 500),
          rpcTimeout: Duration(milliseconds: 200),
          cleanupTimeout: _cleanupTimeout,
        );
        final staleOwner = await expiring.tryAcquire(options: shortLease);
        expect(staleOwner, isNotNull);
        await Future<void>.delayed(const Duration(milliseconds: 800));
        final successor =
            await successorHandle.tryAcquire(options: regularLease);
        expect(successor, isNotNull);
        expect(await staleOwner!.release(), ReleaseOutcome.alreadyNotOwned);
        expect(await expiring.tryAcquire(options: regularLease), isNull);
        expect(await successor!.release(), ReleaseOutcome.released);

        // Explicit owner-checked renewal extends the real Redis TTL.
        final renewable = firstProvider.lock('live-$suffix-renewal');
        final renewalObserver = secondProvider.lock('live-$suffix-renewal');
        const renewableLease = LeaseOptions(
          leaseDuration: Duration(milliseconds: 1200),
          rpcTimeout: Duration(milliseconds: 300),
          cleanupTimeout: _cleanupTimeout,
        );
        final renewingOwner =
            await renewable.tryAcquire(options: renewableLease);
        expect(renewingOwner, isNotNull);
        await Future<void>.delayed(const Duration(milliseconds: 400));
        expect(await renewingOwner!.renew(), isTrue);
        await Future<void>.delayed(const Duration(milliseconds: 900));
        expect(await renewalObserver.tryAcquire(options: regularLease), isNull);
        expect(renewingOwner.state, LeaseState.held);
        expect(await renewingOwner.release(), ReleaseOutcome.released);
      } finally {
        await firstProvider.close();
        await secondProvider?.close();
      }
    },
    skip: missingConfiguration
        ? 'Set DILOCK_TEST_REDIS_ENDPOINT, DILOCK_TEST_REDIS_NAMESPACE, and DILOCK_TEST_REDIS_EPOCH to run against live Redis.'
        : false,
  );
}

String? _nonEmptyEnvironment(String name) {
  final value = Platform.environment[name];
  if (value == null || value.trim().isEmpty) return null;
  return value;
}
