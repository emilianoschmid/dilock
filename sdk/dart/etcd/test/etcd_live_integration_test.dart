import 'dart:async';
import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_etcd/dilock_etcd.dart';
import 'package:test/test.dart';

void main() {
  final endpoint = Platform.environment['DILOCK_TEST_ETCD_ENDPOINT'];
  final namespace = Platform.environment['DILOCK_TEST_NAMESPACE'];
  final epoch = Platform.environment['DILOCK_TEST_EPOCH'];
  final skipReason = endpoint == null || namespace == null || epoch == null
      ? 'Set DILOCK_TEST_ETCD_ENDPOINT, DILOCK_TEST_NAMESPACE and DILOCK_TEST_EPOCH for live etcd.'
      : false;

  test(
    'native lock handoff keeps queued waiter alive and advances verified fence',
    () async {
      final acquisition = AcquireOptions(
        wait: const Duration(seconds: 8),
        lease: const LeaseOptions(
          leaseDuration: Duration(seconds: 10),
          rpcTimeout: Duration(seconds: 1),
          cleanupTimeout: Duration(seconds: 1),
          autoRenew: true,
          requireFencing: true,
        ),
      );
      final options = EtcdProviderOptions(
        namespace: namespace!,
        coordinationEpoch: epoch!,
        rpcTimeout: const Duration(seconds: 1),
        cleanupTimeout: const Duration(seconds: 1),
        defaultAcquisition: acquisition,
      );
      final firstProvider =
          await EtcdProvider.connect(endpoint!, options: options);
      EtcdProvider? secondProvider;
      EtcdLease? first;
      EtcdLease? successor;
      try {
        secondProvider = await EtcdProvider.connect(endpoint, options: options);
        final resource = 'dart-live-${DateTime.now().microsecondsSinceEpoch}';
        final firstLock = firstProvider.lock(resource);
        final secondLock = secondProvider.lock(resource);

        first = await firstLock.acquire();
        expect(first.fence.coordinationEpoch, epoch);
        expect(first.fence.createRevision, greaterThan(0));
        final firstFenceRevision = first.fence.createRevision;
        expect(await secondLock.tryAcquire(), isNull);

        final shortWaiterPolicy = AcquireOptions(
          wait: const Duration(seconds: 8),
          lease: const LeaseOptions(
            leaseDuration: Duration(seconds: 3),
            rpcTimeout: Duration(seconds: 1),
            cleanupTimeout: Duration(seconds: 1),
            requireFencing: true,
          ),
        );
        final waitingStarted = Stopwatch()..start();
        final queuedAcquire = secondLock.acquire(options: shortWaiterPolicy);
        await Future<void>.delayed(const Duration(seconds: 4));
        expect(first.state, LeaseState.held,
            reason: 'first provider auto-renews its active lease');
        expect(await first.release(), ReleaseOutcome.released);
        first = null;

        successor = await queuedAcquire;
        waitingStarted.stop();
        expect(waitingStarted.elapsed,
            greaterThanOrEqualTo(const Duration(seconds: 4)));
        expect(successor.fence.coordinationEpoch, epoch);
        expect(
          successor.fence.createRevision,
          greaterThan(firstFenceRevision),
        );
        expect(await successor.release(), ReleaseOutcome.released);
        successor = null;
      } finally {
        if (successor != null) await successor.release();
        if (first != null) await first.release();
        await secondProvider?.close();
        await firstProvider.close();
      }
    },
    skip: skipReason,
  );

  test(
    'concurrent close callers wait for the same active-lease shutdown',
    () async {
      final provider = await EtcdProvider.connect(
        endpoint!,
        options: EtcdProviderOptions(
          namespace: namespace!,
          coordinationEpoch: epoch!,
          rpcTimeout: const Duration(seconds: 1),
          cleanupTimeout: const Duration(seconds: 1),
        ),
      );
      Future<void>? firstClose;
      try {
        final lease = await provider
            .lock(
              'dart-close-${DateTime.now().microsecondsSinceEpoch}',
            )
            .acquire();
        expect(lease.state, LeaseState.held);

        firstClose = provider.close();
        var secondCloseCompleted = false;
        final secondClose = provider.close().then((_) {
          secondCloseCompleted = true;
        });
        await Future<void>.delayed(Duration.zero);
        expect(secondCloseCompleted, isFalse,
            reason:
                'every caller must wait for active lease cleanup and channel shutdown');

        await Future.wait<void>(<Future<void>>[firstClose, secondClose]);
        expect(secondCloseCompleted, isTrue);
        expect(lease.state, isNot(LeaseState.held));
      } finally {
        await (firstClose ?? provider.close());
        await provider.close();
      }
    },
    skip: skipReason,
  );

  test(
    'withLease releases the native lock after successful and failed work',
    () async {
      final acquisition = AcquireOptions(
        wait: const Duration(seconds: 8),
        lease: const LeaseOptions(
          leaseDuration: Duration(seconds: 10),
          rpcTimeout: Duration(seconds: 1),
          cleanupTimeout: Duration(seconds: 1),
          autoRenew: true,
          requireFencing: true,
        ),
      );
      final options = EtcdProviderOptions(
        namespace: namespace!,
        coordinationEpoch: epoch!,
        rpcTimeout: const Duration(seconds: 1),
        cleanupTimeout: const Duration(seconds: 1),
        defaultAcquisition: acquisition,
      );
      final provider = await EtcdProvider.connect(endpoint!, options: options);
      try {
        final resource = 'dart-scope-${DateTime.now().microsecondsSinceEpoch}';
        final lock = provider.lock(resource);
        final expected = Object();

        final result = await lock.withLease((lease) async {
          expect(lease.state, LeaseState.held);
          expect(lease.fence.coordinationEpoch, epoch);
          expect(await lock.tryAcquire(), isNull,
              reason: 'the scope must retain etcd ownership while work runs');
          return expected;
        });

        expect(identical(result, expected), isTrue);
        final successor = await lock.acquire();
        try {
          expect(successor.fence.coordinationEpoch, epoch);
        } finally {
          expect(await successor.release(), ReleaseOutcome.released);
        }

        final workFailure = StateError('scoped work failed');
        await expectLater(
          lock.withLease<void>((_) async => throw workFailure),
          throwsA(same(workFailure)),
        );
        final afterFailure = await lock.acquire();
        expect(await afterFailure.release(), ReleaseOutcome.released);
      } finally {
        await provider.close();
      }
    },
    skip: skipReason,
  );

  test(
    'natural lease expiry is not reported as an acknowledged release',
    () async {
      final provider = await EtcdProvider.connect(
        endpoint!,
        options: EtcdProviderOptions(
          namespace: namespace!,
          coordinationEpoch: epoch!,
          rpcTimeout: const Duration(seconds: 1),
          cleanupTimeout: const Duration(seconds: 2),
        ),
      );
      try {
        final lease = await provider
            .lock('dart-expiry-${DateTime.now().microsecondsSinceEpoch}')
            .acquire(
              options: const AcquireOptions(
                wait: Duration(seconds: 4),
                lease: LeaseOptions(
                  leaseDuration: Duration(seconds: 1),
                  rpcTimeout: Duration(milliseconds: 500),
                  cleanupTimeout: Duration(seconds: 2),
                  autoRenew: false,
                ),
              ),
            );
        expect(lease.state, LeaseState.held);
        // etcd expires leases on whole-second ticks; wait beyond two TTLs so
        // scheduler and server tick boundaries cannot make this assertion racy.
        await Future<void>.delayed(const Duration(seconds: 4));
        expect(lease.state, LeaseState.lost);
        expect(await lease.release(), ReleaseOutcome.alreadyNotOwned);
      } finally {
        await provider.close();
      }
    },
    skip: skipReason,
  );
}
