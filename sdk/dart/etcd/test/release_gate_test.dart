import 'dart:async';

import 'package:dilock/dilock.dart';
import 'package:dilock/src/lease_scope.dart';
import 'package:dilock_etcd/src/release_gate.dart';
import 'package:test/test.dart';

void main() {
  test('unacknowledged attempt lease cleanup upgrades acquisition failure',
      () async {
    const original = DilockException(
      FailureKind.invalidOptions,
      'RPC budget does not fit the granted TTL.',
    );

    final failure = acquisitionFailureAfterAttemptLeaseCleanup(
      original: original,
      cleanup: () async => false,
    );

    final result = await failure;
    expect(result.kind, FailureKind.acquisitionUnknown);
    expect(result.outcomeUnknown, isTrue);
    expect(identical(result.cause, original), isTrue);
  });

  test('cleanup exceptions upgrade acquisition failure to unknown', () async {
    const original = DilockException(
      FailureKind.invalidOptions,
      'RPC budget does not fit the granted TTL.',
    );

    final result = await acquisitionFailureAfterAttemptLeaseCleanup(
      original: original,
      cleanup: () async => throw StateError('revoke transport failed'),
    );

    expect(result.kind, FailureKind.acquisitionUnknown);
    expect(result.outcomeUnknown, isTrue);
    expect(identical(result.cause, original), isTrue);
  });

  test('acknowledged attempt lease cleanup preserves the acquisition failure',
      () async {
    const original = DilockException(
      FailureKind.invalidOptions,
      'RPC budget does not fit the granted TTL.',
    );

    expect(
      identical(
        await acquisitionFailureAfterAttemptLeaseCleanup(
          original: original,
          cleanup: () async => true,
        ),
        original,
      ),
      isTrue,
    );
  });

  test('late etcd verification returns timeout only after acknowledged cleanup',
      () async {
    final failure = await acquisitionCompletionFailure(
      cancelled: false,
      waitBudgetExpired: true,
      cleanup: () async => true,
    );
    expect(failure?.kind, FailureKind.waitTimeout);

    final unknown = await acquisitionCompletionFailure(
      cancelled: false,
      waitBudgetExpired: true,
      cleanup: () async => false,
    );
    expect(unknown?.kind, FailureKind.acquisitionUnknown);
    expect(unknown?.outcomeUnknown, isTrue);
  });

  test(
      'cancellation during etcd verification preserves cancellation only when cleaned',
      () async {
    final cancelled = await acquisitionCompletionFailure(
      cancelled: true,
      waitBudgetExpired: false,
      cleanup: () async => true,
    );
    expect(cancelled?.kind, FailureKind.cancelled);

    final unknown = await acquisitionCompletionFailure(
      cancelled: true,
      waitBudgetExpired: false,
      cleanup: () async => false,
    );
    expect(unknown?.kind, FailureKind.acquisitionUnknown);
  });

  test('keepalive shutdown failure does not skip attempt lease revoke',
      () async {
    var revokeCalls = 0;
    final acknowledged = await stopKeepAliveAndRevokeAttemptLease(
      stopKeepAlive: () async => throw StateError('stream close failed'),
      revoke: () async {
        revokeCalls++;
      },
    );

    expect(acknowledged, isTrue);
    expect(revokeCalls, 1);
  });

  test(
      'keepalive cleanup cancels subscription and clears handles after close fails',
      () async {
    var closeCalls = 0;
    var cancelCalls = 0;
    var referencesCleared = false;

    await stopKeepAliveResources(
      closeRequests: () async {
        closeCalls++;
        throw StateError('request close failed');
      },
      cancelSubscription: () async {
        cancelCalls++;
      },
      clearReferences: () => referencesCleared = true,
    );

    expect(closeCalls, 1);
    expect(cancelCalls, 1);
    expect(referencesCleared, isTrue);
  });

  test('release RPCs share one cleanup budget', () async {
    Duration? unlockBudget;
    Duration? revokeBudget;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (timeout) async {
        unlockBudget = timeout;
        await Future<void>.delayed(const Duration(milliseconds: 20));
        return false;
      },
      revoke: (timeout) async {
        revokeBudget = timeout;
        return RevokeObservation.acknowledged;
      },
    );

    expect(outcome, ReleaseOutcome.alreadyNotOwned);
    expect(unlockBudget, lessThanOrEqualTo(const Duration(milliseconds: 100)));
    expect(revokeBudget, isNotNull);
    expect(revokeBudget, lessThan(unlockBudget!));
    expect(revokeBudget, greaterThan(Duration.zero));
  });

  test('AlreadyNotOwned is a confirmed terminal cleanup result', () {
    expect(isEtcdReleaseConfirmed(ReleaseOutcome.released), isTrue);
    expect(isEtcdReleaseConfirmed(ReleaseOutcome.alreadyNotOwned), isTrue);
    expect(isEtcdReleaseConfirmed(ReleaseOutcome.unknown), isFalse);
  });

  test('successful etcd scope rejects acknowledged revoke-only cleanup', () {
    expect(isEtcdReleaseConfirmed(ReleaseOutcome.alreadyNotOwned), isTrue);
    final failure = etcdScopeCleanupFailure(
      outcome: ReleaseOutcome.alreadyNotOwned,
      leaseWasLost: false,
    );
    expect(failure?.kind, FailureKind.leaseLost);
  });

  test('acknowledged already-not-owned cleanup preserves a work exception',
      () async {
    final workFailure = StateError('work failed');
    var cleanupOutcome = ReleaseOutcome.unknown;
    await expectLater(
      runLeaseScope<void>(
        work: () async => throw workFailure,
        cleanup: ({required bool bodyFailed}) async {
          cleanupOutcome = ReleaseOutcome.alreadyNotOwned;
          if (bodyFailed) return;
          final failure = etcdScopeCleanupFailure(
            outcome: cleanupOutcome,
            leaseWasLost: false,
          );
          if (failure != null) throw failure;
        },
      ),
      throwsA(same(workFailure)),
    );
    expect(cleanupOutcome, ReleaseOutcome.alreadyNotOwned);
    expect(isEtcdReleaseConfirmed(cleanupOutcome), isTrue);
  });

  test('successful etcd scope rejects unknown cleanup acknowledgement', () {
    final failure = etcdScopeCleanupFailure(
      outcome: ReleaseOutcome.unknown,
      leaseWasLost: false,
    );
    expect(failure?.kind, FailureKind.releaseUnknown);
    expect(failure?.outcomeUnknown, isTrue);
  });

  test('successful etcd scope reports a locally lost lease', () {
    final failure = etcdScopeCleanupFailure(
      outcome: ReleaseOutcome.alreadyNotOwned,
      leaseWasLost: true,
    );
    expect(failure?.kind, FailureKind.leaseLost);
  });

  test('release skips revoke when Unlock consumes the cleanup budget',
      () async {
    var revokeCalls = 0;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 5),
      observeOwnership: (_) async => true,
      unlock: (timeout) async {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        return false;
      },
      revoke: (timeout) async {
        revokeCalls++;
        return RevokeObservation.acknowledged;
      },
    );

    expect(outcome, ReleaseOutcome.unknown);
    expect(revokeCalls, 0);
  });

  test('release without lock key gives full budget to revoke', () async {
    Duration? revokeBudget;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: null,
      unlock: null,
      revoke: (timeout) async {
        revokeBudget = timeout;
        return RevokeObservation.acknowledged;
      },
    );

    expect(outcome, ReleaseOutcome.alreadyNotOwned);
    expect(revokeBudget, greaterThan(Duration.zero));
  });

  test('release without lock key and no remaining budget is unknown', () async {
    var revokeCalls = 0;
    final outcome = await runEtcdReleaseCleanup(
      budget: Duration.zero,
      observeOwnership: null,
      unlock: null,
      revoke: (_) async {
        revokeCalls++;
        return RevokeObservation.acknowledged;
      },
    );

    expect(outcome, ReleaseOutcome.unknown);
    expect(revokeCalls, 0);
  });

  test(
      'Unlock exception still attempts revoke and reports acknowledged cleanup',
      () async {
    var revokeCalls = 0;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (_) async => throw StateError('unlock acknowledgement lost'),
      revoke: (_) async {
        revokeCalls++;
        return RevokeObservation.acknowledged;
      },
    );

    expect(outcome, ReleaseOutcome.alreadyNotOwned);
    expect(revokeCalls, 1);
  });

  test('revoke exception is unknown instead of escaping cleanup', () async {
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (_) async => false,
      revoke: (_) async => throw StateError('revoke acknowledgement lost'),
    );

    expect(outcome, ReleaseOutcome.unknown);
  });

  test('an absent exact key skips Unlock and reports already not owned',
      () async {
    var unlockCalls = 0;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => false,
      unlock: (_) async {
        unlockCalls++;
        return true;
      },
      revoke: (_) async => RevokeObservation.acknowledged,
    );

    expect(unlockCalls, 0);
    expect(outcome, ReleaseOutcome.alreadyNotOwned);
  });

  test('unknown exact-key probe never sends Unlock', () async {
    var unlockCalls = 0;
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => null,
      unlock: (_) async {
        unlockCalls++;
        return true;
      },
      revoke: (_) async => RevokeObservation.unknown,
    );

    expect(unlockCalls, 0);
    expect(outcome, ReleaseOutcome.unknown);
  });

  test('Unlock acknowledgement alone is not a released outcome', () async {
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (_) async => true,
      revoke: (_) async => RevokeObservation.unknown,
    );

    expect(outcome, ReleaseOutcome.unknown);
  });

  test('only exact ownership plus Unlock and positive Revoke ACK is Released',
      () async {
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (_) async => true,
      revoke: (_) async => RevokeObservation.acknowledged,
    );

    expect(outcome, ReleaseOutcome.released);
  });

  test('Revoke NOT_FOUND is AlreadyNotOwned even after a prior probe',
      () async {
    final outcome = await runEtcdReleaseCleanup(
      budget: const Duration(milliseconds: 100),
      observeOwnership: (_) async => true,
      unlock: (_) async => true,
      revoke: (_) async => RevokeObservation.notFound,
    );

    expect(outcome, ReleaseOutcome.alreadyNotOwned);
  });

  test('concurrent and repeated release calls share terminal unknown result',
      () async {
    final gate = ReleaseGate<ReleaseOutcome>();
    final response = Completer<ReleaseOutcome>();
    var backendCalls = 0;

    final first = gate.run(() {
      backendCalls++;
      return response.future;
    });
    final concurrent = gate.run(() {
      backendCalls++;
      return Future<ReleaseOutcome>.value(ReleaseOutcome.released);
    });

    expect(identical(concurrent, first), isTrue);
    expect(backendCalls, 1);
    response.complete(ReleaseOutcome.unknown);
    expect(await first, ReleaseOutcome.unknown);
    expect(await concurrent, ReleaseOutcome.unknown);

    final repeated = gate.run(() {
      backendCalls++;
      return Future<ReleaseOutcome>.value(ReleaseOutcome.released);
    });
    expect(identical(repeated, first), isTrue);
    expect(await repeated, ReleaseOutcome.unknown);
    expect(backendCalls, 1);
  });
}
