import 'package:dilock/dilock.dart';

/// Observation made by etcd LeaseRevoke for this attempt's unique lease.
enum RevokeObservation {
  /// The server acknowledged revocation of this attempt lease.
  acknowledged,

  /// etcd definitively reported that this attempt lease no longer exists.
  notFound,

  /// The client could not establish whether the attempt lease was revoked.
  unknown,
}

/// Preserves an acquisition failure only after the attempt lease is confirmed
/// revoked. A granted lease with uncertain cleanup is an unknown acquisition.
Future<DilockException> acquisitionFailureAfterAttemptLeaseCleanup({
  required DilockException original,
  required Future<bool> Function() cleanup,
}) async {
  var cleanupAcknowledged = false;
  try {
    cleanupAcknowledged = await cleanup();
  } on Object {
    cleanupAcknowledged = false;
  }
  if (cleanupAcknowledged) return original;
  return DilockException(
    FailureKind.acquisitionUnknown,
    'etcd granted an attempt lease, but cleanup was not acknowledged.',
    cause: original,
    outcomeUnknown: true,
  );
}

/// Classifies cancellation or a late successful verification only after the
/// acquisition's unique lease cleanup has been acknowledged.
Future<DilockException?> acquisitionCompletionFailure({
  required bool cancelled,
  required bool waitBudgetExpired,
  required Future<bool> Function() cleanup,
}) async {
  if (!cancelled && !waitBudgetExpired) return null;
  final original = cancelled
      ? const DilockException(
          FailureKind.cancelled,
          'Acquisition was cancelled during etcd ownership verification.',
        )
      : const DilockException(
          FailureKind.waitTimeout,
          'etcd ownership verification completed after the acquisition wait budget.',
        );
  return acquisitionFailureAfterAttemptLeaseCleanup(
    original: original,
    cleanup: cleanup,
  );
}

/// Stops a lease keepalive before revoking its attempt lease.
///
/// Stream shutdown is best-effort: even when it fails, revoke still runs so
/// that the server-side lease receives an explicit cleanup attempt.
Future<bool> stopKeepAliveAndRevokeAttemptLease({
  required Future<void> Function() stopKeepAlive,
  required Future<void> Function() revoke,
}) async {
  try {
    await stopKeepAlive();
  } on Object {
    // Revocation remains necessary even if the client stream did not stop.
  }
  try {
    await revoke();
    return true;
  } on Object {
    return false;
  }
}

/// Attempts both client-side keepalive shutdown actions and clears references.
/// A shutdown error must not skip cancellation or leave stale handles behind.
Future<void> stopKeepAliveResources({
  required Future<void> Function() closeRequests,
  required Future<void> Function() cancelSubscription,
  required void Function() clearReferences,
}) async {
  try {
    await closeRequests();
  } on Object {
    // Continue to cancel the response subscription.
  }
  try {
    await cancelSubscription();
  } on Object {
    // Cleanup remains best-effort; lease revoke reports server cleanup.
  } finally {
    clearReferences();
  }
}

/// Shares one asynchronous release operation across concurrent and later calls.
///
/// The first result, including an ambiguous outcome or an error, is terminal:
/// invoking cleanup again cannot safely infer what the first request did.
final class ReleaseGate<T> {
  Future<T>? _operation;

  /// Runs [operation] at most once and returns the same future to every caller.
  Future<T> run(Future<T> Function() operation) {
    return _operation ??= Future<T>.sync(operation);
  }
}

/// Whether the shared backend confirmed this acquisition's cleanup outcome.
bool isEtcdReleaseConfirmed(ReleaseOutcome outcome) =>
    outcome == ReleaseOutcome.released ||
    outcome == ReleaseOutcome.alreadyNotOwned;

/// Classifies cleanup at the end of successful scoped work.
///
/// An acknowledged revoke is a confirmed terminal cleanup result, including
/// when Unlock was not acknowledged. It does not prove that the lease remained
/// owned while work ran, so `alreadyNotOwned` is always a lease-loss error for
/// successful scoped work. Missing cleanup acknowledgements remain unknown.
DilockException? etcdScopeCleanupFailure({
  required ReleaseOutcome outcome,
  required bool leaseWasLost,
}) {
  if (leaseWasLost || outcome == ReleaseOutcome.alreadyNotOwned) {
    return const DilockException(
      FailureKind.leaseLost,
      'The etcd lease validity ended before scoped work completed.',
    );
  }
  if (outcome == ReleaseOutcome.unknown) {
    return const DilockException(
      FailureKind.releaseUnknown,
      'etcd release may have executed, but Unlock and LeaseRevoke cleanup acknowledgements were not confirmed.',
      outcomeUnknown: true,
    );
  }
  return null;
}

/// Runs etcd Unlock and LeaseRevoke within one monotonic cleanup budget.
///
/// [unlock] may be omitted when no opaque lock key was returned. Each callback
/// receives only the time remaining from [budget]; cleanup stops when no time
/// remains. Unlock is sent only after a positive exact-key/lease observation.
/// `Released` requires that observation and acknowledgements for both Unlock
/// and unique-lease Revoke. A confirmed Revoke without that full sequence
/// reports `AlreadyNotOwned`.
Future<ReleaseOutcome> runEtcdReleaseCleanup({
  required Duration budget,
  required Future<bool?> Function(Duration timeout)? observeOwnership,
  required Future<bool> Function(Duration timeout)? unlock,
  required Future<RevokeObservation> Function(Duration timeout) revoke,
}) async {
  final elapsed = Stopwatch()..start();
  // A native Unlock can acknowledge a delete of an already expired key. Probe
  // the exact opaque key and attempt lease first; never infer ownership from
  // the Unlock response alone.
  var wasOwned = false;
  final observationBudget = budget - elapsed.elapsed;
  if (observeOwnership != null && observationBudget > Duration.zero) {
    try {
      wasOwned = await observeOwnership(observationBudget) ?? false;
    } on Object {
      // An inconclusive probe must not be followed by an Unlock.
      wasOwned = false;
    }
  }
  var unlockAcknowledged = false;
  final unlockBudget = budget - elapsed.elapsed;
  if (wasOwned && unlock != null && unlockBudget > Duration.zero) {
    try {
      unlockAcknowledged = await unlock(unlockBudget);
    } on Object {
      // An Unlock transport or client failure is an unknown acknowledgement;
      // still try this acquisition's unique lease revoke within the same budget.
      unlockAcknowledged = false;
    }
  }

  var revokeObservation = RevokeObservation.unknown;
  final revokeBudget = budget - elapsed.elapsed;
  if (revokeBudget > Duration.zero) {
    try {
      revokeObservation = await revoke(revokeBudget);
    } on Object {
      // A thrown client error cannot be treated as an acknowledged revoke.
      revokeObservation = RevokeObservation.unknown;
    }
  }
  if (wasOwned &&
      unlockAcknowledged &&
      revokeObservation == RevokeObservation.acknowledged) {
    return ReleaseOutcome.released;
  }
  if (revokeObservation != RevokeObservation.unknown) {
    return ReleaseOutcome.alreadyNotOwned;
  }
  return ReleaseOutcome.unknown;
}
