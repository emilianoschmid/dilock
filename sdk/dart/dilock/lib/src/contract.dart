import 'dart:async';

/// Default total acquisition wait budget.
const defaultWait = Duration(seconds: 5);

/// Default requested lease duration.
const defaultLeaseDuration = Duration(seconds: 30);

/// Default per-command RPC budget.
const defaultRpcTimeout = Duration(seconds: 2);

/// Default independent owner-checked cleanup budget.
const defaultCleanupTimeout = Duration(seconds: 2);

/// Complete timing and capability policy for one lease generation.
final class LeaseOptions {
  /// Creates a complete lease policy using the shared draft 1 defaults.
  const LeaseOptions({
    this.leaseDuration = defaultLeaseDuration,
    this.rpcTimeout = defaultRpcTimeout,
    this.cleanupTimeout = defaultCleanupTimeout,
    this.autoRenew = false,
    this.requireFencing = false,
  });

  /// Requested server-side TTL.
  final Duration leaseDuration;

  /// Maximum budget for one coordinator command.
  final Duration rpcTimeout;

  /// Fresh bounded budget used for owner-specific release and orphan cleanup.
  final Duration cleanupTimeout;

  /// Renews while held when this backend implements owner-checked renewal.
  final bool autoRenew;

  /// Requires a monotonic fence supported by etcd but not Redis draft 1.
  final bool requireFencing;

  /// Returns a copy with selected values changed.
  LeaseOptions copyWith({
    Duration? leaseDuration,
    Duration? rpcTimeout,
    Duration? cleanupTimeout,
    bool? autoRenew,
    bool? requireFencing,
  }) =>
      LeaseOptions(
        leaseDuration: leaseDuration ?? this.leaseDuration,
        rpcTimeout: rpcTimeout ?? this.rpcTimeout,
        cleanupTimeout: cleanupTimeout ?? this.cleanupTimeout,
        autoRenew: autoRenew ?? this.autoRenew,
        requireFencing: requireFencing ?? this.requireFencing,
      );

  /// Validates finite positive budgets, profile TTL bounds, and the safety margin.
  void validate() {
    final ttlMs = leaseDuration.inMilliseconds;
    if (leaseDuration.inMicroseconds % Duration.microsecondsPerMillisecond !=
            0 ||
        ttlMs < 100 ||
        ttlMs > const Duration(days: 1).inMilliseconds) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Lease must be 100 ms through one day in whole milliseconds.',
      );
    }
    if (rpcTimeout <= Duration.zero ||
        cleanupTimeout <= Duration.zero ||
        cleanupTimeout > const Duration(days: 1)) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'RPC and cleanup budgets must be positive and bounded.',
      );
    }
    final margin = safetyMargin;
    if (rpcTimeout + margin >= leaseDuration) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'RPC budget plus safety margin must fit within the lease.',
      );
    }
  }

  /// Conservative scheduling allowance deducted from local lease validity.
  Duration get safetyMargin {
    final onePercent = leaseDuration ~/ 100;
    return onePercent > const Duration(milliseconds: 10)
        ? onePercent
        : const Duration(milliseconds: 10);
  }
}

/// Complete wait budget and lease policy for one acquisition.
final class AcquireOptions {
  /// Creates a full acquisition policy using the shared defaults.
  const AcquireOptions({
    this.wait = defaultWait,
    this.lease = const LeaseOptions(),
  });

  /// Total budget, including metadata, network operations, and contention backoff.
  final Duration wait;

  /// Full lease policy; explicit policies are never partially merged.
  final LeaseOptions lease;

  /// Returns a copy with selected values changed.
  AcquireOptions copyWith({Duration? wait, LeaseOptions? lease}) =>
      AcquireOptions(wait: wait ?? this.wait, lease: lease ?? this.lease);

  /// Validates the whole policy before any backend operation.
  void validate() {
    if (wait <= Duration.zero || wait > const Duration(days: 1)) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Wait budget must be positive and at most one day.',
      );
    }
    lease.validate();
  }
}

/// Local observation of a lease, not proof of current server ownership.
enum LeaseState {
  /// Local conservative ownership is still valid.
  held,

  /// Release started; new protected work must stop.
  releasing,

  /// Release or loss of ownership was acknowledged.
  released,

  /// Cleanup was attempted without a reliable acknowledgement.
  releasedUnknown,

  /// Local conservative validity ended or renewal failed.
  lost,
}

/// Acknowledgement of an owner-checked release.
enum ReleaseOutcome {
  /// The backend acknowledged removal of this owner generation.
  released,

  /// No key belonging to this owner generation remained.
  alreadyNotOwned,

  /// Release may have executed, but acknowledgement was not confirmed.
  unknown,
}

/// Adapter cleanup information with no owner token, password, or protocol payload.
final class CleanupObservation {
  /// Creates safe cleanup diagnostic fields.
  const CleanupObservation({
    required this.backend,
    required this.outcome,
    required this.reason,
  });

  /// Backend profile name.
  final String backend;

  /// Explicit cleanup observation.
  final ReleaseOutcome outcome;

  /// Non-sensitive cleanup reason.
  final String reason;
}

/// Stable failure categories shared by native Dart backend adapters.
enum FailureKind {
  /// Backend transport failed or a known server rejection occurred.
  backendUnavailable,

  /// A dispatched acquisition did not receive trustworthy ownership evidence.
  acquisitionUnknown,

  /// Metadata or a backend response violates the selected draft 1 profile.
  protocolMismatch,

  /// The selected backend cannot provide a requested guarantee.
  unsupportedCapability,

  /// The finite acquisition wait budget elapsed after contention.
  waitTimeout,

  /// Local lease validity ended; protected work must stop.
  leaseLost,

  /// Release may have executed, but the backend did not confirm cleanup.
  releaseUnknown,

  /// The owning provider has been closed.
  providerClosed,

  /// Local configuration or timing policy is invalid.
  invalidOptions,

  /// Caller requested cancellation; outcomeUnknown identifies post-dispatch ambiguity.
  cancelled,
}

/// Typed failure with a stable category and an optional original cause.
final class DilockException implements Exception {
  /// Creates a typed failure.
  const DilockException(
    this.kind,
    this.message, {
    this.cause,
    this.outcomeUnknown = false,
  });

  /// Stable semantic category.
  final FailureKind kind;

  /// Safe contextual description.
  final String message;

  /// Original low-level failure, when available.
  final Object? cause;

  /// Whether a dispatched operation may have committed remotely.
  final bool outcomeUnknown;

  @override
  String toString() =>
      'DilockException(${kind.name}): $message${cause == null ? '' : ': $cause'}';
}

/// Cooperative cancellation signal for Dart operations.
///
/// Dart Futures are not preemptively cancellable. A token can cancel an
/// acquisition wait and make the adapter close a dispatched socket, but the
/// command may already have committed; the adapter then performs tracked
/// owner-specific cleanup and reports an unknown acquisition outcome.
final class CancellationToken {
  /// Creates an independent, initially active cancellation token.
  CancellationToken();

  final _cancelled = Completer<void>();

  /// Whether cancellation has been requested.
  bool get isCancelled => _cancelled.isCompleted;

  /// Completes when cancellation is requested.
  Future<void> get whenCancelled => _cancelled.future;

  /// Requests cancellation once.
  void cancel() {
    if (!_cancelled.isCompleted) _cancelled.complete();
  }

  /// Throws a typed cancellation exception if cancellation was requested.
  void throwIfCancelled() {
    if (isCancelled) {
      throw const DilockException(
        FailureKind.cancelled,
        'Operation was cancelled.',
      );
    }
  }
}
