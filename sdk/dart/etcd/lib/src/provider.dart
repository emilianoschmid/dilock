import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dilock/dilock.dart';
// The Redis and etcd packages share this internal contract within the monorepo.
// ignore: implementation_imports
import 'package:dilock/src/metadata_contract.dart';
// ignore: implementation_imports
import 'package:dilock/src/lease_scope.dart';
import 'release_gate.dart';
import 'package:fixnum/fixnum.dart';
import 'package:grpc/grpc.dart';

import 'generated/rpc.pbgrpc.dart';
import 'generated/v3lock.pbgrpc.dart';

const _profile = 'etcd-native-lock-v1';
const _maxTtl = Duration(days: 1);

/// Immutable endpoint, namespace, and timing policy for one etcd authority.
final class EtcdProviderOptions {
  /// Creates options. The namespace is pre-provisioned by the operator.
  const EtcdProviderOptions({
    this.namespace = 'default',
    required this.coordinationEpoch,
    this.rpcTimeout = const Duration(seconds: 2),
    this.cleanupTimeout = const Duration(seconds: 2),
    this.defaultAcquisition = const AcquireOptions(),
    this.cleanupObserver,
  });

  /// Draft 1 metadata namespace, validated before any RPC.
  final String namespace;

  /// Operator-approved cluster incarnation used to scope fencing values.
  final String coordinationEpoch;

  /// Default deadline for one etcd unary call or keepalive acknowledgement.
  final Duration rpcTimeout;

  /// Independent bounded budget for cleanup RPCs.
  final Duration cleanupTimeout;

  /// Complete immutable policy used when an acquisition omits one.
  final AcquireOptions defaultAcquisition;

  /// Optional callback for cleanup whose outcome could not be confirmed.
  final void Function(CleanupObservation observation)? cleanupObserver;
}

/// Owns a gRPC channel and generated clients for one etcd cluster.
///
/// A provider is safe for concurrent independent resources. Its channel must
/// remain private to the provider; use separate providers for different
/// clusters or credentials. [close] cancels in-flight calls, cleans up known
/// leases, and closes the channel.
final class EtcdProvider {
  EtcdProvider._(this.options, this._channel)
      : _kv = KVClient(_channel),
        _leasesRpc = LeaseClient(_channel),
        _locks = LockClient(_channel);

  /// Opens a plaintext `http://` or TLS `https://` etcd endpoint and verifies
  /// the pre-provisioned namespace metadata before returning.
  static Future<EtcdProvider> connect(
    String endpoint, {
    required EtcdProviderOptions options,
  }) async {
    _validateOptions(options);
    final uri = _parseEndpoint(endpoint);
    final channel = ClientChannel(
      uri.host,
      port: uri.port,
      options: ChannelOptions(
        credentials: uri.scheme == 'https'
            ? const ChannelCredentials.secure()
            : const ChannelCredentials.insecure(),
      ),
    );
    final provider = EtcdProvider._(options, channel);
    try {
      await provider._readMetadata(options.defaultAcquisition.lease.rpcTimeout);
      return provider;
    } on Object {
      await channel.shutdown();
      rethrow;
    }
  }

  /// Immutable configuration governing this provider and its locks.
  final EtcdProviderOptions options;
  final ClientChannel _channel;
  final KVClient _kv;
  final LeaseClient _leasesRpc;
  final LockClient _locks;
  final Stopwatch _clock = Stopwatch()..start();
  final Set<EtcdLease> _activeLeases = <EtcdLease>{};
  final Set<Future<void>> _cleanupTasks = <Future<void>>{};
  final Set<void Function()> _cancelPending = <void Function()>{};
  bool _closed = false;
  Future<void>? _closeFuture;
  int _activeOperations = 0;
  Completer<void>? _operationsDone;

  int get _monotonicMicros => _clock.elapsedMicroseconds;

  /// Creates a reusable resource handle without network activity.
  EtcdLock lock(String resource) {
    _ensureOpen();
    return EtcdLock._(
      this,
      utf8.encode(etcdLockName(options.namespace, resource)),
    );
  }

  /// Waits for orphan cleanups started after cancellation or ambiguous RPCs.
  Future<void> waitForCleanup() async {
    while (_cleanupTasks.isNotEmpty) {
      await Future.wait<void>(List<Future<void>>.of(_cleanupTasks));
    }
  }

  /// Cancels calls, revokes active acquisitions, waits for cleanup, then
  /// closes this provider's channel. Repeated calls are safe.
  Future<void> close() => _closeFuture ??= _close();

  Future<void> _close() async {
    _closed = true;
    for (final cancel in List<void Function()>.of(_cancelPending)) {
      cancel();
    }
    final done = _operationsDone;
    if (_activeOperations > 0 && done != null) await done.future;
    for (final lease in List<EtcdLease>.of(_activeLeases)) {
      await lease.release(reason: 'provider_shutdown');
    }
    await waitForCleanup();
    await _channel.shutdown();
  }

  void _ensureOpen() {
    if (_closed) {
      throw const DilockException(
        FailureKind.providerClosed,
        'etcd provider is closed.',
      );
    }
  }

  void _begin() {
    _ensureOpen();
    if (_activeOperations++ == 0) _operationsDone = Completer<void>();
  }

  void _end() {
    if (--_activeOperations == 0) {
      _operationsDone?.complete();
      _operationsDone = null;
    }
  }

  void _register(EtcdLease lease) => _activeLeases.add(lease);
  void _remove(EtcdLease lease) => _activeLeases.remove(lease);

  Future<void> _readMetadata(Duration timeout) async {
    final key = utf8.encode('/dilock/namespaces/${options.namespace}/meta');
    final RangeResponse response;
    try {
      response = await _kv.range(
        RangeRequest()..key = key,
        options: CallOptions(timeout: timeout),
      );
    } on GrpcError catch (error) {
      throw _backendFailure('etcd namespace metadata read failed', error);
    }
    final values = _values(response.getField(2));
    if (_intField(response, 4) != 1 || values.length != 1) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'etcd namespace metadata is missing or not a single exact key.',
      );
    }
    final kv = values.single;
    if (!_sameBytes(_bytesField(kv, 1), key)) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'etcd returned a different key for namespace metadata.',
      );
    }
    final raw = _bytesField(kv, 5);
    if (raw.isEmpty || raw.length > 8192) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'etcd namespace metadata is empty or exceeds 8192 bytes.',
      );
    }
    final Map<String, dynamic> metadata;
    try {
      final decoded = jsonDecode(utf8.decode(raw, allowMalformed: false));
      if (decoded is! Map<String, dynamic>) {
        throw const FormatException('metadata must be an object');
      }
      metadata = decoded;
    } on Object catch (error) {
      throw DilockException(
        FailureKind.protocolMismatch,
        'etcd namespace metadata is not valid UTF-8 JSON.',
        cause: error,
      );
    }
    validateNamespaceMetadata(
      metadata,
      profile: _profile,
      coordinationEpoch: options.coordinationEpoch,
    );
  }

  Future<RangeResponse> _range(
    List<int> key,
    List<int>? end,
    Duration timeout,
  ) {
    final request = RangeRequest()..key = key;
    if (end != null) request.rangeEnd = end;
    return _kv.range(request, options: CallOptions(timeout: timeout));
  }

  Future<void> _revoke(int leaseId, Duration timeout) async {
    final request = LeaseRevokeRequest()..setField(1, Int64(leaseId));
    await _leasesRpc.leaseRevoke(
      request,
      options: CallOptions(timeout: timeout),
    );
  }

  /// Confirms that the returned opaque Lock key still belongs to this attempt.
  /// A null result means the linearizable probe failed or returned malformed
  /// data; callers must not send native Unlock on that uncertainty.
  Future<bool?> _ownsExactLock(
    List<int> key,
    int leaseId,
    Duration timeout,
  ) async {
    try {
      final response = await _range(key, null, timeout);
      final values = _values(response.getField(2));
      if (_intField(response, 4) == 0 && values.isEmpty) return false;
      if (_intField(response, 4) != 1 || values.length != 1) return null;
      final value = values.single;
      return _sameBytes(_bytesField(value, 1), key) &&
          _intField(value, 6) == leaseId;
    } on Object {
      return null;
    }
  }

  void _trackCleanup(Future<void> task) {
    _cleanupTasks.add(task);
    task.whenComplete(() => _cleanupTasks.remove(task));
  }

  void _observe(ReleaseOutcome outcome, String reason) {
    final callback = options.cleanupObserver;
    if (callback == null) return;
    try {
      callback(
        CleanupObservation(backend: 'etcd', outcome: outcome, reason: reason),
      );
    } on Object {
      // Observability is advisory and must never change lease safety outcomes.
    }
  }

  Future<T> _invoke<T>(
    ResponseFuture<T> call,
    CancellationToken? token, {
    required bool acquisitionMayCommit,
  }) async {
    void cancel() => call.cancel();
    _cancelPending.add(cancel);
    if (token == null) {
      try {
        return await call;
      } finally {
        _cancelPending.remove(cancel);
      }
    }
    if (token.isCancelled) {
      call.cancel();
      _cancelPending.remove(cancel);
      throw DilockException(
        acquisitionMayCommit
            ? FailureKind.acquisitionUnknown
            : FailureKind.cancelled,
        'Operation was cancelled after the etcd RPC was dispatched.',
        outcomeUnknown: acquisitionMayCommit,
      );
    }
    try {
      return await Future.any<T>(<Future<T>>[
        call,
        token.whenCancelled.then<T>((_) {
          call.cancel();
          throw DilockException(
            acquisitionMayCommit
                ? FailureKind.acquisitionUnknown
                : FailureKind.cancelled,
            'Operation was cancelled after the etcd RPC was dispatched.',
            outcomeUnknown: acquisitionMayCommit,
          );
        }),
      ]);
    } on DilockException {
      rethrow;
    } finally {
      _cancelPending.remove(cancel);
    }
  }

  Future<EtcdLease?> _acquire(
    List<int> name,
    AcquireOptions policy, {
    required bool tryOnly,
    CancellationToken? token,
  }) async {
    _begin();
    final elapsed = Stopwatch()..start();
    var leaseId = 0;
    EtcdLease? lease;
    try {
      policy.validate();
      token?.throwIfCancelled();
      await _readMetadata(_remaining(policy.wait, elapsed, options.rpcTimeout));
      token?.throwIfCancelled();
      final ttlSeconds =
          (policy.lease.leaseDuration.inMilliseconds + 999) ~/ 1000;
      final grantRequest = LeaseGrantRequest()..setField(1, Int64(ttlSeconds));
      final grantSentAt = _monotonicMicros;
      final LeaseGrantResponse grant;
      final grantCall = _leasesRpc.leaseGrant(
        grantRequest,
        options: CallOptions(
          timeout: _remaining(policy.wait, elapsed, policy.lease.rpcTimeout),
        ),
      );
      try {
        grant = await _invoke(grantCall, token, acquisitionMayCommit: true);
      } on GrpcError catch (error) {
        if (error.code == 4) {
          throw const DilockException(
            FailureKind.acquisitionUnknown,
            'etcd lease grant response was not acknowledged.',
            outcomeUnknown: true,
          );
        }
        throw _backendFailure('etcd lease grant failed', error);
      }
      leaseId = _intField(grant, 2);
      final effectiveTtl = _intField(grant, 3);
      if (leaseId == 0 || effectiveTtl <= 0) {
        throw const DilockException(
          FailureKind.protocolMismatch,
          'etcd returned an invalid lease grant.',
        );
      }
      final effectiveDuration = Duration(seconds: effectiveTtl);
      if (policy.lease.rpcTimeout + _margin(effectiveDuration) >=
          effectiveDuration) {
        throw const DilockException(
          FailureKind.invalidOptions,
          'RPC timeout plus safety margin does not fit within the etcd grant TTL.',
        );
      }
      final attemptLease = EtcdLease._(
        this,
        leaseId,
        effectiveDuration,
        policy.lease,
        grantSentAt,
      );
      lease = attemptLease;
      _register(attemptLease);
      await attemptLease._startKeepAlive(
        token,
        _remaining(policy.wait, elapsed, policy.lease.rpcTimeout),
      );
      token?.throwIfCancelled();

      final lockTimeout = tryOnly
          ? policy.lease.rpcTimeout
          : _remaining(policy.wait, elapsed, policy.wait);
      final lockRequest = LockRequest()
        ..name = name
        ..setField(2, Int64(leaseId));
      final lockCall = _locks.lock(
        lockRequest,
        options: CallOptions(timeout: lockTimeout),
      );
      final LockResponse locked;
      try {
        locked = await _invoke(lockCall, token, acquisitionMayCommit: true);
      } on GrpcError catch (error) {
        await attemptLease._abandon('lock_rpc_failed');
        if (attemptLease.state == LeaseState.releasedUnknown) {
          throw const DilockException(
            FailureKind.acquisitionUnknown,
            'etcd Lock outcome and attempt-lease cleanup are unknown.',
            outcomeUnknown: true,
          );
        }
        if (tryOnly && _isNativeLockWaitDeadline(error)) {
          try {
            if (await _hasQueuedOwner(name, policy.lease.cleanupTimeout)) {
              return null;
            }
          } on GrpcError catch (probeError) {
            throw DilockException(
              FailureKind.acquisitionUnknown,
              'etcd Lock timed out and the contention Range probe failed.',
              cause: probeError,
              outcomeUnknown: true,
            );
          }
          throw DilockException(
            FailureKind.acquisitionUnknown,
            'etcd Lock timed out without a confirmed competing owner.',
            cause: error,
            outcomeUnknown: true,
          );
        }
        if (error.code == 4 && !tryOnly) {
          throw const DilockException(
            FailureKind.waitTimeout,
            'etcd contention wait budget elapsed.',
          );
        }
        throw _backendFailure('etcd native Lock RPC failed', error);
      } on DilockException {
        await attemptLease._abandon('lock_cancelled');
        rethrow;
      }
      final opaqueKey = locked.key;
      if (opaqueKey.isEmpty) {
        await attemptLease._abandon('empty_lock_key');
        throw const DilockException(
          FailureKind.protocolMismatch,
          'etcd native Lock returned an empty opaque key.',
        );
      }
      attemptLease._key = Uint8List.fromList(opaqueKey);
      final RangeResponse verified;
      try {
        verified = await _range(
          attemptLease._key!,
          null,
          tryOnly
              ? policy.lease.rpcTimeout
              : _remaining(policy.wait, elapsed, policy.lease.rpcTimeout),
        );
      } on GrpcError catch (error) {
        final completionFailure = await acquisitionCompletionFailure(
          cancelled: token?.isCancelled ?? false,
          waitBudgetExpired: !tryOnly && elapsed.elapsed >= policy.wait,
          cleanup: () async {
            await attemptLease._abandon('verification_interrupted');
            return attemptLease.state == LeaseState.released;
          },
        );
        if (completionFailure != null) throw completionFailure;
        await attemptLease._abandon('lock_key_verification_failed');
        throw DilockException(
          FailureKind.acquisitionUnknown,
          'etcd exact-key verification failed; cleanup was attempted.',
          cause: error,
          outcomeUnknown: true,
        );
      }
      final kvs = _values(verified.getField(2));
      if (_intField(verified, 4) != 1 || kvs.length != 1) {
        await attemptLease._abandon('lock_key_verification_mismatch');
        throw const DilockException(
          FailureKind.acquisitionUnknown,
          'Linearizable Range did not return exactly one etcd lock key.',
          outcomeUnknown: true,
        );
      }
      final kv = kvs.single;
      final actualKey = _bytesField(kv, 1);
      final actualCreateRevision = _intField(kv, 2);
      if (!_sameBytes(actualKey, attemptLease._key!) ||
          _intField(kv, 6) != leaseId ||
          actualCreateRevision <= 0) {
        await attemptLease._abandon('lock_owner_verification_mismatch');
        throw const DilockException(
          FailureKind.acquisitionUnknown,
          'etcd lock key ownership or create revision did not match this lease.',
          outcomeUnknown: true,
        );
      }
      final completionFailure = await acquisitionCompletionFailure(
        cancelled: token?.isCancelled ?? false,
        waitBudgetExpired: !tryOnly && elapsed.elapsed >= policy.wait,
        cleanup: () async {
          await attemptLease
              ._abandon('verification_completed_too_late_or_cancelled');
          return attemptLease.state == LeaseState.released;
        },
      );
      if (completionFailure != null) throw completionFailure;
      if (!attemptLease._isConservativelyValid) {
        await attemptLease._abandon('verification_exceeded_validity');
        throw const DilockException(
          FailureKind.acquisitionUnknown,
          'etcd lock verification exceeded the confirmed lease budget.',
          outcomeUnknown: true,
        );
      }
      attemptLease._fence =
          EtcdFence(options.coordinationEpoch, actualCreateRevision);
      attemptLease._activate();
      if (!policy.lease.autoRenew) attemptLease._stopAutomaticRenewal();
      return attemptLease;
    } on DilockException catch (error) {
      final activeLease = lease;
      if (activeLease != null && activeLease.state != LeaseState.released) {
        final failure = await acquisitionFailureAfterAttemptLeaseCleanup(
          original: error,
          cleanup: () async {
            await activeLease._abandon('acquisition_failed');
            return activeLease.state == LeaseState.released;
          },
        );
        if (!identical(failure, error)) {
          throw failure;
        }
      } else if (leaseId != 0 && lease == null) {
        final failure = await acquisitionFailureAfterAttemptLeaseCleanup(
          original: error,
          cleanup: () => _boundedRevoke(leaseId, policy.lease.cleanupTimeout),
        );
        if (!identical(failure, error)) {
          throw failure;
        }
      }
      rethrow;
    } on TimeoutException catch (error) {
      if (lease != null) {
        await lease._abandon('acquisition_rpc_timed_out');
      } else if (leaseId != 0) {
        await _boundedRevoke(leaseId, policy.lease.cleanupTimeout);
      }
      throw DilockException(
        FailureKind.acquisitionUnknown,
        'etcd acquisition RPC did not complete within its deadline.',
        cause: error,
        outcomeUnknown: true,
      );
    } finally {
      _end();
    }
  }

  Future<bool> _hasQueuedOwner(List<int> name, Duration timeout) async {
    final prefix = <int>[...name, 0x2f];
    final response = await _range(prefix, _prefixEnd(prefix), timeout);
    return _intField(response, 4) > 0;
  }

  Future<bool> _boundedRevoke(int id, Duration timeout) async {
    try {
      await _revoke(id, timeout);
      return true;
    } on GrpcError {
      return false;
    } on Object {
      return false;
    }
  }
}

/// Reusable handle for one namespaced resource. Each acquisition receives a
/// distinct server lease and cannot re-enter through this handle.
final class EtcdLock {
  EtcdLock._(this._provider, this._name);

  final EtcdProvider _provider;
  final List<int> _name;

  /// Attempts one Lock RPC; returns null only after bounded cleanup and a
  /// Range query confirm another owner is queued or holding the native lock.
  Future<EtcdLease?> tryAcquire({
    LeaseOptions? options,
    CancellationToken? cancellationToken,
  }) {
    final lease = options ?? _provider.options.defaultAcquisition.lease;
    return _provider._acquire(
      _name,
      AcquireOptions(wait: lease.rpcTimeout, lease: lease),
      tryOnly: true,
      token: cancellationToken,
    );
  }

  /// Queues through etcd's native Lock service until ownership, cancellation,
  /// backend failure, or the finite total wait budget is reached.
  Future<EtcdLease> acquire({
    AcquireOptions? options,
    CancellationToken? cancellationToken,
  }) async {
    final result = await _provider._acquire(
      _name,
      options ?? _provider.options.defaultAcquisition,
      tryOnly: false,
      token: cancellationToken,
    );
    return result!;
  }

  /// Acquires using immutable provider defaults.
  Future<EtcdLease> acquireDefault({CancellationToken? cancellationToken}) =>
      acquire(cancellationToken: cancellationToken);

  /// Runs [work] once and performs bounded owner-specific cleanup on scope exit.
  Future<T> withLease<T>(
    Future<T> Function(EtcdLease lease) work, {
    CancellationToken? cancellationToken,
  }) =>
      withLeaseOptions<T>(
        _provider.options.defaultAcquisition,
        work,
        cancellationToken: cancellationToken,
      );

  /// Uses one complete explicit policy. An unknown release acknowledgement
  /// throws [DilockException] with [FailureKind.releaseUnknown] after normal
  /// work; an already-lost lease throws [FailureKind.leaseLost]. An acknowledged
  /// LeaseRevoke is confirmed terminal cleanup even if Unlock was not
  /// acknowledged. Cleanup failure cannot replace an exception thrown by
  /// [work].
  Future<T> withLeaseOptions<T>(
    AcquireOptions policy,
    Future<T> Function(EtcdLease lease) work, {
    CancellationToken? cancellationToken,
  }) async {
    final lease = await acquire(
      options: policy,
      cancellationToken: cancellationToken,
    );
    return runLeaseScope<T>(
      work: () => work(lease),
      cleanup: ({required bool bodyFailed}) async {
        final wasLost = lease.state == LeaseState.lost;
        final outcome = await lease.release(
          reason: bodyFailed ? 'body_failed' : 'scope_exit',
        );
        if (bodyFailed) return;
        final failure = etcdScopeCleanupFailure(
          outcome: outcome,
          leaseWasLost: wasLost,
        );
        if (failure != null) throw failure;
      },
    );
  }
}

/// Epoch-scoped fencing token from an etcd KV create revision.
final class EtcdFence {
  /// Creates a fence obtained by exact-key verification after native Lock.
  const EtcdFence(this.coordinationEpoch, this.createRevision);

  /// Operator-approved cluster incarnation; snapshot restore requires rotation.
  final String coordinationEpoch;

  /// Monotonically increasing etcd key creation revision within the epoch.
  final int createRevision;
}

/// One native etcd lease and opaque Lock key.
final class EtcdLease {
  EtcdLease._(
    this._provider,
    this.id,
    this._ttl,
    this.options,
    int grantSentAtMicros,
  ) : _deadlineMicros =
            grantSentAtMicros + (_ttl - _margin(_ttl)).inMicroseconds;

  final EtcdProvider _provider;

  /// Unique etcd lease identifier, exposed only for diagnostics.
  final int id;

  /// Complete timing and capability policy used for this acquisition.
  final LeaseOptions options;

  Duration _ttl;
  int _deadlineMicros;
  Uint8List? _key;
  EtcdFence? _fence;
  LeaseState _state = LeaseState.held;
  StreamController<LeaseKeepAliveRequest>? _keepAliveRequests;
  StreamSubscription<LeaseKeepAliveResponse>? _keepAliveSubscription;
  Timer? _renewalTimer;
  Timer? _ackDeadline;
  Completer<bool>? _nextRenewal;
  final Queue<int> _sentAt = Queue<int>();
  bool _automaticRenewal = true;
  bool _released = false;
  final ReleaseGate<ReleaseOutcome> _releaseGate =
      ReleaseGate<ReleaseOutcome>();

  /// Local conservative lease state. It is not proof of current server state.
  LeaseState get state {
    _expireIfNeeded();
    return _state;
  }

  /// Etcd fence after successful linearizable ownership verification.
  EtcdFence get fence {
    final value = _fence;
    if (value == null) {
      throw const DilockException(
        FailureKind.leaseLost,
        'Fencing value is not available before acquisition verification.',
      );
    }
    return value;
  }

  /// Server-assigned effective lease TTL, rounded up by etcd to whole seconds.
  Duration get effectiveLease => _ttl;

  /// Withdraws protected-work permission as soon as local validity is lost.
  void ensureHeld() {
    if (state != LeaseState.held) {
      throw const DilockException(
        FailureKind.leaseLost,
        'etcd lease is no longer conservatively valid.',
      );
    }
  }

  /// Requests an acknowledged keepalive renewal and returns false after loss.
  Future<bool> renew() async {
    ensureHeld();
    final pending = _nextRenewal ??= Completer<bool>();
    _sendKeepAlive();
    try {
      return await pending.future.timeout(options.rpcTimeout);
    } on TimeoutException catch (error) {
      _markLost();
      throw DilockException(
        FailureKind.backendUnavailable,
        'etcd LeaseKeepAlive was not acknowledged within its RPC budget.',
        cause: error,
      );
    } finally {
      if (identical(_nextRenewal, pending)) _nextRenewal = null;
    }
  }

  /// Releases the exact opaque key and revokes this acquisition's unique lease.
  Future<ReleaseOutcome> release({String reason = 'explicit_release'}) {
    return _releaseGate.run(() => _release(reason));
  }

  Future<ReleaseOutcome> _release(String reason) async {
    if (_released) {
      return _state == LeaseState.releasedUnknown
          ? ReleaseOutcome.unknown
          : ReleaseOutcome.alreadyNotOwned;
    }
    _released = true;
    _state = LeaseState.releasing;
    await _stopKeepAlive();
    final key = _key;
    final outcome = await runEtcdReleaseCleanup(
      budget: options.cleanupTimeout,
      observeOwnership: key == null
          ? null
          : (timeout) => _provider._ownsExactLock(key, id, timeout),
      unlock: key == null
          ? null
          : (timeout) async {
              try {
                await _provider._locks.unlock(
                  UnlockRequest()..key = key,
                  options: CallOptions(timeout: timeout),
                );
                return true;
              } on GrpcError {
                return false;
              }
            },
      revoke: (timeout) async {
        try {
          await _provider._revoke(id, timeout);
          return RevokeObservation.acknowledged;
        } on GrpcError catch (error) {
          // etcd's NOT_FOUND is a definitive observation that the unique
          // attempt lease has already expired or been revoked.
          if (error.code == 5) return RevokeObservation.notFound;
          return RevokeObservation.unknown;
        } on Object {
          return RevokeObservation.unknown;
        }
      },
    );
    _provider._remove(this);
    if (isEtcdReleaseConfirmed(outcome)) {
      _state = LeaseState.released;
      return outcome;
    }
    _state = LeaseState.releasedUnknown;
    _provider._observe(ReleaseOutcome.unknown, reason);
    return ReleaseOutcome.unknown;
  }

  bool get _isConservativelyValid {
    _expireIfNeeded();
    return _state == LeaseState.held &&
        _provider._monotonicMicros < _deadlineMicros;
  }

  void _activate() => _state = LeaseState.held;

  Future<void> _startKeepAlive(
    CancellationToken? token,
    Duration initialAcknowledgementBudget,
  ) async {
    _nextRenewal = Completer<bool>();
    _keepAliveRequests = StreamController<LeaseKeepAliveRequest>();
    final responses = _provider._leasesRpc.leaseKeepAlive(
      _keepAliveRequests!.stream,
      options: CallOptions(),
    );
    _keepAliveSubscription = responses.listen(
      _onKeepAlive,
      onError: (Object error) => _markLost(),
      onDone: () => _markLost(),
      cancelOnError: true,
    );
    _sendKeepAlive();
    final tokenFuture = token?.whenCancelled;
    if (tokenFuture != null) {
      await Future.any<void>(<Future<void>>[
        _nextRenewal!.future.then<void>((_) {}),
        tokenFuture.then<void>(
          (_) => throw const DilockException(
            FailureKind.acquisitionUnknown,
            'Cancelled while confirming etcd lease keepalive.',
            outcomeUnknown: true,
          ),
        ),
      ]).timeout(initialAcknowledgementBudget);
    } else {
      // The first confirmation is part of acquisition, so it shares the
      // caller's total wait deadline instead of receiving a fresh RPC budget.
      await _nextRenewal!.future.timeout(initialAcknowledgementBudget);
    }
    if (_state != LeaseState.held) {
      throw const DilockException(
        FailureKind.backendUnavailable,
        'etcd lease could not be confirmed before native Lock.',
      );
    }
    _nextRenewal = null;
    // Native Lock waiters must keep their unique attempt lease alive even
    // when automatic renewal after acquisition was not requested.
    _automaticRenewal = true;
    _scheduleRenewal();
  }

  void _sendKeepAlive() {
    if (_state == LeaseState.lost || _state == LeaseState.released) return;
    final requests = _keepAliveRequests;
    if (requests == null || requests.isClosed) return;
    _sentAt.add(_provider._monotonicMicros);
    _ackDeadline?.cancel();
    _ackDeadline = Timer(options.rpcTimeout, _markLost);
    requests.add(LeaseKeepAliveRequest()..setField(1, Int64(id)));
  }

  void _onKeepAlive(LeaseKeepAliveResponse response) {
    if (_sentAt.isEmpty) return;
    final sent = _sentAt.removeFirst();
    _ackDeadline?.cancel();
    _ackDeadline = null;
    final responseId = _intField(response, 2);
    final responseTtl = _intField(response, 3);
    if (responseId != id || responseTtl <= 0 || !_isConservativelyValid) {
      _markLost();
      return;
    }
    final ttl = Duration(seconds: responseTtl);
    final deadlineMicros = sent + (ttl - _margin(ttl)).inMicroseconds;
    if (_provider._monotonicMicros >= deadlineMicros) {
      _markLost();
      return;
    }
    _ttl = ttl;
    _deadlineMicros = deadlineMicros;
    final pending = _nextRenewal;
    if (pending != null && !pending.isCompleted) pending.complete(true);
  }

  void _scheduleRenewal() {
    _renewalTimer?.cancel();
    _renewalTimer = Timer(_ttl ~/ 3, () {
      if (!_automaticRenewal || state != LeaseState.held) return;
      _sendKeepAlive();
      _scheduleRenewal();
    });
  }

  void _stopAutomaticRenewal() {
    _automaticRenewal = false;
    _renewalTimer?.cancel();
    _renewalTimer = null;
  }

  void _expireIfNeeded() {
    if (_state == LeaseState.held &&
        _provider._monotonicMicros >= _deadlineMicros) {
      _markLost();
    }
  }

  void _markLost() {
    if (_state == LeaseState.released || _state == LeaseState.releasedUnknown) {
      return;
    }
    _state = LeaseState.lost;
    _stopAutomaticRenewal();
    final pending = _nextRenewal;
    if (pending != null && !pending.isCompleted) pending.complete(false);
  }

  Future<void> _abandon(String reason) async {
    if (_released) return;
    _released = true;
    _state = LeaseState.releasing;
    final acknowledged = await stopKeepAliveAndRevokeAttemptLease(
      stopKeepAlive: _stopKeepAlive,
      revoke: () => _provider._revoke(id, options.cleanupTimeout),
    );
    _provider._remove(this);
    _state = acknowledged ? LeaseState.released : LeaseState.releasedUnknown;
    if (!acknowledged) {
      _provider._observe(ReleaseOutcome.unknown, reason);
      Future<void> retryRevoke() async {
        try {
          await _provider._revoke(id, options.cleanupTimeout);
        } on Object {
          _provider._observe(ReleaseOutcome.unknown, '$reason:retry_failed');
        }
      }

      _provider._trackCleanup(retryRevoke());
    }
  }

  Future<void> _stopKeepAlive() async {
    _stopAutomaticRenewal();
    _ackDeadline?.cancel();
    _ackDeadline = null;
    final requests = _keepAliveRequests;
    final subscription = _keepAliveSubscription;
    await stopKeepAliveResources(
      closeRequests: () async {
        if (requests != null) await requests.close();
      },
      cancelSubscription: () async {
        if (subscription != null) await subscription.cancel();
      },
      clearReferences: () {
        _keepAliveRequests = null;
        _keepAliveSubscription = null;
      },
    );
  }
}

Uri _parseEndpoint(String endpoint) {
  final uri = Uri.tryParse(
    endpoint.contains('://') ? endpoint : 'http://$endpoint',
  );
  if (uri == null ||
      (uri.scheme != 'http' && uri.scheme != 'https') ||
      uri.host.isEmpty ||
      uri.userInfo.isNotEmpty ||
      uri.path.isNotEmpty && uri.path != '/' ||
      uri.hasQuery ||
      uri.hasFragment) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'etcd endpoint must be an http(s) authority without credentials or path.',
    );
  }
  return uri;
}

void _validateOptions(EtcdProviderOptions options) {
  resourceId(options.namespace, 'configuration-validation');
  final epoch = utf8.encode(options.coordinationEpoch);
  if (epoch.isEmpty ||
      epoch.length > 1024 ||
      options.coordinationEpoch.contains('\u0000')) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'Coordination epoch must be 1 through 1024 UTF-8 bytes without U+0000.',
    );
  }
  if (options.rpcTimeout <= Duration.zero ||
      options.cleanupTimeout <= Duration.zero ||
      options.rpcTimeout > _maxTtl ||
      options.cleanupTimeout > _maxTtl) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'RPC and cleanup timeouts must be positive and at most one day.',
    );
  }
  options.defaultAcquisition.validate();
}

DilockException _backendFailure(String message, GrpcError cause) {
  return DilockException(FailureKind.backendUnavailable, message, cause: cause);
}

Duration _remaining(Duration total, Stopwatch elapsed, Duration perRpc) {
  final left = total - elapsed.elapsed;
  if (left <= Duration.zero) {
    throw const DilockException(
      FailureKind.waitTimeout,
      'etcd acquisition budget elapsed.',
    );
  }
  return left < perRpc ? left : perRpc;
}

Duration _margin(Duration ttl) {
  final onePercent = ttl ~/ 100;
  return onePercent > const Duration(milliseconds: 10)
      ? onePercent
      : const Duration(milliseconds: 10);
}

int _intField(dynamic message, int field) {
  final value = message.getField(field);
  if (value is Int64) return value.toInt();
  if (value is int) return value;
  throw const DilockException(
    FailureKind.protocolMismatch,
    'etcd protobuf integer field has an unexpected type.',
  );
}

List<int> _bytesField(dynamic message, int field) {
  final value = message.getField(field);
  if (value is List<int>) return value;
  throw const DilockException(
    FailureKind.protocolMismatch,
    'etcd protobuf bytes field has an unexpected type.',
  );
}

List<dynamic> _values(Object? value) =>
    value is List<dynamic> ? value : const <dynamic>[];

bool _sameBytes(List<int> left, List<int> right) {
  if (left.length != right.length) return false;
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) return false;
  }
  return true;
}

/// grpc-dart can surface a locally expired Lock deadline as UNKNOWN when the
/// peer resets the HTTP/2 stream with CANCEL instead of returning status 4.
/// That is only a contention result after the caller's lease was revoked and
/// a linearizable Range confirms a competing native lock key.
bool _isNativeLockWaitDeadline(GrpcError error) =>
    error.code == 4 ||
    error.code == 1 ||
    (error.code == 2 &&
        (error.message?.contains(
              'Stream was terminated by peer (errorCode: 8)',
            ) ??
            false));

List<int> _prefixEnd(List<int> prefix) {
  final end = List<int>.of(prefix);
  for (var i = end.length - 1; i >= 0; i--) {
    if (end[i] < 0xff) {
      end[i]++;
      return end.sublist(0, i + 1);
    }
  }
  return <int>[0];
}
