import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:dilock/dilock.dart';
// The Redis and etcd packages share this internal contract within the monorepo.
// ignore: implementation_imports
import 'package:dilock/src/metadata_contract.dart';
// ignore: implementation_imports
import 'package:dilock/src/lease_scope.dart';

part 'resp.dart';

const _releaseScript = '''
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
''';

const _renewScript = '''
local ttl = tonumber(ARGV[2])
if not ttl or ttl ~= math.floor(ttl) or ttl <= 0 or ttl > 86400000 then
    return redis.error_reply('DILOCK_INVALID_TTL')
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ttl)
end
return 0
''';

/// Options for one reusable Redis authority and its connection pool.
final class RedisProviderOptions {
  /// Creates provider configuration. Namespace defaults to `default`;
  /// coordination epoch must be deployment-approved and explicitly supplied.
  const RedisProviderOptions({
    this.namespace = 'default',
    required this.coordinationEpoch,
    this.connectTimeout = const Duration(seconds: 2),
    this.defaultAcquisition = const AcquireOptions(),
    this.maxConnections = 8,
    this.cleanupObserver,
  });

  /// Shared draft 1 namespace.
  final String namespace;

  /// Stable deployment epoch matching the pre-provisioned namespace metadata.
  final String coordinationEpoch;

  /// Maximum budget for one TCP/TLS connection attempt.
  final Duration connectTimeout;

  /// Immutable policy captured by new handle acquisition methods.
  final AcquireOptions defaultAcquisition;

  /// Maximum concurrent pooled sockets for this provider.
  final int maxConnections;

  /// Optional safe notification for uncertain best-effort scope cleanup.
  final void Function(CleanupObservation observation)? cleanupObserver;
}

/// One configured Redis endpoint, namespace, epoch, and reusable bounded pool.
///
/// This SDK uses standalone Redis commands and bundled Lua scripts. It does not
/// guarantee fencing across failover. Closing the provider first withdraws and
/// owner-cleans active leases within their cleanup budgets, waits for tracked
/// orphan cleanup, then closes pooled sockets.
final class RedisProvider {
  RedisProvider._(this._pool, this.options);

  final _RedisPool _pool;

  /// Immutable options supplied when this provider was connected.
  final RedisProviderOptions options;
  final Set<RedisLease> _leases = <RedisLease>{};
  final Set<Future<void>> _cleanupTasks = <Future<void>>{};
  bool _closed = false;
  Future<void>? _closeFuture;
  int _activeOperations = 0;
  Completer<void>? _operationsDone;

  /// Opens one standalone `redis://` or `rediss://` authority, verifies PING
  /// and existing namespace metadata, and owns the resulting connection pool.
  ///
  /// The server must permit the metadata `GET`, lock `SET NX PX`, `TYPE`, and
  /// `EVAL` commands. Runtime initialization never creates or repairs metadata.
  /// TLS certificate verification stays enabled for `rediss://` URLs.
  static Future<RedisProvider> connect(
    String endpoint, {
    required RedisProviderOptions options,
  }) async {
    final providerOptions = options;
    _validateProviderOptions(providerOptions);
    final uri = _parseEndpoint(endpoint);
    final pool = _RedisPool(uri, providerOptions);
    final provider = RedisProvider._(pool, providerOptions);
    try {
      final ping = await pool.command(<Object?>[
        'PING',
      ], providerOptions.connectTimeout);
      if (ping != 'PONG') {
        throw const DilockException(
          FailureKind.protocolMismatch,
          'Redis PING returned an unexpected reply.',
        );
      }
      await provider._validateMetadata(
          providerOptions.defaultAcquisition.lease.rpcTimeout);
      return provider;
    } on DilockException {
      await pool.close();
      rethrow;
    } on Object catch (error) {
      await pool.close();
      throw DilockException(
        FailureKind.backendUnavailable,
        'Redis initialization failed.',
        cause: error,
      );
    }
  }

  /// Creates a reusable validated resource handle without network activity.
  RedisHandle lock(String resource) {
    _ensureOpen();
    return RedisHandle._(this, redisKey(options.namespace, resource));
  }

  /// Waits for background owner-specific cleanup tasks created after ambiguity.
  Future<void> waitForCleanup() async {
    while (_cleanupTasks.isNotEmpty) {
      await Future.wait(List<Future<void>>.of(_cleanupTasks));
    }
  }

  /// Closes the owned pool after active operations and lease cleanup finish.
  Future<void> close() => _closeFuture ??= _close();

  Future<void> _close() async {
    _closed = true;
    final operationsDone = _operationsDone;
    if (_activeOperations > 0 && operationsDone != null) {
      await operationsDone.future;
    }
    for (final lease in List<RedisLease>.of(_leases)) {
      final outcome = await lease.release(reason: 'provider_shutdown');
      _observe(outcome, 'provider_shutdown');
    }
    await waitForCleanup();
    await _pool.close();
  }

  void _beginOperation() {
    _ensureOpen();
    if (_activeOperations++ == 0) _operationsDone = Completer<void>();
  }

  void _endOperation() {
    if (--_activeOperations == 0) {
      _operationsDone?.complete();
      _operationsDone = null;
    }
  }

  void _register(RedisLease lease) => _leases.add(lease);

  void _remove(RedisLease lease) => _leases.remove(lease);

  void _ensureOpen() {
    if (_closed) {
      throw const DilockException(
        FailureKind.providerClosed,
        'Redis provider is closed.',
      );
    }
  }

  Future<void> _validateMetadata(
    Duration timeout, {
    CancellationToken? cancellationToken,
  }) async {
    final Object? raw;
    try {
      raw = await _pool.command(<Object?>[
        'GET',
        'dilock:namespace:${options.namespace}:meta',
      ], timeout, cancellationToken: cancellationToken);
    } on DilockException {
      rethrow;
    } on _RedisTransportException catch (error) {
      if (cancellationToken?.isCancelled ?? false) {
        throw const DilockException(
          FailureKind.cancelled,
          'Redis acquisition was cancelled during metadata validation.',
        );
      }
      throw DilockException(
        FailureKind.backendUnavailable,
        'Redis namespace metadata read failed.',
        cause: error,
      );
    } on Object catch (error) {
      throw DilockException(
        FailureKind.backendUnavailable,
        'Redis namespace metadata read failed.',
        cause: error,
      );
    }
    if (raw is! List<int> || raw.isEmpty || raw.length > 8192) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Redis namespace metadata is missing or invalid.',
      );
    }
    late final Map<String, dynamic> metadata;
    try {
      final decoded = jsonDecode(utf8.decode(raw, allowMalformed: false));
      if (decoded is! Map<String, dynamic>) {
        throw const FormatException('metadata must be an object');
      }
      metadata = decoded;
    } on Object catch (error) {
      throw DilockException(
        FailureKind.protocolMismatch,
        'Redis namespace metadata is not valid JSON.',
        cause: error,
      );
    }
    validateNamespaceMetadata(
      metadata,
      profile: 'redis-single-authority-v1',
      coordinationEpoch: options.coordinationEpoch,
    );
  }

  Future<ReleaseOutcome> _cleanupOwner(
    String key,
    String owner,
    Duration timeout,
  ) async {
    try {
      final reply = await _pool.command(<Object?>[
        'EVAL',
        _releaseScript,
        '1',
        key,
        owner,
      ], timeout);
      if (reply == 1) return ReleaseOutcome.released;
      if (reply == 0) return ReleaseOutcome.alreadyNotOwned;
      return ReleaseOutcome.unknown;
    } on Object {
      return ReleaseOutcome.unknown;
    }
  }

  void _trackCleanup(
    Future<ReleaseOutcome> Function() cleanup, {
    required String reason,
  }) {
    late final Future<void> task;
    task = Future<void>(() async {
      try {
        final outcome = await cleanup();
        _observe(outcome, reason);
      } catch (_) {
        // Diagnostics report uncertainty; the bounded server-side TTL is fallback.
        _observe(ReleaseOutcome.unknown, reason);
      } finally {
        _cleanupTasks.remove(task);
      }
    });
    _cleanupTasks.add(task);
  }

  void _observe(ReleaseOutcome outcome, String reason) {
    if (outcome != ReleaseOutcome.unknown) return;
    final observation = CleanupObservation(
      backend: 'redis',
      outcome: outcome,
      reason: reason,
    );
    try {
      options.cleanupObserver?.call(observation);
    } on Object {
      // Diagnostics must never replace user work or cleanup results.
    }
  }
}

/// Reusable identity handle owned by a Redis provider.
final class RedisHandle {
  const RedisHandle._(this._provider, this.key);

  final RedisProvider _provider;

  /// Canonical draft 1 Redis key; contains only the identity digest.
  final String key;

  /// Performs one owner-specific Redis `SET NX PX` attempt.
  ///
  /// Only an acknowledged `SET NX` conflict returns `null`. Backend errors,
  /// cancellation after dispatch, and missing acknowledgements throw a typed
  /// failure. The acquired lease must be explicitly released or scoped.
  Future<RedisLease?> tryAcquire({
    LeaseOptions options = const LeaseOptions(),
    CancellationToken? cancellationToken,
  }) =>
      _tryAcquire(
        options: options,
        cancellationToken: cancellationToken,
        totalBudget: options.rpcTimeout,
        waitBudgetLimited: false,
      );

  Future<RedisLease?> _tryAcquire({
    required LeaseOptions options,
    required CancellationToken? cancellationToken,
    required Duration totalBudget,
    required bool waitBudgetLimited,
  }) async {
    _provider._beginOperation();
    final attempt = Stopwatch()..start();
    Duration remainingBudget() => totalBudget - attempt.elapsed;
    DilockException exhaustedBudget() => DilockException(
          waitBudgetLimited
              ? FailureKind.waitTimeout
              : FailureKind.backendUnavailable,
          waitBudgetLimited
              ? 'Redis acquisition wait budget elapsed before ownership was confirmed.'
              : 'Redis attempt RPC budget elapsed before ownership was confirmed.',
        );
    try {
      options.validate();
      if (options.requireFencing) {
        throw const DilockException(
          FailureKind.unsupportedCapability,
          'Redis draft 1 has no strong fencing token.',
        );
      }
      cancellationToken?.throwIfCancelled();
      final metadataBudget = remainingBudget();
      if (metadataBudget <= Duration.zero) throw exhaustedBudget();
      try {
        await _provider._validateMetadata(
          metadataBudget,
          cancellationToken: cancellationToken,
        );
      } on DilockException catch (error) {
        if (cancellationToken?.isCancelled ?? false) {
          throw const DilockException(
            FailureKind.cancelled,
            'Redis acquisition was cancelled during metadata validation.',
          );
        }
        if (_redisErrorCausedByTimeout(error) &&
            remainingBudget() <= Duration.zero) {
          throw exhaustedBudget();
        }
        rethrow;
      }
      cancellationToken?.throwIfCancelled();

      final ownerBytes = List<int>.generate(
        32,
        (_) => Random.secure().nextInt(256),
      );
      final owner =
          'dilock/1/${ownerBytes.map((value) => value.toRadixString(16).padLeft(2, '0')).join()}';
      final setBudget = remainingBudget();
      if (setBudget <= Duration.zero) throw exhaustedBudget();
      final stopwatch = Stopwatch()..start();
      Object? reply;
      try {
        reply = await _provider._pool.command(
          <Object?>[
            'SET',
            key,
            owner,
            'NX',
            'PX',
            options.leaseDuration.inMilliseconds.toString(),
          ],
          setBudget,
          cancellationToken: cancellationToken,
        );
      } on RedisServerException catch (error) {
        throw DilockException(
          FailureKind.backendUnavailable,
          'Redis rejected the acquisition command.',
          cause: error,
        );
      } on _RedisTransportException catch (error) {
        if (!error.dispatched) {
          throw DilockException(
            FailureKind.backendUnavailable,
            'Redis connection or command setup failed before SET was dispatched.',
            cause: error.cause,
          );
        }
        _provider._trackCleanup(
          () => _provider._cleanupOwner(key, owner, options.cleanupTimeout),
          reason: 'acquisition_unknown',
        );
        throw DilockException(
          FailureKind.acquisitionUnknown,
          'Redis SET may have been dispatched without an ownership acknowledgement.',
          cause: error.cause,
          outcomeUnknown: true,
        );
      } on DilockException catch (error) {
        // Cancellation while waiting for a pooled connection is known to have
        // happened before SET dispatch, so preserve the cancellation outcome.
        if (_redisErrorCausedByTimeout(error) &&
            waitBudgetLimited &&
            remainingBudget() <= Duration.zero) {
          throw exhaustedBudget();
        }
        rethrow;
      } on TimeoutException catch (error) {
        if (waitBudgetLimited && remainingBudget() <= Duration.zero) {
          throw exhaustedBudget();
        }
        throw DilockException(
          FailureKind.backendUnavailable,
          'Redis connection pool did not provide a connection within the RPC budget.',
          cause: error,
        );
      } on Object catch (error) {
        throw DilockException(
          FailureKind.backendUnavailable,
          'Redis SET could not be dispatched.',
          cause: error,
        );
      }

      // Cancellation may race with delivery of a positive SET response after
      // the command future has already completed. Acknowledged ownership must
      // be cleaned up before the caller is told the operation was cancelled.
      if ((cancellationToken?.isCancelled ?? false) && reply == 'OK') {
        _provider._trackCleanup(
          () => _provider._cleanupOwner(key, owner, options.cleanupTimeout),
          reason: 'cancelled_after_set_acknowledgement',
        );
        throw redisCancellationAfterSetResponse(
          ownershipAcknowledged: true,
        );
      }
      if ((cancellationToken?.isCancelled ?? false) && reply == null) {
        throw redisCancellationAfterSetResponse(
          ownershipAcknowledged: false,
        );
      }

      if (reply == null) {
        cancellationToken?.throwIfCancelled();
        final typeBudget = remainingBudget();
        if (typeBudget <= Duration.zero) throw exhaustedBudget();
        final Object? keyType;
        try {
          keyType = await _provider._pool.command(
            <Object?>['TYPE', key],
            typeBudget,
            cancellationToken: cancellationToken,
          );
        } on _RedisTransportException catch (error) {
          if (cancellationToken?.isCancelled ?? false) {
            throw const DilockException(
              FailureKind.cancelled,
              'Redis acquisition was cancelled during contention verification.',
            );
          }
          if (_redisErrorCausedByTimeout(error) &&
              remainingBudget() <= Duration.zero &&
              waitBudgetLimited) {
            throw exhaustedBudget();
          }
          throw DilockException(
            FailureKind.backendUnavailable,
            'Redis contention key type could not be verified.',
            cause: error,
          );
        } on DilockException catch (error) {
          if (cancellationToken?.isCancelled ?? false) {
            throw const DilockException(
              FailureKind.cancelled,
              'Redis acquisition was cancelled during contention verification.',
            );
          }
          if (_redisErrorCausedByTimeout(error) &&
              remainingBudget() <= Duration.zero &&
              waitBudgetLimited) {
            throw exhaustedBudget();
          }
          rethrow;
        } on TimeoutException catch (error) {
          if (waitBudgetLimited && remainingBudget() <= Duration.zero) {
            throw exhaustedBudget();
          }
          throw DilockException(
            FailureKind.backendUnavailable,
            'Redis contention verification exceeded its RPC budget.',
            cause: error,
          );
        }
        cancellationToken?.throwIfCancelled();
        if (remainingBudget() <= Duration.zero) throw exhaustedBudget();
        if (keyType != 'string' && keyType != 'none') {
          throw const DilockException(
            FailureKind.protocolMismatch,
            'Redis lock key has a non-string type.',
          );
        }
        return null;
      }
      if (reply != 'OK' ||
          stopwatch.elapsed >= options.leaseDuration - options.safetyMargin) {
        _provider._trackCleanup(
          () => _provider._cleanupOwner(key, owner, options.cleanupTimeout),
          reason: 'late_acquisition_acknowledgement',
        );
        throw const DilockException(
          FailureKind.acquisitionUnknown,
          'Redis acknowledged ownership too late to authorize protected work.',
          outcomeUnknown: true,
        );
      }

      final initialValidity =
          options.leaseDuration - options.safetyMargin - stopwatch.elapsed;
      if (initialValidity <= Duration.zero) {
        _provider._trackCleanup(
          () => _provider._cleanupOwner(key, owner, options.cleanupTimeout),
          reason: 'acquisition_validity_exhausted',
        );
        throw const DilockException(
          FailureKind.acquisitionUnknown,
          'Redis reply left no conservative validity budget.',
          outcomeUnknown: true,
        );
      }
      final lease = RedisLease._(
        _provider,
        key,
        owner,
        options,
        initialValidity,
      );
      _provider._register(lease);
      return lease;
    } finally {
      _provider._endOperation();
    }
  }

  /// Waits only by retrying acknowledged contention until `wait` expires.
  Future<RedisLease> acquire(
    Duration wait, {
    LeaseOptions options = const LeaseOptions(),
    CancellationToken? cancellationToken,
  }) async {
    final policy = AcquireOptions(wait: wait, lease: options)..validate();
    final elapsed = Stopwatch()..start();
    while (true) {
      cancellationToken?.throwIfCancelled();
      final remaining = policy.wait - elapsed.elapsed;
      if (remaining <= Duration.zero) {
        throw const DilockException(
          FailureKind.waitTimeout,
          'Redis contention wait budget elapsed.',
        );
      }
      final lease = await _tryAcquire(
        options: options,
        cancellationToken: cancellationToken,
        totalBudget:
            options.rpcTimeout < remaining ? options.rpcTimeout : remaining,
        waitBudgetLimited: remaining <= options.rpcTimeout,
      );
      if (lease != null) {
        if (elapsed.elapsed >= policy.wait) {
          final outcome = await lease.release(reason: 'wait_deadline_elapsed');
          if (outcome == ReleaseOutcome.unknown) {
            throw const DilockException(
              FailureKind.acquisitionUnknown,
              'Acquired after the wait budget; cleanup acknowledgement is unknown.',
              outcomeUnknown: true,
            );
          }
          throw const DilockException(
            FailureKind.waitTimeout,
            'Redis contention wait budget elapsed.',
          );
        }
        return lease;
      }
      final left = policy.wait - elapsed.elapsed;
      if (left <= Duration.zero) {
        throw const DilockException(
          FailureKind.waitTimeout,
          'Redis contention wait budget elapsed.',
        );
      }
      final jitterMs = 20 + Random.secure().nextInt(31);
      final delay = Duration(milliseconds: jitterMs) < left
          ? Duration(milliseconds: jitterMs)
          : left;
      if (cancellationToken == null) {
        await Future<void>.delayed(delay);
      } else {
        await Future.any<void>(<Future<void>>[
          Future<void>.delayed(delay),
          cancellationToken.whenCancelled.then(
            (_) => cancellationToken.throwIfCancelled(),
          ),
        ]);
      }
    }
  }

  /// Acquires with the provider's immutable default policy.
  Future<RedisLease> acquireDefault({CancellationToken? cancellationToken}) =>
      acquire(
        _provider.options.defaultAcquisition.wait,
        options: _provider.options.defaultAcquisition.lease,
        cancellationToken: cancellationToken,
      );

  /// Runs one callback under the provider defaults and awaits owner release.
  ///
  /// The body exception is preserved if it throws. Unknown cleanup is reported
  /// to the provider observer (if provided) and does not replace the body error.
  Future<T> withLease<T>(
    Future<T> Function(RedisLease lease) work, {
    CancellationToken? cancellationToken,
  }) =>
      withLeaseOptions(
        work,
        _provider.options.defaultAcquisition,
        cancellationToken: cancellationToken,
      );

  /// Runs one callback under a complete explicit wait/lease policy.
  ///
  /// If the callback succeeds, an unknown release acknowledgement throws
  /// [DilockException] with [FailureKind.releaseUnknown]. If Redis confirms
  /// that this owner no longer holds the key, it throws [FailureKind.leaseLost]
  /// because the callback may have outlived its lease. A callback exception
  /// remains primary if cleanup also fails.
  Future<T> withLeaseOptions<T>(
    Future<T> Function(RedisLease lease) work,
    AcquireOptions policy, {
    CancellationToken? cancellationToken,
  }) async {
    policy.validate();
    final lease = await acquire(
      policy.wait,
      options: policy.lease,
      cancellationToken: cancellationToken,
    );
    return runLeaseScope<T>(
      work: () => work(lease),
      cleanup: ({required bool bodyFailed}) async {
        final reason = bodyFailed ? 'body_failed' : 'scope_exit';
        final wasLost = lease.state == LeaseState.lost;
        final outcome = await lease.release(reason: reason);
        _provider._observe(outcome, reason);
        if (bodyFailed) return;
        if (wasLost || outcome == ReleaseOutcome.alreadyNotOwned) {
          throw const DilockException(
            FailureKind.leaseLost,
            'Redis no longer confirms this owner at scope exit; protected work may have outlived its lease.',
          );
        }
        if (outcome == ReleaseOutcome.unknown) {
          throw const DilockException(
            FailureKind.releaseUnknown,
            'Redis release may have executed, but cleanup acknowledgement was not confirmed.',
            outcomeUnknown: true,
          );
        }
      },
    );
  }
}

/// Classifies cancellation observed after the Redis SET response is delivered.
///
/// A positive acknowledgement makes ownership possible and requires owner
/// cleanup; until it is acknowledged the result remains unknown. A null SET
/// response is confirmed contention, so cancellation is known.
DilockException redisCancellationAfterSetResponse({
  required bool ownershipAcknowledged,
}) =>
    ownershipAcknowledged
        ? const DilockException(
            FailureKind.acquisitionUnknown,
            'Cancellation raced with an acknowledged Redis SET; owner cleanup was scheduled.',
            outcomeUnknown: true,
          )
        : const DilockException(
            FailureKind.cancelled,
            'Redis acquisition was cancelled after a non-owning response.',
          );

/// One non-copyable-by-convention Redis ownership generation.
///
/// Dart does not provide deterministic destruction; callers must use
/// [release] or [RedisHandle.withLease]. A dropped reference leaves the server
/// TTL as fallback. State and expiry use a monotonic stopwatch. Mutating
/// operations are serialized within this object; callers must still check
/// [state] before starting protected work after awaits.
final class RedisLease {
  RedisLease._(
    this._provider,
    this.key,
    this._owner,
    this.options,
    Duration initialValidity,
  )   : _validity = Stopwatch()..start(),
        _deadline = initialValidity {
    if (options.autoRenew) _scheduleRenewal();
  }

  final RedisProvider _provider;

  /// Canonical Redis lease key.
  final String key;

  final String _owner;

  /// Complete policy used by this acquisition generation.
  final LeaseOptions options;

  final Stopwatch _validity;
  LeaseState _state = LeaseState.held;
  String? _lossReason;
  Duration _deadline = Duration.zero;
  Timer? _renewTimer;
  Future<void> _tail = Future<void>.value();
  Future<ReleaseOutcome>? _releaseFuture;

  /// Current local state; expiration changes it monotonically to `lost`.
  LeaseState get state {
    _expireIfNeeded();
    return _state;
  }

  /// Reason for local loss, or null while no loss has been observed.
  String? get lossReason => _lossReason;

  /// Server TTL requested by this generation, not remaining local validity.
  Duration get effectiveLease => options.leaseDuration;

  /// Rejects protected work after conservative local validity ends.
  void ensureHeld() {
    _expireIfNeeded();
    if (_state != LeaseState.held) {
      throw const DilockException(
        FailureKind.leaseLost,
        'Redis lease is no longer locally held.',
      );
    }
  }

  /// Extends this owner's TTL atomically; no response after the old deadline
  /// can restore local permission.
  Future<bool> renew() => _serialize<bool>(() async {
        _expireIfNeeded();
        if (_state != LeaseState.held) return false;
        final sent = _validity.elapsed;
        final oldDeadline = _deadline;
        final remaining = oldDeadline - sent;
        if (remaining <= Duration.zero) {
          _markLost('conservative_deadline_elapsed');
          return false;
        }
        final budget =
            options.rpcTimeout < remaining ? options.rpcTimeout : remaining;
        try {
          final result = await _provider._pool.command(<Object?>[
            'EVAL',
            _renewScript,
            '1',
            key,
            _owner,
            options.leaseDuration.inMilliseconds.toString(),
          ], budget);
          final deadline = sent + options.leaseDuration - options.safetyMargin;
          if (result != 1 ||
              _validity.elapsed >= oldDeadline ||
              _validity.elapsed >= deadline) {
            _markLost('renewal_unconfirmed_or_not_owned');
            _provider._trackCleanup(
              () => _provider._cleanupOwner(
                key,
                _owner,
                options.cleanupTimeout,
              ),
              reason: 'renewal_unconfirmed_or_not_owned',
            );
            return false;
          }
          _deadline = deadline;
          return true;
        } on Object catch (error) {
          _markLost('renewal_unconfirmed');
          _provider._trackCleanup(
            () => _provider._cleanupOwner(
              key,
              _owner,
              options.cleanupTimeout,
            ),
            reason: 'renewal_unconfirmed',
          );
          throw DilockException(
            FailureKind.backendUnavailable,
            'Redis renewal failed; local lease is lost.',
            cause: error,
          );
        }
      });

  /// Releases only if this exact generation still owns the key.
  ///
  /// Cleanup uses a fresh budget independent of acquisition cancellation.
  /// Transport errors return [ReleaseOutcome.unknown], never success. Repeated
  /// calls share and return the first cleanup observation.
  Future<ReleaseOutcome> release({String reason = 'explicit_release'}) {
    final existing = _releaseFuture;
    if (existing != null) return existing;
    _renewTimer?.cancel();
    if (_state == LeaseState.held) _state = LeaseState.releasing;
    final future = _serialize<ReleaseOutcome>(() async {
      final outcome = await _provider._cleanupOwner(
        key,
        _owner,
        options.cleanupTimeout,
      );
      _state = outcome == ReleaseOutcome.unknown
          ? LeaseState.releasedUnknown
          : LeaseState.released;
      if (outcome == ReleaseOutcome.unknown) _lossReason ??= reason;
      _provider._remove(this);
      return outcome;
    });
    _releaseFuture = future;
    return future;
  }

  Future<T> _serialize<T>(Future<T> Function() action) {
    final result = _tail.then((_) => action());
    _tail = result.then<void>((_) {}, onError: (Object _, StackTrace __) {});
    return result;
  }

  void _expireIfNeeded() {
    if (_state == LeaseState.held && _validity.elapsed >= _deadline) {
      _markLost('conservative_deadline_elapsed');
      _provider._trackCleanup(
        () => _provider._cleanupOwner(key, _owner, options.cleanupTimeout),
        reason: 'conservative_deadline_elapsed',
      );
    }
  }

  void _markLost(String reason) {
    if (_state == LeaseState.held) {
      _state = LeaseState.lost;
      _lossReason = reason;
      _renewTimer?.cancel();
    }
  }

  void _scheduleRenewal() {
    final remaining = _deadline - _validity.elapsed;
    if (remaining <= Duration.zero) {
      _markLost('conservative_deadline_elapsed');
      return;
    }
    final delay = options.leaseDuration ~/ 3;
    _renewTimer = Timer(delay < remaining ? delay : remaining, () async {
      if (_state != LeaseState.held) return;
      try {
        if (await renew() && _state == LeaseState.held) _scheduleRenewal();
      } on Object {
        // renew() already transitions to Lost and records cleanup.
      }
    });
  }
}

void _validateProviderOptions(RedisProviderOptions options) {
  resourceId(options.namespace, 'validation');
  if (options.coordinationEpoch.isEmpty ||
      utf8.encode(options.coordinationEpoch).length > 1024 ||
      options.coordinationEpoch.contains('\u0000')) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'A stable valid coordination epoch is required.',
    );
  }
  if (options.connectTimeout <= Duration.zero ||
      options.maxConnections < 1 ||
      options.maxConnections > 256) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'Connect timeout and pool size are invalid.',
    );
  }
  options.defaultAcquisition.validate();
  if (options.defaultAcquisition.lease.requireFencing) {
    throw const DilockException(FailureKind.unsupportedCapability,
        'Redis draft 1 cannot provide strong fencing.');
  }
}

Uri _parseEndpoint(String value) {
  late final Uri uri;
  try {
    uri = Uri.parse(value);
  } on FormatException catch (error) {
    throw DilockException(
      FailureKind.invalidOptions,
      'Invalid Redis URL.',
      cause: error,
    );
  }
  final validPath = uri.path.isEmpty ||
      uri.path == '/' ||
      RegExp(r'^/[0-9]+$').hasMatch(uri.path);
  if (!<String>{'redis', 'rediss'}.contains(uri.scheme) ||
      uri.host.isEmpty ||
      uri.userInfo.contains('@') ||
      !validPath ||
      uri.hasQuery ||
      uri.hasFragment) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'Redis URL must select one redis:// or rediss:// standalone authority.',
    );
  }
  if (uri.path.length > 1 && int.tryParse(uri.path.substring(1)) == null) {
    throw const DilockException(
      FailureKind.invalidOptions,
      'Redis database path must be an integer.',
    );
  }
  return uri;
}

final class _RedisPool {
  _RedisPool(this.uri, this.options);

  final Uri uri;
  final RedisProviderOptions options;
  final List<_RedisConnection> _idle = <_RedisConnection>[];
  final List<Completer<void>> _waiters = <Completer<void>>[];
  int _opened = 0;
  bool _closing = false;

  Future<Object?> command(
    List<Object?> args,
    Duration timeout, {
    CancellationToken? cancellationToken,
  }) async {
    final connection = await _borrow(timeout, cancellationToken);
    var reusable = true;
    try {
      final command = connection.execute(args);
      final reply = await _awaitCommand(command, timeout, cancellationToken);
      return reply;
    } on _RedisCommandCancelled catch (error) {
      // The command may already have committed. Never return a connection with
      // an unread response to the pool: the next borrower could consume it as
      // its own reply. Closing the socket also bounds the abandoned read.
      reusable = false;
      connection.close();
      throw _RedisTransportException(true, error);
    } on RedisServerException {
      rethrow;
    } on _RedisTransportException {
      reusable = false;
      rethrow;
    } on Object catch (error) {
      reusable = false;
      throw _RedisTransportException(true, error);
    } finally {
      _return(connection, reusable);
    }
  }

  Future<_RedisConnection> _borrow(
    Duration timeout,
    CancellationToken? token,
  ) async {
    final elapsed = Stopwatch()..start();
    while (true) {
      token?.throwIfCancelled();
      if (_closing) {
        throw const DilockException(
          FailureKind.providerClosed,
          'Redis connection pool is closing.',
        );
      }
      if (_idle.isNotEmpty) return _idle.removeLast();
      if (_opened < options.maxConnections) {
        _opened++;
        try {
          final connection = await _open(timeout - elapsed.elapsed);
          return connection;
        } on Object {
          _opened--;
          _wakeWaiter();
          rethrow;
        }
      }
      final left = timeout - elapsed.elapsed;
      if (left <= Duration.zero) {
        throw TimeoutException(
          'Redis connection pool wait exceeded its RPC budget.',
        );
      }
      final waiter = Completer<void>();
      _waiters.add(waiter);
      try {
        final futures = <Future<void>>[waiter.future.timeout(left)];
        if (token != null) {
          futures.add(
            token.whenCancelled.then((_) => token.throwIfCancelled()),
          );
        }
        await Future.any<void>(futures);
      } finally {
        _waiters.remove(waiter);
      }
    }
  }

  Future<_RedisConnection> _open(Duration timeout) async {
    if (timeout <= Duration.zero) {
      throw TimeoutException('Redis connection budget elapsed.');
    }
    final elapsed = Stopwatch()..start();
    final host = uri.host;
    final port = uri.hasPort ? uri.port : 6379;
    final opening = uri.scheme == 'rediss'
        ? SecureSocket.connect(host, port)
        : Socket.connect(host, port);
    late final Socket socket;
    try {
      socket = await opening.timeout(timeout);
    } on Object catch (error) {
      unawaited(
        opening.then<void>(
          (socket) => socket.destroy(),
          onError: (Object _) {},
        ),
      );
      throw DilockException(
        FailureKind.backendUnavailable,
        'Redis connection failed.',
        cause: error,
      );
    }
    final connection = _RedisConnection(socket);
    try {
      Duration remaining() {
        final budget = timeout - elapsed.elapsed;
        if (budget <= Duration.zero) {
          throw TimeoutException('Redis connection budget elapsed.');
        }
        return budget;
      }

      if (uri.userInfo.isNotEmpty) {
        final credentials = uri.userInfo.split(':');
        if (credentials.length == 1) {
          await connection.execute(<Object?>[
            'AUTH',
            Uri.decodeComponent(credentials.first),
          ]).timeout(remaining());
        } else {
          await connection.execute(<Object?>[
            'AUTH',
            Uri.decodeComponent(credentials.first),
            Uri.decodeComponent(credentials.skip(1).join(':')),
          ]).timeout(remaining());
        }
      }
      if (uri.path.length > 1) {
        await connection.execute(
            <Object?>['SELECT', uri.path.substring(1)]).timeout(remaining());
      }
      return connection;
    } on Object catch (error) {
      connection.close();
      throw DilockException(
        FailureKind.backendUnavailable,
        'Redis connection authentication or database selection failed.',
        cause: error,
      );
    }
  }

  Future<Object?> _awaitCommand(
    Future<Object?> command,
    Duration timeout,
    CancellationToken? token,
  ) {
    final timed = command.timeout(timeout);
    if (token == null) return timed;
    return Future.any<Object?>(<Future<Object?>>[
      timed,
      token.whenCancelled.then<Object?>((_) {
        throw const _RedisCommandCancelled();
      }),
    ]);
  }

  void _return(_RedisConnection connection, bool reusable) {
    if (!reusable || _closing || !connection.isOpen) {
      connection.close();
      _opened--;
      _wakeWaiter();
      return;
    }
    _idle.add(connection);
    _wakeWaiter();
  }

  void _wakeWaiter() {
    while (_waiters.isNotEmpty) {
      final waiter = _waiters.removeAt(0);
      if (!waiter.isCompleted) {
        waiter.complete();
        return;
      }
    }
  }

  Future<void> close() async {
    _closing = true;
    for (final waiter in _waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
    for (final connection in _idle) {
      connection.close();
      _opened--;
    }
    _idle.clear();
  }
}

/// Cancellation won after a command was dispatched but before its reply.
final class _RedisCommandCancelled implements Exception {
  const _RedisCommandCancelled();
}

bool _redisErrorCausedByTimeout(Object? error) {
  var current = error;
  while (true) {
    if (current is TimeoutException) return true;
    if (current is _RedisTransportException) {
      current = current.cause;
      continue;
    }
    if (current is DilockException) {
      current = current.cause;
      continue;
    }
    return false;
  }
}

/// RESP server error, kept separate from transport ambiguity.
final class RedisServerException implements Exception {
  /// Creates a server rejection.
  const RedisServerException(this.message);

  /// Server-supplied safe command error text.
  final String message;

  @override
  String toString() => 'RedisServerException: $message';
}
