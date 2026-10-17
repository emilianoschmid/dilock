import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dilock/dilock.dart';
// The backend adapters share this metadata contract within the monorepo.
// ignore: implementation_imports
import 'package:dilock/src/metadata_contract.dart';

const _profile = 'consul-kv-session-v1';
const _minTtl = Duration(seconds: 10);
const _maxTtl = Duration(days: 1);

/// Configuration for one Consul agent authority.
final class ConsulProviderOptions {
  /// Creates immutable endpoint, identity and timing settings.
  const ConsulProviderOptions({
    this.namespace = 'default',
    required this.coordinationEpoch,
    this.token,
    this.rpcTimeout = const Duration(seconds: 2),
    this.cleanupTimeout = const Duration(seconds: 2),
    this.defaultAcquisition = const AcquireOptions(),
    this.cleanupObserver,
  });

  /// Pre-provisioned metadata namespace.
  final String namespace;

  /// Operator-approved incarnation used to scope fences.
  final String coordinationEpoch;

  /// Optional Consul ACL token. It is sent only in `X-Consul-Token`.
  final String? token;

  /// Bound for each HTTP request.
  final Duration rpcTimeout;

  /// Separate bounded budget for release and session destruction.
  final Duration cleanupTimeout;

  /// Default acquisition policy.
  final AcquireOptions defaultAcquisition;

  /// Called asynchronously when cleanup cannot be confirmed.
  final void Function(CleanupObservation observation)? cleanupObserver;
}

/// Consul fencing value. It is monotonic only while the lock KV key is retained.
final class ConsulFence {
  /// Creates an epoch-scoped LockIndex fence.
  const ConsulFence(this.coordinationEpoch, this.lockIndex);

  /// Operator-approved authority incarnation.
  final String coordinationEpoch;

  /// Consul's retained per-key acquisition index.
  final int lockIndex;
}

/// Owns a reusable HTTP client for a single Consul agent.
final class ConsulProvider {
  ConsulProvider._(this.endpoint, this.options, this._client);

  /// Validates existing namespace metadata before returning a provider.
  static Future<ConsulProvider> connect(
    String endpoint, {
    required ConsulProviderOptions options,
  }) async {
    _validateOptions(options);
    final uri = _parseEndpoint(endpoint);
    final provider = ConsulProvider._(uri, options, HttpClient());
    try {
      await provider._readMetadata(options.rpcTimeout);
      return provider;
    } on Object {
      provider._client.close(force: true);
      rethrow;
    }
  }

  /// Explicitly creates metadata only when the exact key is absent.
  static Future<void> provisionNamespace(
    String endpoint, {
    required ConsulProviderOptions options,
  }) async {
    _validateOptions(options);
    final provider = ConsulProvider._(
      _parseEndpoint(endpoint),
      options,
      HttpClient(),
    );
    try {
      final key = 'dilock/namespaces/${options.namespace}/meta';
      final metadata = <String, Object?>{
        'protocol_major': 1,
        'identity_scheme': 'sha256-framed-utf8-v1',
        'profile': _profile,
        'coordination_epoch': options.coordinationEpoch,
        'fixed_lease_ms': null,
      };
      final response = await provider._request(
        'PUT',
        '/v1/kv/$key',
        query: const {'cas': '0'},
        body: utf8.encode(jsonEncode(metadata)),
        timeout: options.rpcTimeout,
        mutation: true,
      );
      if (response == true) return;
      if (response != false) {
        throw const DilockException(
          FailureKind.protocolMismatch,
          'Consul metadata CAS response was not a boolean.',
        );
      }
      await provider._readMetadata(options.rpcTimeout);
    } finally {
      provider._client.close(force: true);
    }
  }

  /// Endpoint and immutable authority configuration.
  final Uri endpoint;

  /// Provider settings.
  final ConsulProviderOptions options;
  final HttpClient _client;
  final Set<ConsulLease> _leases = <ConsulLease>{};
  bool _closed = false;

  /// Creates a local resource handle without network activity.
  ConsulLock lock(String resource) {
    _ensureOpen();
    return ConsulLock._(this, resourceId(options.namespace, resource));
  }

  /// Closes active leases and this provider's HTTP client.
  Future<void> close() async {
    if (_closed) return;
    for (final lease in List<ConsulLease>.of(_leases)) {
      await lease.release(reason: 'provider_shutdown');
    }
    _closed = true;
    _client.close(force: true);
  }

  void _ensureOpen() {
    if (_closed) {
      throw const DilockException(
        FailureKind.providerClosed,
        'Consul provider is closed.',
      );
    }
  }

  Future<void> _readMetadata(Duration timeout) async {
    final key = 'dilock/namespaces/${options.namespace}/meta';
    final Object? value;
    try {
      value = await _request(
        'GET',
        '/v1/kv/$key',
        query: const {'consistent': ''},
        timeout: timeout,
      );
    } on _ConsulNotFound {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul namespace metadata is missing.',
      );
    }
    if (value is! List || value.length != 1 || value.single is! Map) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul namespace metadata is missing or not one exact key.',
      );
    }
    final row = value.single as Map;
    if (row['Key'] != key || row['Value'] is! String) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul returned malformed namespace metadata.',
      );
    }
    final List<int> raw;
    try {
      raw = base64.decode(row['Value'] as String);
    } on FormatException catch (error) {
      throw DilockException(
        FailureKind.protocolMismatch,
        'Consul metadata is not valid base64.',
        cause: error,
      );
    }
    if (raw.isEmpty || raw.length > 8192) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul metadata is empty or exceeds 8192 bytes.',
      );
    }
    final Object? decoded;
    try {
      decoded = jsonDecode(utf8.decode(raw, allowMalformed: false));
    } on Object catch (error) {
      throw DilockException(
        FailureKind.protocolMismatch,
        'Consul metadata is not valid UTF-8 JSON.',
        cause: error,
      );
    }
    if (decoded is! Map<String, dynamic>) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul metadata must be a JSON object.',
      );
    }
    if (decoded.length != 5) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul metadata must contain exactly the five profile fields.',
      );
    }
    validateNamespaceMetadata(
      decoded,
      profile: _profile,
      coordinationEpoch: options.coordinationEpoch,
    );
  }

  Future<(String?, int)> _readOwner(String key, Duration timeout) async {
    final Object? response;
    try {
      response = await _request(
        'GET',
        '/v1/kv/$key',
        query: const {'consistent': ''},
        timeout: timeout,
      );
    } on _ConsulNotFound {
      return (null, 0);
    }
    if (response is! List || response.length != 1 || response.single is! Map) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul KV owner response is malformed.',
      );
    }
    final row = response.single as Map;
    if (row['Key'] != key ||
        row['LockIndex'] is! int ||
        (row['Session'] != null && row['Session'] is! String)) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul KV owner fields are malformed.',
      );
    }
    return (row['Session'] as String?, row['LockIndex'] as int);
  }

  Future<Object?> _request(
    String method,
    String path, {
    Map<String, String> query = const {},
    List<int>? body,
    required Duration timeout,
    bool mutation = false,
  }) async {
    _ensureOpen();
    final uri = endpoint.replace(
      path: path,
      queryParameters: query.isEmpty ? null : query,
    );
    final Future<HttpClientRequest> opening = _client.openUrl(method, uri);
    HttpClientRequest request;
    final stopwatch = Stopwatch()..start();
    Duration remaining() {
      final value = timeout - stopwatch.elapsed;
      if (value <= Duration.zero) {
        throw TimeoutException('Consul request exceeded its total timeout.');
      }
      return value;
    }

    try {
      request = await opening.timeout(remaining());
      request.followRedirects = false;
      request.headers.set(HttpHeaders.acceptHeader, 'application/json');
      if (options.token != null) {
        request.headers.set('X-Consul-Token', options.token!);
      }
      if (body != null) {
        request.headers.contentType = ContentType.json;
        request.add(body);
      }
      final response = await request.close().timeout(remaining());
      final bytes = await response.fold<List<int>>(<int>[], (out, chunk) {
        if (out.length + chunk.length > 65536) {
          throw const FormatException('Consul response exceeds 64 KiB.');
        }
        return out..addAll(chunk);
      }).timeout(remaining());
      if (response.statusCode < 200 || response.statusCode >= 300) {
        if (response.statusCode == 404) {
          throw const _ConsulNotFound();
        }
        throw DilockException(
          mutation && response.statusCode >= 500
              ? FailureKind.acquisitionUnknown
              : FailureKind.backendUnavailable,
          'Consul HTTP request was rejected (${response.statusCode}).',
          outcomeUnknown: mutation && response.statusCode >= 500,
        );
      }
      if (bytes.isEmpty) return null;
      return jsonDecode(utf8.decode(bytes, allowMalformed: false));
    } on _ConsulNotFound {
      rethrow;
    } on DilockException {
      rethrow;
    } on Object catch (error) {
      if (mutation) {
        throw DilockException(
          FailureKind.acquisitionUnknown,
          'Consul mutation acknowledgement was not confirmed.',
          cause: error,
          outcomeUnknown: true,
        );
      }
      throw DilockException(
        FailureKind.backendUnavailable,
        'Consul HTTP request failed.',
        cause: error,
      );
    }
  }

  static Uri _parseEndpoint(String value) {
    final uri = Uri.tryParse(value);
    if (uri == null ||
        (uri.scheme != 'http' && uri.scheme != 'https') ||
        uri.host.isEmpty ||
        uri.userInfo.isNotEmpty ||
        (uri.path.isNotEmpty && uri.path != '/') ||
        uri.hasQuery ||
        uri.hasFragment) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Consul endpoint must be an http(s) authority without credentials or path.',
      );
    }
    return uri;
  }

  static void _validateOptions(ConsulProviderOptions options) {
    resourceId(options.namespace, 'validation');
    final epoch = utf8.encode(options.coordinationEpoch);
    if (epoch.isEmpty ||
        epoch.length > 1024 ||
        options.coordinationEpoch.contains('\u0000')) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Coordination epoch must contain 1 through 1024 UTF-8 bytes.',
      );
    }
    if (options.token != null &&
        (options.token!.isEmpty ||
            !RegExp(r'^[!-~]+$').hasMatch(options.token!))) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Consul token must contain printable non-space ASCII only.',
      );
    }
    if (options.rpcTimeout <= Duration.zero ||
        options.cleanupTimeout <= Duration.zero ||
        options.rpcTimeout > _maxTtl ||
        options.cleanupTimeout > _maxTtl) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Consul RPC budgets must be positive and bounded.',
      );
    }
    options.defaultAcquisition.validate();
    _validateTtl(options.defaultAcquisition.lease.leaseDuration);
  }

  static Duration _validateTtl(Duration ttl) {
    if (ttl < _minTtl ||
        ttl > _maxTtl ||
        ttl.inMicroseconds % Duration.microsecondsPerMillisecond != 0) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Consul session TTL must be 10 seconds through 24 hours in whole milliseconds.',
      );
    }
    final rounded = Duration(seconds: (ttl.inMilliseconds + 999) ~/ 1000);
    if (rounded > _maxTtl) {
      throw const DilockException(
        FailureKind.invalidOptions,
        'Rounded Consul session TTL exceeds 24 hours.',
      );
    }
    return rounded;
  }
}

/// A handle for one namespaced resource.
final class ConsulLock {
  ConsulLock._(this._provider, this.resourceId);
  final ConsulProvider _provider;

  /// Canonical draft 1 resource digest.
  final String resourceId;

  /// Attempts one acquisition and returns null on confirmed contention.
  Future<ConsulLease?> tryAcquire({
    AcquireOptions? options,
    CancellationToken? cancellationToken,
  }) =>
      _acquire(
        options ?? _provider.options.defaultAcquisition,
        waitForLock: false,
        cancellationToken: cancellationToken,
      );

  /// Waits up to the complete supplied acquisition budget.
  Future<ConsulLease> acquire({
    AcquireOptions? options,
    CancellationToken? cancellationToken,
  }) async {
    final lease = await _acquire(
      options ?? _provider.options.defaultAcquisition,
      waitForLock: true,
      cancellationToken: cancellationToken,
    );
    if (lease == null) {
      throw const DilockException(
        FailureKind.waitTimeout,
        'Consul acquisition wait elapsed.',
      );
    }
    return lease;
  }

  Future<ConsulLease?> _acquire(
    AcquireOptions policy, {
    required bool waitForLock,
    CancellationToken? cancellationToken,
  }) async {
    final provider = _provider;
    provider._ensureOpen();
    policy.validate();
    final ttl = ConsulProvider._validateTtl(policy.lease.leaseDuration);
    final ttlSeconds = ttl.inSeconds;
    final waitClock = Stopwatch()..start();
    final wait = waitForLock ? policy.wait : Duration.zero;
    final key = 'dilock/locks/$resourceId';
    final createdAt = Stopwatch()..start();
    final created = await _createSession(
      ttlSeconds,
      policy.lease.rpcTimeout,
      cancellationToken,
    );
    var leaseDeadline = createdAt.elapsed + ttl - policy.lease.safetyMargin;
    var lastRenewed = Duration.zero;
    try {
      while (true) {
        cancellationToken?.throwIfCancelled();
        if (createdAt.elapsed >= leaseDeadline) {
          throw const DilockException(
            FailureKind.acquisitionUnknown,
            'Consul session lease deadline elapsed.',
            outcomeUnknown: true,
          );
        }
        if (waitForLock && waitClock.elapsed >= wait) {
          if (!await _abandon(key, created, provider.options.cleanupTimeout)) {
            throw const DilockException(
              FailureKind.acquisitionUnknown,
              'Consul wait expired and session cleanup was not confirmed.',
              outcomeUnknown: true,
            );
          }
          return null;
        }
        final result = await provider._request(
          'PUT',
          '/v1/kv/$key',
          query: {'acquire': created},
          body: const <int>[],
          timeout: waitForLock
              ? _remaining(wait, waitClock, policy.lease.rpcTimeout)
              : policy.lease.rpcTimeout,
          mutation: true,
        );
        if (result == true) {
          final owner = await provider._readOwner(key, policy.lease.rpcTimeout);
          if (owner.$1 != created ||
              owner.$2 < 1 ||
              createdAt.elapsed >= leaseDeadline) {
            throw const DilockException(
              FailureKind.acquisitionUnknown,
              'Consul lock owner verification failed.',
              outcomeUnknown: true,
            );
          }
          final lease = ConsulLease._(
            provider,
            key,
            created,
            owner.$2,
            ttl,
            policy.lease,
            leaseDeadline,
            createdAt,
          );
          provider._leases.add(lease);
          lease._activate();
          return lease;
        }
        if (result != false) {
          throw const DilockException(
            FailureKind.protocolMismatch,
            'Consul KV acquire response was not a boolean.',
          );
        }
        if (!waitForLock) {
          if (!await _abandon(key, created, provider.options.cleanupTimeout)) {
            throw const DilockException(
              FailureKind.acquisitionUnknown,
              'Contended Consul session cleanup was not confirmed.',
              outcomeUnknown: true,
            );
          }
          return null;
        }
        final pause = _minDuration(
          const Duration(milliseconds: 100),
          wait - waitClock.elapsed,
        );
        if (pause > Duration.zero) await Future<void>.delayed(pause);
        final renewAfter = Duration(milliseconds: ttl.inMilliseconds * 2 ~/ 3);
        if (createdAt.elapsed - lastRenewed >= renewAfter) {
          final sent = createdAt.elapsed;
          await _renewSession(created, policy.lease.rpcTimeout);
          lastRenewed = sent;
          leaseDeadline = sent + ttl - policy.lease.safetyMargin;
        }
      }
    } on Object catch (error) {
      final cleanup = await _abandon(
        key,
        created,
        provider.options.cleanupTimeout,
      );
      if (error is DilockException && error.kind == FailureKind.cancelled) {
        throw DilockException(
          FailureKind.acquisitionUnknown,
          'Consul acquisition was cancelled after session creation.',
          cause: error,
          outcomeUnknown: true,
        );
      }
      if (!cleanup) {
        throw DilockException(
          FailureKind.acquisitionUnknown,
          'Consul acquisition outcome is unknown and cleanup was not confirmed.',
          cause: error,
          outcomeUnknown: true,
        );
      }
      rethrow;
    }
  }

  Future<String> _createSession(
    int ttlSeconds,
    Duration timeout,
    CancellationToken? cancellationToken,
  ) async {
    cancellationToken?.throwIfCancelled();
    final body = utf8.encode(
      jsonEncode(<String, Object?>{
        'Name': 'dilock lease',
        'Behavior': 'release',
        'LockDelay': '0s',
        'TTL': '${ttlSeconds}s',
        'Checks': <Object?>[],
      }),
    );
    final response = await _provider._request(
      'PUT',
      '/v1/session/create',
      body: body,
      timeout: timeout,
      mutation: true,
    );
    if (response is! Map ||
        response['ID'] is! String ||
        (response['ID'] as String).isEmpty) {
      throw const DilockException(
        FailureKind.protocolMismatch,
        'Consul session create response is malformed.',
      );
    }
    return response['ID'] as String;
  }

  Future<void> _renewSession(String session, Duration timeout) async {
    final response = await _provider._request(
      'PUT',
      '/v1/session/renew/$session',
      timeout: timeout,
      mutation: true,
    );
    if (response is! List ||
        response.length != 1 ||
        response.single is! Map ||
        response.single['ID'] != session) {
      throw const DilockException(
        FailureKind.leaseLost,
        'Consul session renewal response did not identify this session.',
      );
    }
  }

  Future<bool> _abandon(String key, String session, Duration timeout) async {
    try {
      await _provider._request(
        'PUT',
        '/v1/kv/$key',
        query: {'release': session},
        body: const <int>[],
        timeout: timeout,
        mutation: true,
      );
    } on Object {}
    try {
      final destroyed = await _provider._request(
        'PUT',
        '/v1/session/destroy/$session',
        timeout: timeout,
        mutation: true,
      );
      return destroyed == true;
    } on Object {
      return false;
    }
  }
}

/// One Consul session and its conservative local lease deadline.
final class ConsulLease {
  ConsulLease._(
    this._provider,
    this._key,
    this._session,
    this.lockIndex,
    this._ttl,
    this._options,
    this._deadline,
    this._clock,
  );
  final ConsulProvider _provider;
  final String _key;
  final String _session;
  final LeaseOptions _options;
  final Stopwatch _clock;
  Duration _ttl;
  Duration _deadline;
  Timer? _renewTimer;
  Future<ReleaseOutcome>? _releaseFuture;
  LeaseState _state = LeaseState.held;

  /// Verified retained-key LockIndex assigned by Consul.
  final int lockIndex;

  /// Current local lease state.
  LeaseState get state => _state;

  /// Fence paired with this provider's approved epoch.
  ConsulFence get fence =>
      ConsulFence(_provider.options.coordinationEpoch, lockIndex);

  /// Renews the session then verifies the same session and LockIndex.
  Future<bool> renew() async {
    if (_state != LeaseState.held || _clock.elapsed >= _deadline) {
      _markLost();
      return false;
    }
    final sent = _clock.elapsed;
    try {
      await _provider
          ._request(
        'PUT',
        '/v1/session/renew/$_session',
        timeout: _options.rpcTimeout,
        mutation: true,
      )
          .then((response) {
        if (response is! List ||
            response.length != 1 ||
            response.single is! Map ||
            response.single['ID'] != _session) {
          throw const DilockException(
            FailureKind.leaseLost,
            'Consul session renewal response is malformed.',
          );
        }
      });
      final owner = await _provider._readOwner(_key, _options.rpcTimeout);
      if (owner.$1 != _session || owner.$2 != lockIndex) {
        _markLost();
        return false;
      }
      _deadline = sent + _ttl - _options.safetyMargin;
      if (_clock.elapsed >= _deadline) {
        _markLost();
        return false;
      }
      return true;
    } on Object {
      _markLost();
      return false;
    }
  }

  /// Releases only this session's ownership and destroys its session.
  Future<ReleaseOutcome> release({String reason = 'explicit_release'}) =>
      _releaseFuture ??= _release(reason);

  Future<ReleaseOutcome> _release(String reason) async {
    if (_state == LeaseState.released) return ReleaseOutcome.alreadyNotOwned;
    _state = LeaseState.releasing;
    _renewTimer?.cancel();
    var outcome = ReleaseOutcome.unknown;
    try {
      final released = await _provider._request(
        'PUT',
        '/v1/kv/$_key',
        query: {'release': _session},
        body: const <int>[],
        timeout: _options.cleanupTimeout,
        mutation: true,
      );
      if (released == true) {
        outcome = ReleaseOutcome.released;
      } else if (released == false) {
        final owner = await _provider._readOwner(_key, _options.cleanupTimeout);
        outcome = owner.$1 == _session
            ? ReleaseOutcome.unknown
            : ReleaseOutcome.alreadyNotOwned;
      }
    } on Object {}
    try {
      final destroyed = await _provider._request(
        'PUT',
        '/v1/session/destroy/$_session',
        timeout: _options.cleanupTimeout,
        mutation: true,
      );
      if (destroyed != true) outcome = ReleaseOutcome.unknown;
    } on Object {
      outcome = ReleaseOutcome.unknown;
    }
    _provider._leases.remove(this);
    _state = outcome == ReleaseOutcome.unknown
        ? LeaseState.releasedUnknown
        : LeaseState.released;
    if (outcome != ReleaseOutcome.released) {
      try {
        _provider.options.cleanupObserver?.call(
          CleanupObservation(
            backend: 'consul',
            outcome: outcome,
            reason: reason,
          ),
        );
      } on Object {
        // Observability callbacks cannot change the distributed outcome.
      }
    }
    return outcome;
  }

  void _activate() {
    if (_options.autoRenew) {
      _renewTimer = Timer.periodic(_ttl ~/ 3, (_) async {
        if (_state == LeaseState.held && !await renew()) {
          _renewTimer?.cancel();
        }
      });
    }
  }

  void _markLost() {
    if (_state == LeaseState.held) _state = LeaseState.lost;
    _renewTimer?.cancel();
  }
}

Duration _minDuration(Duration left, Duration right) =>
    left < right ? left : right;

Duration _remaining(Duration total, Stopwatch elapsed, Duration perCall) {
  final left = total - elapsed.elapsed;
  if (left <= Duration.zero) {
    throw const DilockException(
      FailureKind.waitTimeout,
      'Consul acquisition wait elapsed.',
    );
  }
  return left < perCall ? left : perCall;
}

final class _ConsulNotFound implements Exception {
  const _ConsulNotFound();
}
