import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dilock/dilock.dart';
import 'package:dilock_etcd/dilock_etcd.dart';
import 'package:dilock_redis/dilock_redis.dart';

import 'package:dilock_conformance_worker/src/protocol.dart';

const _maxActiveRequests = 64;
const _maxHeldLeases = 64;
const _shutdownSettle = Duration(seconds: 2);
const _shutdownCleanup = Duration(seconds: 5);

Future<void> main(List<String> args) async {
  if (args.length != 1) {
    stderr.writeln('usage: dilock_worker <config.json>');
    exitCode = 64;
    return;
  }
  final Map<String, Object?> config;
  try {
    final decoded = jsonDecode(await File(args.single).readAsString());
    if (decoded is! Map<String, dynamic>) throw const FormatException();
    config = decoded.cast<String, Object?>();
  } on Object {
    stderr.writeln('worker startup failed: invalid config file');
    exitCode = 1;
    return;
  }
  try {
    await _JsonlWorker(config).run();
  } on Object catch (error) {
    stderr.writeln('worker startup failed: ${_safeError(error)}');
    exitCode = 1;
  }
}

final class _JsonlWorker {
  _JsonlWorker(Map<String, Object?> config)
      : backend = _text(config, 'backend'),
        endpoint = _text(config, 'endpoint'),
        namespace = _text(config, 'namespace'),
        epoch = _text(config, 'epoch'),
        defaultWaitMs = _configDuration(config, 'wait_ms', 5000),
        defaultLeaseMs = _configDuration(config, 'lease_ms', 30000),
        rpcMs = _configDuration(config, 'rpc_ms', 2000),
        cleanupMs = _configDuration(config, 'cleanup_ms', 2000) {
    if (backend != 'redis' && backend != 'etcd') {
      throw const FormatException('backend must be redis or etcd');
    }
    final profile = config['profile'];
    final expectedProfile = backend == 'redis'
        ? 'redis-single-authority-v1'
        : 'etcd-native-lock-v1';
    if (profile != null && profile != expectedProfile) {
      throw const FormatException('profile does not match selected backend');
    }
  }

  final String backend;
  final String endpoint;
  final String namespace;
  final String epoch;
  final int defaultWaitMs;
  final int defaultLeaseMs;
  final int rpcMs;
  final int cleanupMs;
  RedisProvider? redis;
  EtcdProvider? etcd;
  final Map<String, Object> leases = <String, Object>{};
  final Map<String, String> released = <String, String>{};
  final Map<String, CancellationToken> cancellations =
      <String, CancellationToken>{};
  final Set<String> activeIds = <String>{};
  final Set<Future<void>> tasks = <Future<void>>{};
  bool stopping = false;
  Future<void>? shutdownTask;
  int cleanupAttempts = 0;
  int outstandingCleanup = 0;

  Future<void> run() async {
    final defaultLease = LeaseOptions(
      leaseDuration: Duration(milliseconds: defaultLeaseMs),
      rpcTimeout: Duration(milliseconds: rpcMs),
      cleanupTimeout: Duration(milliseconds: cleanupMs),
    );
    final defaults = AcquireOptions(
      wait: Duration(milliseconds: defaultWaitMs),
      lease: defaultLease,
    );
    if (backend == 'redis') {
      redis = await RedisProvider.connect(
        endpoint,
        options: RedisProviderOptions(
          namespace: namespace,
          coordinationEpoch: epoch,
          defaultAcquisition: defaults,
          connectTimeout: Duration(milliseconds: rpcMs),
        ),
      );
    } else {
      etcd = await EtcdProvider.connect(
        endpoint,
        options: EtcdProviderOptions(
          namespace: namespace,
          coordinationEpoch: epoch,
          rpcTimeout: Duration(milliseconds: rpcMs),
          cleanupTimeout: Duration(milliseconds: cleanupMs),
          defaultAcquisition: defaults,
        ),
      );
    }
    try {
      await _readRequests();
      if (shutdownTask case final task?) {
        await task;
      } else {
        stopping = true;
        await _shutdown(null);
      }
    } finally {
      await _closeProviders();
    }
  }

  Future<void> _readRequests() async {
    final line = <int>[];
    var oversized = false;
    await for (final chunk in stdin) {
      for (final byte in chunk) {
        if (byte == 10) {
          if (oversized) {
            stderr.writeln('worker: input line exceeds 65536 bytes');
          } else {
            _acceptBytes(line);
          }
          line.clear();
          oversized = false;
          if (stopping) return;
        } else if (!oversized) {
          if (line.length == maxLineBytes) {
            line.clear();
            oversized = true;
          } else {
            line.add(byte);
          }
        }
      }
    }
    if (oversized) {
      stderr.writeln('worker: input line exceeds 65536 bytes');
    } else if (line.isNotEmpty) {
      _acceptBytes(line);
    }
  }

  void _acceptBytes(List<int> bytes) {
    var length = bytes.length;
    if (length > 0 && bytes[length - 1] == 13) length--;
    final String line;
    try {
      line = utf8.decode(bytes.sublist(0, length), allowMalformed: false);
    } on FormatException {
      stderr.writeln('worker: malformed UTF-8 input');
      return;
    }
    final Map<String, Object?> request;
    try {
      request = parseRequest(line);
    } on FormatException catch (error) {
      stderr.writeln('worker: ${error.message}');
      return;
    }
    final id = request['id']! as String;
    if (!activeIds.add(id)) {
      _write(_response(id, 'invalid_argument'));
      return;
    }
    if (request['op'] == 'shutdown') {
      stopping = true;
      shutdownTask = _shutdown(id);
      unawaited(shutdownTask!);
      return;
    }
    if (request['op'] == 'cancel_request') {
      _cancel(id, request);
      return;
    }
    if (activeIds.length > _maxActiveRequests) {
      activeIds.remove(id);
      _write(
        _response(id, 'invalid_argument')
          ..['error'] = 'worker active-request limit reached',
      );
      return;
    }
    late final Future<void> task;
    task = _handle(id, request).whenComplete(() {
      activeIds.remove(id);
      cancellations.remove(id);
      tasks.remove(task);
    });
    tasks.add(task);
  }

  void _cancel(String id, Map<String, Object?> request) {
    final target = request['target_id']! as String;
    final token = cancellations[target];
    token?.cancel();
    _write(_response(id, 'ok')..['cancelled'] = token != null);
    activeIds.remove(id);
  }

  Future<void> _handle(String id, Map<String, Object?> request) async {
    try {
      final op = request['op']! as String;
      switch (op) {
        case 'hello':
          _write(_hello(id));
        case 'acquire':
        case 'try_acquire':
          await _acquire(id, op, request);
        case 'renew':
          await _renew(id, request);
        case 'release':
          await _release(id, request);
        case 'inspect':
          _inspect(id, request);
        default:
          _write(_response(id, 'invalid_argument'));
      }
    } on Object catch (error) {
      final response = _response(id, _mapError(error));
      if (error is DilockException &&
          (error.kind == FailureKind.cancelled ||
              error.kind == FailureKind.acquisitionUnknown) &&
          cancellations[id]?.isCancelled == true) {
        response['status'] = 'cancelled';
        response['remote_outcome_unknown'] = error.outcomeUnknown;
      }
      _write(response);
    }
  }

  Map<String, Object?> _hello(String id) => <String, Object?>{
        'id': id,
        'status': 'ok',
        'sdk_version': '0.1.0',
        'protocol': 1,
        'backend': backend,
        'server_version': 'unknown',
        'client_version': 'unknown',
        'capabilities': <String, Object?>{
          'renewal': true,
          'auto_renew': true,
          'fencing': backend == 'etcd',
          'try_acquire': true,
        },
      };

  Future<void> _acquire(
    String id,
    String op,
    Map<String, Object?> request,
  ) async {
    if (leases.length >= _maxHeldLeases) {
      _write(
        _response(id, 'invalid_argument')
          ..['error'] = 'worker held-lease limit reached',
      );
      return;
    }
    final resource = requiredText(request, 'resource')!;
    final leaseMs = boundedInteger(
      request,
      'lease_ms',
      defaultLeaseMs,
      allowZero: false,
    );
    final waitMs = boundedInteger(
      request,
      'wait_ms',
      defaultWaitMs,
      allowZero: true,
    );
    final autoRenew = request['auto_renew'] as bool? ?? false;
    final options = LeaseOptions(
      leaseDuration: Duration(milliseconds: leaseMs),
      rpcTimeout: Duration(milliseconds: rpcMs),
      cleanupTimeout: Duration(milliseconds: cleanupMs),
      autoRenew: autoRenew,
    );
    final token = CancellationToken();
    cancellations[id] = token;
    Object? lease;
    if (backend == 'redis') {
      final lock = redis!.lock(resource);
      if (op == 'try_acquire') {
        lease = await lock.tryAcquire(
          options: options,
          cancellationToken: token,
        );
      } else if (waitMs == 0) {
        lease = await lock.tryAcquire(
          options: options,
          cancellationToken: token,
        );
      } else {
        lease = await lock.acquire(
          Duration(milliseconds: waitMs),
          options: options,
          cancellationToken: token,
        );
      }
    } else {
      final lock = etcd!.lock(resource);
      final policy = AcquireOptions(
        wait: Duration(milliseconds: waitMs == 0 ? 1 : waitMs),
        lease: options,
      );
      if (op == 'try_acquire' || waitMs == 0) {
        lease = await lock.tryAcquire(
          options: options,
          cancellationToken: token,
        );
      } else {
        lease = await lock.acquire(options: policy, cancellationToken: token);
      }
    }
    if (lease == null) {
      _write(_response(id, op == 'try_acquire' ? 'busy' : 'timeout'));
      return;
    }
    if (stopping || token.isCancelled) {
      cleanupAttempts++;
      final result = await _releaseObject(lease)
          .timeout(_shutdownCleanup, onTimeout: () => ReleaseOutcome.unknown);
      if (result == ReleaseOutcome.unknown) outstandingCleanup++;
      _write(_response(id, 'cancelled')..['remote_outcome_unknown'] = false);
      return;
    }
    final leaseId =
        'dart-${DateTime.now().microsecondsSinceEpoch}-${leases.length}';
    leases[leaseId] = lease;
    final response = _response(id, 'acquired')..['lease_id'] = leaseId;
    if (lease is RedisLease) {
      response['effective_lease_ms'] = lease.effectiveLease.inMilliseconds;
    } else if (lease is EtcdLease) {
      response['effective_lease_ms'] = lease.effectiveLease.inMilliseconds;
      response['fence'] = <String, Object?>{
        'epoch': lease.fence.coordinationEpoch,
        'value': lease.fence.createRevision.toString(),
      };
    }
    _write(response);
  }

  Future<void> _renew(String id, Map<String, Object?> request) async {
    final leaseId = requiredText(request, 'lease_id')!;
    final lease = leases[leaseId];
    if (lease == null) {
      _write(
        _response(
          id,
          released.containsKey(leaseId)
              ? 'already_not_owned'
              : 'invalid_argument',
        ),
      );
      return;
    }
    final leaseMs = boundedInteger(
      request,
      'lease_ms',
      _effectiveLeaseMs(lease),
      allowZero: false,
    );
    if (leaseMs != _effectiveLeaseMs(lease)) {
      _write(_response(id, 'unsupported_capability'));
      return;
    }
    if (lease is RedisLease) {
      final renewed = await lease.renew();
      _write(_response(id, renewed ? 'renewed' : 'already_not_owned'));
    } else if (lease is EtcdLease) {
      final renewed = await lease.renew();
      _write(_response(id, renewed ? 'renewed' : 'already_not_owned'));
    } else {
      _write(_response(id, 'unsupported_capability'));
    }
    // Both SDKs renew using the lease's captured TTL. The wire request cannot
    // mutate a live lease's immutable safety policy.
  }

  Future<void> _release(String id, Map<String, Object?> request) async {
    final leaseId = requiredText(request, 'lease_id')!;
    final prior = released[leaseId];
    if (prior != null) {
      _write(_response(id, prior));
      return;
    }
    final lease = leases[leaseId];
    if (lease == null) {
      _write(_response(id, 'invalid_argument'));
      return;
    }
    cleanupAttempts++;
    final outcome = await _releaseObject(lease);
    final status = _releaseStatus(outcome);
    leases.remove(leaseId);
    released[leaseId] = status;
    _write(_response(id, status));
  }

  void _inspect(String id, Map<String, Object?> request) {
    final leaseId = requiredText(request, 'lease_id')!;
    final lease = leases[leaseId];
    if (lease == null) {
      _write(_response(id, 'invalid_argument'));
      return;
    }
    final state = lease is RedisLease
        ? lease.state.name
        : (lease as EtcdLease).state.name;
    _write(_response(id, 'ok')..['state'] = _snakeCase(state));
  }

  Future<void> _shutdown(String? id) async {
    for (final token in cancellations.values) {
      token.cancel();
    }
    final pending = List<Future<void>>.of(tasks);
    if (pending.isNotEmpty) {
      await Future.wait<void>(pending)
          .timeout(_shutdownSettle, onTimeout: () => <void>[]);
    }
    final toRelease = Map<String, Object>.of(leases);
    for (final entry in toRelease.entries) {
      cleanupAttempts++;
      try {
        final outcome = await _releaseObject(entry.value)
            .timeout(_shutdownCleanup, onTimeout: () => ReleaseOutcome.unknown);
        if (outcome == ReleaseOutcome.unknown) outstandingCleanup++;
        released[entry.key] = _releaseStatus(outcome);
      } on Object {
        outstandingCleanup++;
      }
      leases.remove(entry.key);
    }
    if (id != null) {
      _write(
        _response(id, 'ok')
          ..['cleanup_attempts'] = cleanupAttempts
          ..['outstanding_cleanup_attempts'] = outstandingCleanup,
      );
      activeIds.remove(id);
    }
  }

  Future<void> _closeProviders() async {
    try {
      await (redis?.close() ?? etcd?.close() ?? Future<void>.value()).timeout(
        _shutdownCleanup,
      );
    } on Object {
      outstandingCleanup++;
      stderr.writeln('worker: provider close exceeded cleanup budget');
    }
  }

  Future<ReleaseOutcome> _releaseObject(Object lease) {
    if (lease is RedisLease) return lease.release(reason: 'worker_shutdown');
    if (lease is EtcdLease) return lease.release(reason: 'worker_shutdown');
    return Future<ReleaseOutcome>.error(StateError('unknown lease type'));
  }

  int _effectiveLeaseMs(Object lease) => switch (lease) {
        RedisLease value => value.effectiveLease.inMilliseconds,
        EtcdLease value => value.effectiveLease.inMilliseconds,
        _ => throw StateError('unknown lease type'),
      };

  void _write(Map<String, Object?> response) {
    // The Dart event loop serializes synchronous writes; keep one JSON object
    // per line and never expose owner tokens or backend exception payloads.
    final encoded = '${jsonEncode(response)}\n';
    stdout.write(encoded);
  }
}

Map<String, Object?> _response(String id, String status) => <String, Object?>{
      'id': id,
      'status': status,
    };

String _mapError(Object error) {
  if (error is FormatException || error is ArgumentError) {
    return 'invalid_argument';
  }
  if (error is DilockException) {
    return switch (error.kind) {
      FailureKind.backendUnavailable => 'backend_unavailable',
      FailureKind.acquisitionUnknown => 'acquisition_unknown',
      FailureKind.protocolMismatch => 'protocol_mismatch',
      FailureKind.unsupportedCapability => 'unsupported_capability',
      FailureKind.waitTimeout => 'timeout',
      FailureKind.leaseLost => 'already_not_owned',
      FailureKind.releaseUnknown => 'release_unknown',
      FailureKind.providerClosed => 'backend_unavailable',
      FailureKind.invalidOptions => 'invalid_argument',
      FailureKind.cancelled => 'cancelled',
    };
  }
  return 'internal_error';
}

String _releaseStatus(ReleaseOutcome outcome) => switch (outcome) {
      ReleaseOutcome.released => 'released',
      ReleaseOutcome.alreadyNotOwned => 'already_not_owned',
      ReleaseOutcome.unknown => 'release_unknown',
    };

String _snakeCase(String name) => name.replaceAllMapped(
      RegExp(r'[A-Z]'),
      (match) => '_${match[0]!.toLowerCase()}',
    );

String _text(Map<String, Object?> config, String key) {
  final value = config[key];
  if (value is! String || value.isEmpty) throw FormatException('missing $key');
  return value;
}

String _safeError(Object error) => error is FormatException
    ? error.message.toString()
    : error is DilockException
        ? error.kind.name
        : 'provider initialization failed';

int _configDuration(Map<String, Object?> config, String key, int fallback) {
  final value = config[key];
  if (value == null) return fallback;
  if (value is! int || value < 1 || value > maxDurationMs) {
    throw FormatException('$key must be a bounded positive integer');
  }
  return value;
}
