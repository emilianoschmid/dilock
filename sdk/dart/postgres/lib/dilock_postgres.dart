import 'dart:async';

import 'package:dilock/dilock.dart';
import 'package:postgres/postgres.dart';

const _profile = 'postgres-advisory-session-v1';
const _scheme = 'sha256-framed-utf8-v1';

/// PostgreSQL connection and namespace settings.
final class PostgresProviderOptions {
  const PostgresProviderOptions({
    required this.endpoint,
    required this.coordinationEpoch,
    this.namespace = 'default',
    this.rpcTimeout = defaultRpcTimeout,
    this.cleanupTimeout = defaultCleanupTimeout,
    this.defaultAcquisition = const AcquireOptions(),
    this.sslMode = SslMode.disable,
  });

  final Endpoint endpoint;
  final String namespace;
  final String coordinationEpoch;
  final Duration rpcTimeout;
  final Duration cleanupTimeout;
  final AcquireOptions defaultAcquisition;
  final SslMode sslMode;
}

/// Signed PostgreSQL bigint derived from the first eight identity bytes.
BigInt advisoryKey(String resourceIdentity) {
  if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(resourceIdentity)) {
    throw const DilockException(FailureKind.invalidOptions, 'resource identity must be lowercase SHA-256 hex');
  }
  var value = BigInt.parse(resourceIdentity.substring(0, 16), radix: 16);
  final sign = BigInt.one << 63;
  if ((value & sign) != BigInt.zero) value -= BigInt.one << 64;
  return value;
}

/// Owns metadata validation and creates one physical session per acquisition.
final class PostgresProvider {
  PostgresProvider._(this.options);

  final PostgresProviderOptions options;
  final Set<PostgresLease> _leases = <PostgresLease>{};
  bool _closed = false;

  static Future<PostgresProvider> connect({required PostgresProviderOptions options}) async {
    options.defaultAcquisition.validate();
    resourceId(options.namespace, 'validation');
    final provider = PostgresProvider._(options);
    final connection = await Connection.open(options.endpoint, settings: ConnectionSettings(sslMode: options.sslMode));
    try {
      await provider._validateMetadata(connection);
    } finally {
      await connection.close();
    }
    return provider;
  }

  static Future<String> provisionNamespace({required PostgresProviderOptions options}) async {
    resourceId(options.namespace, 'validation');
    final connection = await Connection.open(options.endpoint, settings: ConnectionSettings(sslMode: options.sslMode));
    try {
      await connection.execute(Sql.named('BEGIN'));
      try {
        await connection.execute(Sql.named("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))"));
        await connection.execute(Sql.named('CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)'));
        final result = await connection.execute(Sql.named('INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (@namespace, 1, @scheme, @profile, @epoch, NULL) ON CONFLICT (namespace) DO NOTHING RETURNING namespace'), parameters: {'namespace': options.namespace, 'scheme': _scheme, 'profile': _profile, 'epoch': options.coordinationEpoch});
        await connection.execute(Sql.named('COMMIT'));
        await PostgresProvider._(options)._validateMetadata(connection);
        return result.isNotEmpty ? 'created' : 'compatible-existing';
      } catch (_) {
        try {
          await connection.execute(Sql.named('ROLLBACK'));
        } catch (_) {
          // Preserve the provisioning error when rollback acknowledgement is unavailable.
        }
        rethrow;
      }
    } finally {
      await connection.close();
    }
  }

  PostgresLock lock(String resource) {
    _ensureOpen();
    return PostgresLock._(this, resourceId(options.namespace, resource));
  }

  Future<void> close() async {
    if (_closed) return;
    for (final lease in List<PostgresLease>.of(_leases)) {
      await lease.release();
    }
    _closed = true;
  }

  void _ensureOpen() {
    if (_closed) throw const DilockException(FailureKind.providerClosed, 'PostgreSQL provider is closed');
  }

  Future<void> _validateMetadata(Connection connection) async {
    final rows = await connection.execute(Sql.named('SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace=@namespace'), parameters: {'namespace': options.namespace});
    if (rows.length != 1 || rows.single[0] != 1 || rows.single[1] != _scheme || rows.single[2] != _profile || rows.single[3] != options.coordinationEpoch || rows.single[4] != null) {
      throw const DilockException(FailureKind.protocolMismatch, 'PostgreSQL namespace metadata is absent or incompatible');
    }
  }

  Future<PostgresLease?> _tryAcquire(String identity, LeaseOptions policy) async {
    _ensureOpen();
    if (policy.requireFencing || policy.autoRenew) throw const DilockException(FailureKind.unsupportedCapability, 'PostgreSQL advisory sessions provide no fencing or auto-renewal');
    final connection = await Connection.open(options.endpoint, settings: ConnectionSettings(sslMode: options.sslMode));
    try {
      final rows = await connection.execute(Sql.named('SELECT pg_try_advisory_lock(CAST(@key AS bigint))'), parameters: {'key': advisoryKey(identity).toString()});
      if (rows.length != 1 || rows.single[0] != true) {
        await connection.close();
        return null;
      }
      final lease = PostgresLease._(this, connection, advisoryKey(identity), policy);
      _leases.add(lease);
      return lease;
    } catch (error) {
      await connection.close();
      throw DilockException(FailureKind.acquisitionUnknown, 'PostgreSQL advisory-lock acknowledgement is unknown', cause: error, outcomeUnknown: true);
    }
  }
}

final class PostgresLock {
  PostgresLock._(this.provider, this.resourceIdentity);
  final PostgresProvider provider;
  final String resourceIdentity;

  Future<PostgresLease?> tryAcquire({LeaseOptions? options}) => provider._tryAcquire(resourceIdentity, options ?? provider.options.defaultAcquisition.lease);

  Future<PostgresLease> acquire({AcquireOptions? options}) async {
    final policy = options ?? provider.options.defaultAcquisition;
    policy.validate();
    final deadline = DateTime.now().add(policy.wait);
    while (DateTime.now().isBefore(deadline)) {
      final lease = await tryAcquire(options: policy.lease);
      if (lease != null) return lease;
      await Future<void>.delayed(const Duration(milliseconds: 25));
    }
    throw const DilockException(FailureKind.waitTimeout, 'PostgreSQL advisory-lock contention wait exhausted');
  }
}

final class PostgresLease {
  PostgresLease._(this.provider, this.connection, this.key, this.options);
  final PostgresProvider provider;
  final Connection connection;
  final BigInt key;
  final LeaseOptions options;
  LeaseState _state = LeaseState.held;
  ReleaseOutcome? _outcome;

  LeaseState get state => _state;
  Future<bool> renew() async {
    if (_state != LeaseState.held) throw const DilockException(FailureKind.leaseLost, 'PostgreSQL session is no longer live');
    try { await connection.execute(Sql.named('SELECT 1')); return true; } catch (error) { _state = LeaseState.lost; throw DilockException(FailureKind.backendUnavailable, 'PostgreSQL liveness check failed', cause: error); }
  }

  Future<ReleaseOutcome> release() async {
    if (_outcome != null) return _outcome!;
    _state = LeaseState.releasing;
    try {
      final rows = await connection.execute(Sql.named('SELECT pg_advisory_unlock(CAST(@key AS bigint))'), parameters: {'key': key.toString()});
      _outcome = rows.length == 1 && rows.single[0] == true ? ReleaseOutcome.released : ReleaseOutcome.alreadyNotOwned;
    } catch (_) { _outcome = ReleaseOutcome.unknown; }
    await connection.close();
    _state = _outcome == ReleaseOutcome.unknown ? LeaseState.releasedUnknown : LeaseState.released;
    provider._leases.remove(this);
    return _outcome!;
  }
}
