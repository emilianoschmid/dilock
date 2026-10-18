import DilockCore
import Foundation
import Logging
import PostgresNIO

/// Configuration for the experimental `postgres-advisory-session-v1` profile.
/// A connection is opened for each acquisition and is never borrowed from a pool.
public struct PostgresProviderOptions: Sendable, Equatable {
    public let host: String
    public let port: Int
    public let username: String
    public let password: String?
    public let database: String
    public let namespace: String
    public let epoch: String
    public let defaults: AcquireOptions

    public init(host: String, port: Int = 5432, username: String,
                password: String? = nil, database: String,
                namespace: String = "default", epoch: String,
                defaults: AcquireOptions = try! AcquireOptions()) throws {
        guard !host.isEmpty, (1...65535).contains(port), !username.isEmpty,
              !database.isEmpty else {
            throw DilockError.invalidConfiguration("PostgreSQL connection fields are invalid")
        }
        _ = try Identity.resourceID(namespace: namespace, resource: "validation")
        guard !epoch.isEmpty, epoch.utf8.count <= 1024,
              !epoch.unicodeScalars.contains(where: { $0.value == 0 }) else {
            throw DilockError.invalidConfiguration("epoch must contain 1..1024 UTF-8 bytes and no NUL")
        }
        self.host = host; self.port = port; self.username = username
        self.password = password; self.database = database; self.namespace = namespace
        self.epoch = epoch; self.defaults = defaults
    }
}

public enum PostgresMetadataProvision: Sendable, Equatable {
    case created
    case alreadyCompatible
}

private let postgresLogger = Logger(label: "org.dilock.postgres")

public actor PostgresProvider {
    fileprivate let options: PostgresProviderOptions
    private var closed = false

    public init(options: PostgresProviderOptions) async throws {
        self.options = options
        let connection = try await Self.connect(options)
        do {
            try await Self.validateMetadata(connection, options: options)
            try await connection.close()
        } catch {
            try? await connection.close()
            throw error
        }
    }

    public static func provisionNamespace(options: PostgresProviderOptions) async throws -> PostgresMetadataProvision {
        let connection = try await connect(options)
        do {
            try await connection.query("BEGIN", logger: postgresLogger)
            try await connection.query(
                "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))",
                logger: postgresLogger)
            try await connection.query("""
                CREATE TABLE IF NOT EXISTS dilock_namespace_meta (
                  namespace text PRIMARY KEY,
                  protocol_major integer NOT NULL,
                  identity_scheme text NOT NULL,
                  profile text NOT NULL,
                  coordination_epoch text NOT NULL,
                  fixed_lease_ms bigint NULL
                )
                """, logger: postgresLogger)
            let rows = try await connection.query("""
                INSERT INTO dilock_namespace_meta
                  (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms)
                VALUES (\(options.namespace), 1, 'sha256-framed-utf8-v1',
                        'postgres-advisory-session-v1', \(options.epoch), NULL)
                ON CONFLICT (namespace) DO NOTHING
                RETURNING namespace
                """, logger: postgresLogger)
            var created = false
            for try await _ in rows { created = true }
            try await connection.query("COMMIT", logger: postgresLogger)
            try await validateMetadata(connection, options: options)
            try await connection.close()
            return created ? .created : .alreadyCompatible
        } catch {
            try? await connection.query("ROLLBACK", logger: postgresLogger)
            try? await connection.close()
            throw error
        }
    }

    public func lock(_ resource: String) throws -> PostgresLock {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        let id = try Identity.resourceID(namespace: options.namespace, resource: resource)
        return PostgresLock(provider: self, key: PostgresProvider.advisoryKey(id), defaults: options.defaults)
    }

    public func close() async { closed = true }

    fileprivate func acquire(key: Int64, policy: AcquireOptions, zeroWait: Bool) async throws -> PostgresLease? {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        let deadline = ContinuousClock.now + (zeroWait ? .milliseconds(1) : policy.wait)
        while ContinuousClock.now < deadline {
            try Task.checkCancellation()
            let connection = try await Self.connect(options)
            do {
                var granted = false
                let rows = try await connection.query(
                    "SELECT pg_try_advisory_lock((\(key))::bigint)", logger: postgresLogger)
                for try await row in rows {
                    granted = try row.decode(Bool.self)
                }
                if granted { return PostgresLease(connection: connection, key: key) }
                try await connection.close()
            } catch {
                try? await connection.close()
                throw DilockError.acquisitionOutcomeUnknown("PostgreSQL advisory-lock outcome is unknown: \(error)")
            }
            if zeroWait { return nil }
            try await Task.sleep(for: .milliseconds(25))
        }
        if zeroWait { return nil }
        throw DilockError.timedOut
    }

    private static func connect(_ options: PostgresProviderOptions) async throws -> PostgresConnection {
        let configuration = PostgresConnection.Configuration(
            host: options.host, port: options.port, username: options.username,
            password: options.password, database: options.database, tls: .disable)
        do {
            return try await PostgresConnection.connect(
                configuration: configuration,
                id: Int.random(in: 1...Int.max), logger: postgresLogger)
        } catch {
            throw DilockError.backendUnavailable("PostgreSQL connection failed: \(error)")
        }
    }

    private static func validateMetadata(_ connection: PostgresConnection,
                                         options: PostgresProviderOptions) async throws {
        var valid = false
        do {
            let rows = try await connection.query("""
                SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms
                FROM dilock_namespace_meta WHERE namespace = \(options.namespace)
                """, logger: postgresLogger)
            for try await row in rows {
                let decoded = try row.decode((Int32, String, String, String, Int64?).self)
                valid = decoded.0 == 1 && decoded.1 == "sha256-framed-utf8-v1"
                    && decoded.2 == "postgres-advisory-session-v1"
                    && decoded.3 == options.epoch && decoded.4 == nil
            }
        } catch {
            throw DilockError.backendUnavailable("PostgreSQL metadata query failed: \(error)")
        }
        guard valid else { throw DilockError.protocolMismatch("PostgreSQL namespace metadata is absent or incompatible") }
    }

    static func advisoryKey(_ resourceID: String) -> Int64 {
        let prefix = resourceID.prefix(16)
        let raw = UInt64(String(prefix), radix: 16)!
        return Int64(bitPattern: raw)
    }
}

public struct PostgresLock: Sendable {
    fileprivate let provider: PostgresProvider
    fileprivate let key: Int64
    fileprivate let defaults: AcquireOptions

    public func tryAcquire() async throws -> PostgresLease? {
        try await provider.acquire(key: key, policy: defaults, zeroWait: true)
    }

    public func acquire(_ policy: AcquireOptions? = nil) async throws -> PostgresLease {
        guard let lease = try await provider.acquire(key: key, policy: policy ?? defaults, zeroWait: false) else {
            throw DilockError.timedOut
        }
        return lease
    }
}

public actor PostgresLease: Lease {
    private let connection: PostgresConnection
    private let key: Int64
    private var released = false

    fileprivate init(connection: PostgresConnection, key: Int64) {
        self.connection = connection; self.key = key
    }

    public func renew() async throws -> Bool {
        guard !released else { throw DilockError.leaseLost }
        do {
            try await connection.query("SELECT 1", logger: postgresLogger)
            return true
        } catch {
            released = true
            throw DilockError.leaseLost
        }
    }

    public func release() async -> ReleaseOutcome {
        guard !released else { return .alreadyNotOwned }
        released = true
        do {
            var unlocked = false
            let rows = try await connection.query(
                "SELECT pg_advisory_unlock((\(key))::bigint)", logger: postgresLogger)
            for try await row in rows {
                unlocked = try row.decode(Bool.self)
            }
            try await connection.close()
            return unlocked ? .released : .alreadyNotOwned
        } catch {
            try? await connection.close()
            return .unknown
        }
    }
}
