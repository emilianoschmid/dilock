package org.dilock.postgres;

import org.dilock.redis.AcquireOptions;
import org.dilock.redis.DilockException;
import org.dilock.redis.Identity;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** PostgreSQL session advisory-lock provider. Metadata is read-only at runtime. */
public final class PostgresProvider implements AutoCloseable {
    static final String PROFILE = "postgres-advisory-session-v1";
    static final String SCHEME = "sha256-framed-utf8-v1";
    private final PostgresProviderOptions options;
    private final Set<PostgresLease> leases = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    private PostgresProvider(PostgresProviderOptions options) { this.options = options; validateMetadata(); }

    public static PostgresProvider connect(PostgresProviderOptions options) {
        if (options == null) throw new IllegalArgumentException("options are required");
        return new PostgresProvider(options);
    }

    public static PostgresProvider connect(String jdbcUrl, String namespace, String epoch) {
        return connect(new PostgresProviderOptions(jdbcUrl, namespace, epoch));
    }

    public static PostgresProvider connect(String jdbcUrl, String epoch) {
        return connect(new PostgresProviderOptions(jdbcUrl, epoch));
    }

    public PostgresLock lock(String resource) {
        ensureOpen();
        Identity.resourceId(options.namespace(), resource);
        return new PostgresLock(this, resource);
    }

    AcquireOptions defaultAcquisition() { return options.defaultAcquisition(); }
    String namespace() { return options.namespace(); }
    long requestTimeoutMillis() { return options.requestTimeoutMillis(); }
    long cleanupTimeoutMillis() { return options.cleanupTimeoutMillis(); }
    void track(PostgresLease lease) { leases.add(lease); }
    void untrack(PostgresLease lease) { leases.remove(lease); }
    void ensureOpen() { if (closed.get()) throw new IllegalStateException("PostgreSQL provider closed"); }

    private Connection connectSession(long timeoutMillis) throws SQLException {
        DriverManager.setLoginTimeout((int) Math.max(1, Math.ceil(timeoutMillis / 1000.0)));
        Connection connection = DriverManager.getConnection(options.jdbcUrl());
        connection.setAutoCommit(true);
        return connection;
    }

    private void validateMetadata() {
        try (Connection connection = connectSession(options.requestTimeoutMillis());
             PreparedStatement statement = bounded(connection,
                 "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = ?",
                 options.requestTimeoutMillis())) {
            statement.setString(1, options.namespace());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw protocolMismatch("missing PostgreSQL namespace metadata");
                if (rows.getInt("protocol_major") != 1 || !SCHEME.equals(rows.getString("identity_scheme"))
                        || !PROFILE.equals(rows.getString("profile")) || !options.epoch().equals(rows.getString("coordination_epoch"))
                        || rows.getObject("fixed_lease_ms") != null || rows.next())
                    throw protocolMismatch("incompatible PostgreSQL namespace metadata");
            }
        } catch (DilockException e) { throw e; }
        catch (SQLException e) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "failed reading PostgreSQL namespace metadata", e); }
    }

    private static PreparedStatement bounded(Connection connection, String sql, long timeoutMillis) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setQueryTimeout((int) Math.max(1, Math.ceil(timeoutMillis / 1000.0)));
        return statement;
    }

    private static DilockException protocolMismatch(String message) {
        return new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, message);
    }

    Optional<PostgresLease> tryAcquire(String resource, long leaseMillis) {
        ensureOpen();
        long key = advisoryKey(Identity.resourceId(options.namespace(), resource));
        final Connection connection;
        try { connection = connectSession(options.requestTimeoutMillis()); }
        catch (SQLException e) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "failed opening PostgreSQL lock session", e); }
        try (PreparedStatement statement = bounded(connection,
                "SELECT pg_try_advisory_lock(?::bigint)", options.requestTimeoutMillis())) {
            statement.setLong(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "PostgreSQL advisory-lock response was empty");
                if (!rows.getBoolean(1)) { connection.close(); return Optional.empty(); }
                PostgresLease lease = new PostgresLease(this, connection, key, leaseMillis);
                track(lease);
                return Optional.of(lease);
            }
        } catch (DilockException e) { closeQuietly(connection); throw e; }
        catch (SQLException e) { closeQuietly(connection); throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "PostgreSQL advisory-lock acknowledgement is unknown", e); }
    }

    static long advisoryKey(String resourceId) {
        if (resourceId == null || !resourceId.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("resourceId must be a lowercase SHA-256 digest");
        return new BigInteger(resourceId.substring(0, 16), 16).longValue();
    }

    boolean release(PostgresLease lease) {
        try (PreparedStatement statement = bounded(lease.connection(),
                "SELECT pg_advisory_unlock(?::bigint)", options.cleanupTimeoutMillis())) {
            statement.setLong(1, lease.key());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("empty PostgreSQL unlock response");
                boolean released = rows.getBoolean(1);
                lease.connection().close();
                untrack(lease);
                return released;
            }
        } catch (SQLException e) { throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "PostgreSQL advisory-lock release was not confirmed", e); }
    }

    void liveness(PostgresLease lease) {
        try (Statement statement = lease.connection().createStatement()) {
            statement.setQueryTimeout((int) Math.max(1, Math.ceil(options.requestTimeoutMillis() / 1000.0)));
            statement.execute("SELECT 1");
        } catch (SQLException e) {
            lease.lose();
            throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "PostgreSQL session liveness failed", e);
        }
    }

    private static void closeQuietly(Connection connection) { try { connection.close(); } catch (SQLException ignored) { } }

    /** Explicit create-if-absent provisioning; runtime connect never calls this method. */
    public static String provisionNamespace(String jdbcUrl, String namespace, String epoch) {
        PostgresProviderOptions options = new PostgresProviderOptions(jdbcUrl, namespace, epoch);
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement create = connection.createStatement()) {
            connection.setAutoCommit(false);
            try {
                create.execute("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))");
                create.execute("CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)");
            int rows;
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (?, 1, ?, ?, ?, NULL) ON CONFLICT (namespace) DO NOTHING")) {
                insert.setString(1, namespace); insert.setString(2, SCHEME); insert.setString(3, PROFILE); insert.setString(4, epoch);
                rows = insert.executeUpdate();
            }
                connection.commit();
            try (PostgresProvider provider = PostgresProvider.connect(options)) { }
            return rows == 1 ? "created" : "compatible-existing";
            } catch (SQLException error) {
                try { connection.rollback(); } catch (SQLException rollbackError) { error.addSuppressed(rollbackError); }
                throw error;
            }
        } catch (SQLException e) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "failed provisioning PostgreSQL namespace metadata", e); }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (PostgresLease lease : Set.copyOf(leases)) try { lease.release(); } catch (RuntimeException ignored) { }
    }
}
