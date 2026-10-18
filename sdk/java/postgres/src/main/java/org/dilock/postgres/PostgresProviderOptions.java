package org.dilock.postgres;

import org.dilock.redis.AcquireOptions;

/** Immutable configuration for the experimental PostgreSQL advisory-session profile. */
public record PostgresProviderOptions(String jdbcUrl, String namespace, String epoch,
                                     long requestTimeoutMillis, long cleanupTimeoutMillis,
                                     AcquireOptions defaultAcquisition) {
    public PostgresProviderOptions {
        if (jdbcUrl == null || jdbcUrl.isBlank() || !jdbcUrl.startsWith("jdbc:postgresql:"))
            throw new IllegalArgumentException("jdbcUrl must be a PostgreSQL JDBC URL");
        namespace = namespace == null ? "default" : namespace;
        if (epoch == null || epoch.isBlank() || epoch.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024)
            throw new IllegalArgumentException("epoch must be nonempty and at most 1024 UTF-8 bytes");
        if (requestTimeoutMillis < 1 || requestTimeoutMillis > 60_000) throw new IllegalArgumentException("request timeout must be 1..60000 ms");
        if (cleanupTimeoutMillis < 1 || cleanupTimeoutMillis > 60_000) throw new IllegalArgumentException("cleanup timeout must be 1..60000 ms");
        defaultAcquisition = defaultAcquisition == null ? AcquireOptions.DEFAULT : defaultAcquisition;
        org.dilock.redis.Identity.resourceId(namespace, "validation");
    }

    public PostgresProviderOptions(String jdbcUrl, String namespace, String epoch) {
        this(jdbcUrl, namespace, epoch, 2_000, 2_000, AcquireOptions.DEFAULT);
    }

    public PostgresProviderOptions(String jdbcUrl, String epoch) {
        this(jdbcUrl, "default", epoch);
    }
}
