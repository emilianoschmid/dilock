package org.dilock.redis;

/** Immutable full provider configuration. Redis URI controls TLS and optional authentication.
 * @param endpoint Redis URI
 * @param namespace pre-provisioned namespace; null selects {@code default}
 * @param epoch operator-approved coordination epoch
 * @param rpcTimeoutMillis per-request RPC timeout
 * @param cleanupTimeoutMillis best-effort cleanup timeout
 * @param defaultAcquisition immutable default acquisition policy
 * @param cleanupObserver optional observer for uncertain cleanup
 */
public record RedisProviderOptions(String endpoint, String namespace, String epoch,
                                   long rpcTimeoutMillis, long cleanupTimeoutMillis,
                                   AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver) {
    /** Validates and snapshots all provider settings before a connection is opened. */
    public RedisProviderOptions {
        if (endpoint == null || endpoint.isBlank()) throw new IllegalArgumentException("endpoint is required");
        namespace = namespace == null ? "default" : namespace;
        if (epoch == null || epoch.isEmpty()) throw new IllegalArgumentException("epoch is required");
        if (rpcTimeoutMillis < 1 || rpcTimeoutMillis > 60_000) throw new IllegalArgumentException("rpcTimeoutMillis must be 1 through 60000");
        if (cleanupTimeoutMillis < 1 || cleanupTimeoutMillis > 60_000) throw new IllegalArgumentException("cleanupTimeoutMillis must be 1 through 60000");
        defaultAcquisition = defaultAcquisition == null ? AcquireOptions.DEFAULT : defaultAcquisition;
        if (rpcTimeoutMillis + Math.max(10, (defaultAcquisition.leaseMillis() + 99) / 100) >= defaultAcquisition.leaseMillis()) throw new IllegalArgumentException("RPC timeout plus safety margin must be less than the default lease");
    }
    /** Creates settings with the 2 s transport budgets and standard 5 s/30 s acquisition policy.
     * @param endpoint Redis URI
     * @param namespace pre-provisioned namespace
     * @param epoch operator-approved coordination epoch
     */
    public RedisProviderOptions(String endpoint, String namespace, String epoch) {
        this(endpoint, namespace, epoch, 2_000, 2_000, AcquireOptions.DEFAULT, null);
    }

    /** Creates full settings with native Java timeout values.
     * @param endpoint Redis URI
     * @param namespace pre-provisioned namespace
     * @param epoch operator-approved coordination epoch
     * @param rpcTimeout per-request timeout
     * @param cleanupTimeout best-effort cleanup timeout
     * @param defaultAcquisition immutable default acquisition policy
     * @param cleanupObserver optional observer for uncertain cleanup
     */
    public RedisProviderOptions(String endpoint, String namespace, String epoch,
                                java.time.Duration rpcTimeout, java.time.Duration cleanupTimeout,
                                AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver) {
        this(endpoint, namespace, epoch, durationMillis(rpcTimeout, "rpcTimeout"), durationMillis(cleanupTimeout, "cleanupTimeout"), defaultAcquisition, cleanupObserver);
    }

    private static long durationMillis(java.time.Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero() || value.getNano() % 1_000_000 != 0) throw new IllegalArgumentException(name + " must be a positive whole-millisecond duration");
        return value.toMillis();
    }
}
