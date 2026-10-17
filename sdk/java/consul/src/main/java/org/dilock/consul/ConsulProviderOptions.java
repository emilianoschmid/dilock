package org.dilock.consul;

import org.dilock.redis.AcquireOptions;

/** Immutable Consul HTTP provider configuration. */
public record ConsulProviderOptions(String endpoint, String namespace, String epoch, String token,
                                   long requestTimeoutMillis, long cleanupTimeoutMillis,
                                   AcquireOptions defaultAcquisition) {
    public ConsulProviderOptions {
        if (endpoint == null || endpoint.isBlank()) throw new IllegalArgumentException("endpoint is required");
        namespace = namespace == null ? "default" : namespace;
        if (epoch == null || epoch.isBlank()) throw new IllegalArgumentException("epoch is required");
        if (token != null && (token.isEmpty() || !token.chars().allMatch(c -> c >= 0x21 && c <= 0x7e)))
            throw new IllegalArgumentException("token must contain printable ASCII header characters");
        if (requestTimeoutMillis < 1 || requestTimeoutMillis > 60_000) throw new IllegalArgumentException("request timeout must be 1..60000 ms");
        if (cleanupTimeoutMillis < 1 || cleanupTimeoutMillis > 60_000) throw new IllegalArgumentException("cleanup timeout must be 1..60000 ms");
        defaultAcquisition = defaultAcquisition == null ? AcquireOptions.DEFAULT : defaultAcquisition;
        validateTtl(defaultAcquisition.leaseMillis());
    }

    public ConsulProviderOptions(String endpoint, String namespace, String epoch) {
        this(endpoint, namespace, epoch, null, 2_000, 2_000, AcquireOptions.DEFAULT);
    }

    static int validateTtl(long millis) {
        if (millis < 10_000 || millis > 86_400_000) throw new IllegalArgumentException("Consul session TTL must be 10000..86400000 ms");
        return (int) ((millis + 999) / 1000);
    }
}
