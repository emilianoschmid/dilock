package org.dilock.consul;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.dilock.redis.AcquireOptions;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsulLiveTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "DILOCK_CONSUL_ENDPOINT", matches = ".+")
    void acquiresRenewsAndReleasesWithLockIndexFence() {
        String endpoint = System.getenv("DILOCK_CONSUL_ENDPOINT");
        String namespace = System.getenv("DILOCK_CONSUL_NAMESPACE");
        String epoch = System.getenv("DILOCK_CONSUL_EPOCH");
        String token = System.getenv("CONSUL_HTTP_TOKEN");
        var options = new ConsulProviderOptions(endpoint, namespace, epoch, token,
                2_000, 2_000, new AcquireOptions(Duration.ofSeconds(4), Duration.ofSeconds(12)));
        try (var provider = ConsulProvider.connect(options)) {
            try (var lease = provider.lock("java/live/" + UUID.randomUUID()).acquire()) {
                assertTrue(lease.fence().lockIndex() > 0);
                assertEquals(epoch, lease.fence().epoch());
                assertEquals(ConsulLease.ReleaseOutcome.RELEASED, lease.release());
            }
        }
    }
}
