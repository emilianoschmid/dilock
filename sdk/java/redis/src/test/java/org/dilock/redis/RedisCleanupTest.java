package org.dilock.redis;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RedisCleanupTest {
    @Test void cleanupClientTimeoutIsAppliedToConnectAndSocketBudgets() {
        DefaultJedisClientConfig config = RedisProvider.clientConfig(URI.create("rediss://user:secret@redis.example:6380/2"), 73);

        assertEquals(73, config.getConnectionTimeoutMillis());
        assertEquals(73, config.getSocketTimeoutMillis());
        assertEquals("user", config.getUser());
        assertEquals("secret", config.getPassword());
        assertEquals(2, config.getDatabase());
        assertTrue(config.isSsl());
    }

    @Test void ambiguousOwnerCleanupReportsUnknownAndNeverReplacesPrimaryOutcome() {
        List<CleanupObservation> observations = new ArrayList<>();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                AcquireOptions.DEFAULT, observation -> {
                    observations.add(observation);
                    throw new IllegalStateException("observer failure");
                }, (key, owner) -> { throw new IllegalStateException("acknowledgement lost"); });

        provider.cleanupOwner("lease-key", "owner-token", "acquisition-unknown");

        assertEquals(1, observations.size());
        assertEquals("redis", observations.getFirst().backend());
        assertEquals("unknown", observations.getFirst().outcome());
        assertEquals("acquisition-unknown: IllegalStateException", observations.getFirst().reason());
    }

    @Test void unknownScopeCleanupDoesNotReplaceWorkFailure() {
        List<CleanupObservation> observations = new ArrayList<>();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                AcquireOptions.DEFAULT, observations::add,
                (key, owner) -> { throw new IllegalStateException("release acknowledgement lost"); });
        RedisLease lease = new RedisLease(provider, "lease-key", "owner-token", 1_000, System.nanoTime() + 1_000_000_000L);
        RuntimeException workFailure = new IllegalArgumentException("work failed");

        RuntimeException actual = assertThrows(IllegalArgumentException.class, () -> {
            try (lease) {
                throw workFailure;
            }
        });

        assertSame(workFailure, actual);
        assertEquals(RedisLease.State.RELEASED_UNKNOWN, lease.state());
        assertEquals("unknown", observations.getFirst().outcome());
        assertEquals("scope-close", observations.getFirst().reason());
    }

    @Test void alreadyNotOwnedScopeFailsClosedAndPreservesBodyFailure() {
        List<CleanupObservation> observations = new ArrayList<>();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                AcquireOptions.DEFAULT, observations::add, (key, owner) -> 0L);
        RedisLease lease = new RedisLease(provider, "lease-key", "owner-token", 1_000,
                System.nanoTime() + 1_000_000_000L);

        org.dilock.redis.DilockException error = assertThrows(org.dilock.redis.DilockException.class, lease::close);

        assertEquals(org.dilock.redis.DilockException.Kind.LEASE_LOST, error.kind());
        assertEquals("already_not_owned", observations.getFirst().outcome());
        assertEquals("scope-close", observations.getFirst().reason());

        RedisLease bodyFailureLease = new RedisLease(provider, "lease-key", "owner-token", 1_000,
                System.nanoTime() + 1_000_000_000L);
        IllegalStateException bodyFailure = new IllegalStateException("work failed");
        IllegalStateException actual = assertThrows(IllegalStateException.class, () -> {
            try (bodyFailureLease) { throw bodyFailure; }
        });
        assertSame(bodyFailure, actual);
        assertEquals(org.dilock.redis.DilockException.Kind.LEASE_LOST,
                ((org.dilock.redis.DilockException) actual.getSuppressed()[0]).kind());
    }
}
