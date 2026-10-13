package org.dilock.examples;

import org.dilock.redis.DilockException;
import org.dilock.redis.RedisLease;
import org.dilock.redis.RedisLock;
import org.dilock.redis.RedisProvider;

import java.util.Optional;
import java.util.UUID;

/** A standalone consumer demonstrating contention and an acknowledged handoff. */
public final class RedisConsumer {
    private static final long WAIT_MILLIS = 2_000;
    private static final long LEASE_MILLIS = 5_000;

    private RedisConsumer() {}

    public static void main(String[] args) throws InterruptedException {
        String endpoint = requiredEnvironment("DILOCK_REDIS_URL");
        String namespace = requiredEnvironment("DILOCK_NAMESPACE");
        String epoch = requiredEnvironment("DILOCK_EPOCH");
        String resource = "docs/java-consumer/" + UUID.randomUUID();

        try (RedisProvider ownerProvider = RedisProvider.connect(endpoint, namespace, epoch);
             RedisProvider contenderProvider = RedisProvider.connect(endpoint, namespace, epoch)) {
            RedisLock ownerLock = ownerProvider.lock(resource);
            RedisLock contenderLock = contenderProvider.lock(resource);

            try (RedisLease owner = ownerLock.acquire(WAIT_MILLIS, LEASE_MILLIS)) {
                owner.assertValid();
                Optional<RedisLease> contention = contenderLock.tryAcquire(LEASE_MILLIS);
                if (contention.isPresent()) {
                    contention.get().release();
                    throw new IllegalStateException("second provider acquired while the owner lease was held");
                }
                System.out.println("contender: busy (acknowledged contention)");
            }

            try (RedisLease successor = contenderLock.acquire(WAIT_MILLIS, LEASE_MILLIS)) {
                successor.assertValid();
                System.out.println("successor: acquired after owner release");
            }
        } catch (DilockException error) {
            System.err.println("Dilock failed: " + error.kind() + ": " + error.getMessage());
            throw error;
        } catch (InterruptedException interrupted) {
            // Preserve the signal for callers that wrap this main flow in a worker thread.
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set the required environment variable " + name);
        }
        return value;
    }
}
