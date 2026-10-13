package org.dilock.redis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import redis.clients.jedis.RedisClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisLostReleaseTest {
    @Test void backendFailureDuringRenewalPermanentlyLosesLocalLease() {
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                AcquireOptions.DEFAULT, observation -> { }, (key, owner) -> 0L);
        RedisLease lease = new RedisLease(provider, "lease-key", "owner-token", 5_000,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(5));

        DilockException failure = assertThrows(DilockException.class, () -> lease.renew(5_000));

        assertEquals(DilockException.Kind.BACKEND_UNAVAILABLE, failure.kind());
        assertEquals(RedisLease.State.LOST, lease.state(),
                "a renewal with an uncertain backend outcome must not leave local validity held");
        DilockException retry = assertThrows(DilockException.class, () -> lease.renew(5_000));
        assertEquals(DilockException.Kind.LEASE_LOST, retry.kind(),
                "a lost generation must not retry renewal");
    }

    @Test void appliedRedisReleaseWithInterruptedAcknowledgementIsTerminal() throws Exception {
        String endpoint = System.getenv("DILOCK_REDIS_URL");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "set DILOCK_REDIS_URL for the live Redis ACK-withholding regression");

        String key = "dilock:test:lost-ack:" + java.util.UUID.randomUUID();
        String owner = "dilock/1/" + "ab".repeat(32);
        var applied = new CountDownLatch(1);
        var backendCalls = new AtomicInteger();
        var releaseThread = new AtomicReference<Thread>();
        try (RedisClient backend = RedisClient.create(endpoint)) {
            assertEquals("OK", backend.set(key, owner));
            RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                    AcquireOptions.DEFAULT, observation -> { }, (resourceKey, token) -> {
                        backendCalls.incrementAndGet();
                        Object result = backend.eval(RedisProvider.RELEASE_LUA, List.of(resourceKey), List.of(token));
                        if (!Long.valueOf(1).equals(result)) throw new AssertionError("server did not apply owner-checked release");
                        applied.countDown();
                        try {
                            new CountDownLatch(1).await();
                            throw new AssertionError("unreachable");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("release acknowledgement was interrupted after Redis applied EVAL", interrupted);
                        }
                    });
            RedisLease lease = new RedisLease(provider, key, owner, 1_000, System.nanoTime() + 1_000_000_000L);
            Thread thread = new Thread(() -> {
                try { lease.release(); }
                catch (RuntimeException expectedUnknown) { }
            }, "dilock-redis-applied-release-ack-withheld");
            releaseThread.set(thread);
            thread.start();

            if (!applied.await(5, TimeUnit.SECONDS)) throw new AssertionError("real Redis release EVAL did not apply");
            assertEquals(false, backend.exists(key), "Redis must have removed the key before ACK is withheld");
            thread.interrupt();
            thread.join(5_000);
            assertEquals(false, thread.isAlive(), "interrupted release must finish");
            assertEquals(RedisLease.State.RELEASED_UNKNOWN, lease.state());
            DilockException unknown = assertThrows(DilockException.class, lease::release);
            assertEquals(DilockException.Kind.RELEASE_UNKNOWN, unknown.kind());
            assertEquals(1, backendCalls.get(), "terminal UNKNOWN must not replay EVAL");
        } finally {
            Thread thread = releaseThread.get();
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                thread.join(1_000);
            }
        }
    }

    @Test void lostLeaseDoesNotClaimRemoteAbsenceWithoutOwnerCheckAcknowledgement() {
        RedisLease lease = new RedisLease(null, "lease-key", "owner-value", 1_000, System.nanoTime() - 1);

        RuntimeException error = assertThrows(RuntimeException.class, lease::release);

        if (error instanceof DilockException dilockError) {
            assertEquals(DilockException.Kind.RELEASE_UNKNOWN, dilockError.kind());
        }
        assertEquals(RedisLease.State.RELEASED_UNKNOWN, lease.state());
    }

    @Test void concurrentReleaseDoesNotReplayAfterAmbiguousAcknowledgement() throws Exception {
        var enteredBackend = new CountDownLatch(1);
        var secondCallStarted = new CountDownLatch(1);
        var allowBackendReturn = new CountDownLatch(1);
        var backendCalls = new AtomicInteger();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 2_000, 17,
                AcquireOptions.DEFAULT, observation -> { }, (key, owner) -> {
                    backendCalls.incrementAndGet();
                    enteredBackend.countDown();
                    try {
                        if (!allowBackendReturn.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test timed out");
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("test interrupted", error);
                    }
                    throw new IllegalStateException("release acknowledgement lost");
                });
        RedisLease lease = new RedisLease(provider, "lease-key", "owner-token", 1_000, System.nanoTime() + 1_000_000_000L);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            var first = threads.submit(() -> releaseKind(lease));
            if (!enteredBackend.await(2, TimeUnit.SECONDS)) throw new AssertionError("first release did not reach backend");
            var second = threads.submit(() -> {
                secondCallStarted.countDown();
                return releaseKind(lease);
            });
            if (!secondCallStarted.await(2, TimeUnit.SECONDS)) throw new AssertionError("second release did not start");
            allowBackendReturn.countDown();

            assertEquals(DilockException.Kind.RELEASE_UNKNOWN, first.get(2, TimeUnit.SECONDS));
            assertEquals(DilockException.Kind.RELEASE_UNKNOWN, second.get(2, TimeUnit.SECONDS));
            assertEquals(1, backendCalls.get());
        } finally {
            allowBackendReturn.countDown();
            threads.shutdownNow();
        }
    }

    private static DilockException.Kind releaseKind(RedisLease lease) {
        try {
            lease.release();
            throw new AssertionError("expected unknown release outcome");
        } catch (DilockException error) {
            return error.kind();
        }
    }
}
