package org.dilock.redis;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisAppliedSetInterruptionLiveTest {
    @Test void interruptionAfterRedisAppliedSetCleansOnlyTheAttemptOwner() throws Exception {
        exerciseAppliedSetInterruption(false);
    }

    @Test void interruptionAfterRedisAppliedSetReportsLostCleanupAcknowledgement() throws Exception {
        exerciseAppliedSetInterruption(true);
    }

    private void exerciseAppliedSetInterruption(boolean dropCleanupAcknowledgement) throws Exception {
        String endpoint = System.getenv("DILOCK_REDIS_URL");
        String verificationEndpoint = System.getenv("DILOCK_REDIS_VERIFY_URL");
        String proxyControl = System.getenv("DILOCK_REDIS_PROXY_CONTROL_URL");
        Assumptions.assumeTrue(endpoint != null && verificationEndpoint != null && proxyControl != null,
                "set Redis endpoints and proxy control URL for the applied-SET interruption regression");

        String namespace = "java-cancel-set-" + UUID.randomUUID().toString().replace("-", "");
        String epoch = "java-cancel-set-epoch";
        String resource = "cancel-after-applied-set";
        String metadataKey = "dilock:namespace:" + namespace + ":meta";
        String key = Identity.redisKey(Identity.resourceId(namespace, resource));
        AtomicReference<Throwable> workerOutcome = new AtomicReference<>();
        Thread worker = null;

        try (RedisClient admin = RedisClient.create(verificationEndpoint)) {
            try {
                assertEquals("created", RedisProvider.provisionNamespace(admin, namespace, epoch));
                try (RedisProvider provider = RedisProvider.connect(new RedisProviderOptions(
                        endpoint, namespace, epoch, Duration.ofSeconds(2), Duration.ofSeconds(2),
                        new AcquireOptions(1_000, 30_000), null))) {
                    proxyCommand(proxyControl, "RESET");
                    worker = new Thread(() -> {
                        try {
                            provider.lock(resource).tryAcquire(30_000);
                            workerOutcome.set(new AssertionError("interrupted applied SET must not return a lease"));
                        } catch (InterruptedException expected) {
                            workerOutcome.set(expected);
                        } catch (Throwable error) {
                            workerOutcome.set(error);
                        }
                    }, "dilock-java-redis-applied-set-interruption");
                    worker.start();

                    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    String stats;
                    do {
                        stats = proxyCommand(proxyControl, "STATS");
                        if (stats.contains("\"delayed_set_count\": 1")) break;
                        Thread.sleep(5);
                    } while (System.nanoTime() < deadline);
                    assertNotNull(stats);
                    assertTrue(stats.contains("\"delayed_set_count\": 1"),
                            "fault proxy must observe Redis apply SET before interrupting the caller: " + stats);

                    if (dropCleanupAcknowledgement) proxyCommand(proxyControl, "ARM");
                    worker.interrupt();
                    worker.join(5_000);
                    assertFalse(worker.isAlive(), "interrupted acquisition must finish after delayed ACK");
                    assertEquals(RedisAcquisitionCancelledException.class, workerOutcome.get().getClass(),
                            "confirmed owner cleanup must preserve Java interruption");
                    assertEquals(dropCleanupAcknowledgement,
                            ((RedisAcquisitionCancelledException) workerOutcome.get()).remoteOutcomeUnknown(),
                            "typed interruption must retain owner cleanup acknowledgement certainty");

                    stats = proxyCommand(proxyControl, "STATS");
                    assertTrue(stats.contains("\"eval_count\": 1"), "exactly one owner cleanup EVAL expected: " + stats);
                    assertTrue(stats.contains("\"dropped_count\": " + (dropCleanupAcknowledgement ? "1" : "0")),
                            "proxy cleanup ACK-loss evidence did not match the requested fault: " + stats);
                    assertNull(admin.get(key), "owner-checked cleanup must remove the applied lease key");
                }
            } finally {
                admin.del(metadataKey);
            }
        } finally {
            if (worker != null && worker.isAlive()) {
                worker.interrupt();
                worker.join(1_000);
            }
        }
    }

    private static String proxyCommand(String endpoint, String command) throws Exception {
        String authority = endpoint.startsWith("tcp://") ? endpoint.substring("tcp://".length()) : endpoint;
        String[] parts = authority.split(":", 2);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), 2_000);
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write((command + "\n").getBytes(StandardCharsets.US_ASCII));
            return new String(socket.getInputStream().readNBytes(4096), StandardCharsets.UTF_8);
        }
    }
}
