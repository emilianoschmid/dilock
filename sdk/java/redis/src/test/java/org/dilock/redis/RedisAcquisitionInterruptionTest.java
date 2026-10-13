package org.dilock.redis;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class RedisAcquisitionInterruptionTest {
    private void interruptedAcknowledgedSet(Object cleanupReply, boolean cleanupFails,
                                             boolean expectedUnknown) {
        List<CleanupObservation> observations = new ArrayList<>();
        AtomicInteger acquisitions = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 100, 17,
                AcquireOptions.DEFAULT, observations::add, (key, owner) -> {
                    cleanups.incrementAndGet();
                    assertTrue(owner.startsWith("dilock/1/"));
                    // A cleanup transport may consume interruption; the SDK must restore it.
                    Thread.interrupted();
                    if (cleanupFails) throw new IllegalStateException("cleanup ACK lost");
                    return cleanupReply;
                }, (key, owner, ttl) -> {
                    acquisitions.incrementAndGet();
                    Thread.currentThread().interrupt();
                    return "OK";
                });
        try {
            RedisAcquisitionCancelledException error = assertThrows(RedisAcquisitionCancelledException.class,
                    () -> provider.lock("invoice/42").tryAcquire(1_000));
            assertEquals(expectedUnknown, error.remoteOutcomeUnknown());
            assertTrue(Thread.currentThread().isInterrupted(), "interruption flag must remain set");
            assertEquals(1, acquisitions.get(), "ambiguous acquisition must never be replayed");
            assertEquals(1, cleanups.get(), "only one owner-specific cleanup may be attempted");
            assertEquals(expectedUnknown ? "unknown" : Long.valueOf(1).equals(cleanupReply)
                    ? "released" : "already_not_owned", observations.getFirst().outcome());
        } finally {
            Thread.interrupted();
        }
    }

    @Test void acknowledgedOwnerRemovalReportsKnownCancellation() {
        interruptedAcknowledgedSet(1L, false, false);
    }

    @Test void acknowledgedOwnerMismatchReportsKnownCancellation() {
        interruptedAcknowledgedSet(0L, false, false);
    }

    @Test void lostCleanupAcknowledgementReportsUnknownCancellation() {
        interruptedAcknowledgedSet(null, true, true);
    }

    @Test void malformedCleanupReplyCannotConfirmCancellation() {
        interruptedAcknowledgedSet(null, false, true);
    }

    @Test void acknowledgedContentionNeedsNoOwnerCleanup() {
        AtomicInteger cleanups = new AtomicInteger();
        RedisProvider provider = new RedisProvider(null, null, "default", "epoch", 100, 17,
                AcquireOptions.DEFAULT, null, (key, owner) -> {
                    cleanups.incrementAndGet(); return 1L;
                }, (key, owner, ttl) -> { Thread.currentThread().interrupt(); return null; });
        try {
            RedisAcquisitionCancelledException error = assertThrows(RedisAcquisitionCancelledException.class,
                    () -> provider.lock("invoice/42").tryAcquire(1_000));
            assertFalse(error.remoteOutcomeUnknown());
            assertEquals(0, cleanups.get());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }
}
