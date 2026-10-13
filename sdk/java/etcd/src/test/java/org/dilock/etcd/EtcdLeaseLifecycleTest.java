package org.dilock.etcd;

import com.google.protobuf.ByteString;
import com.google.common.util.concurrent.SettableFuture;
import org.dilock.redis.DilockException;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EtcdLeaseLifecycleTest {
    @Test void lostAcknowledgementLiveTestRequiresItsDedicatedFaultProxySentinel() {
        Map<String, String> ordinaryEtcd = Map.of(
                "DILOCK_ETCD_ENDPOINT", "http://etcd:2379",
                "DILOCK_ETCD_NAMESPACE", "test",
                "DILOCK_ETCD_EPOCH", "epoch");
        assertEquals(false, EtcdAppliedDoubleLostAckLiveTest.isEnabled(ordinaryEtcd));

        Map<String, String> faultProxy = Map.of(
                "DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT", "http://fault-proxy:2379",
                "DILOCK_ETCD_NAMESPACE", "test",
                "DILOCK_ETCD_EPOCH", "epoch");
        assertEquals(true, EtcdAppliedDoubleLostAckLiveTest.isEnabled(faultProxy));
        assertEquals(false, EtcdAppliedDoubleLostAckLiveTest.isEnabled(Map.of(
                "DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT", " ",
                "DILOCK_ETCD_NAMESPACE", "test",
                "DILOCK_ETCD_EPOCH", "epoch")));
    }

    @Test void releaseUnlockRequiresPreflightProofOfExactKeyAndAttemptLease() {
        assertEquals(EtcdProvider.ReleaseOwnership.NOT_OWNED,
                EtcdProvider.classifyReleaseOwnership(false, false, 0, 17));
        assertEquals(EtcdProvider.ReleaseOwnership.OWNED,
                EtcdProvider.classifyReleaseOwnership(true, true, 17, 17));
        assertEquals(EtcdProvider.ReleaseOwnership.MISMATCH,
                EtcdProvider.classifyReleaseOwnership(true, false, 17, 17));
        assertEquals(EtcdProvider.ReleaseOwnership.MISMATCH,
                EtcdProvider.classifyReleaseOwnership(true, true, 18, 17));
    }

    @Test void releaseOutcomeRequiresExactOwnershipUnlockAndPositiveRevokeAcknowledgements() {
        assertEquals(EtcdLease.ReleaseOutcome.RELEASED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.OWNED, true,
                        EtcdProvider.RevokeObservation.ACKNOWLEDGED, false, null));
        assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.OWNED, false,
                        EtcdProvider.RevokeObservation.ACKNOWLEDGED, false, null));
        assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.NOT_OWNED, false,
                        EtcdProvider.RevokeObservation.NOT_FOUND, false, null));
        assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.MISMATCH, false,
                        EtcdProvider.RevokeObservation.ACKNOWLEDGED, false, null));
        assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.MISMATCH, false,
                        EtcdProvider.RevokeObservation.NOT_FOUND, false, null));
        assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.UNKNOWN, false,
                        EtcdProvider.RevokeObservation.UNKNOWN, true, null));

        DilockException unlockWithoutRevoke = assertThrows(DilockException.class,
                () -> EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.OWNED, true,
                        EtcdProvider.RevokeObservation.NOT_FOUND, false, null));
        assertEquals(DilockException.Kind.RELEASE_UNKNOWN, unlockWithoutRevoke.kind());
        DilockException unknownWithoutProof = assertThrows(DilockException.class,
                () -> EtcdProvider.classifyReleaseOutcome(EtcdProvider.ReleaseOwnership.UNKNOWN, false,
                        EtcdProvider.RevokeObservation.ACKNOWLEDGED, false, null));
        assertEquals(DilockException.Kind.RELEASE_UNKNOWN, unknownWithoutProof.kind());
    }

    @Test void revokeNotFoundIsDistinctFromAnUnknownBackendOutcome() {
        assertEquals(EtcdProvider.RevokeObservation.NOT_FOUND,
                EtcdProvider.classifyRevokeError(new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND)));
        assertEquals(EtcdProvider.RevokeObservation.NOT_FOUND,
                EtcdProvider.classifyRevokeError(new java.util.concurrent.ExecutionException(
                        new io.grpc.StatusException(io.grpc.Status.NOT_FOUND))));
        assertEquals(EtcdProvider.RevokeObservation.UNKNOWN,
                EtcdProvider.classifyRevokeError(new io.grpc.StatusRuntimeException(
                        io.grpc.Status.UNAVAILABLE)));
    }

    @Test void tryWithResourcesRequiresReleasedOutcomeAfterSuccessfulWork() {
        EtcdLease.requireReleasedScopeOutcome(EtcdLease.ReleaseOutcome.RELEASED);
        DilockException error = assertThrows(DilockException.class,
                () -> EtcdLease.requireReleasedScopeOutcome(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED));
        assertEquals(DilockException.Kind.LEASE_LOST, error.kind());
    }

    @Test void cleanupBudgetShrinksAcrossSequentialRpcPhases() {
        long start = TimeUnit.SECONDS.toNanos(10);
        long deadline = start + TimeUnit.SECONDS.toNanos(1);

        assertEquals(1_000, EtcdProvider.remainingCleanupMillis(deadline, start));
        assertEquals(250, EtcdProvider.remainingCleanupMillis(deadline, start + TimeUnit.MILLISECONDS.toNanos(750)));
        assertEquals(0, EtcdProvider.remainingCleanupMillis(deadline, deadline));
        assertEquals(0, EtcdProvider.remainingCleanupMillis(deadline, deadline + 1));
    }

    @Test void channelStartupBudgetShrinksMonotonicallyAndExpiresAtItsDeadline() {
        long start = TimeUnit.SECONDS.toNanos(15);
        long deadline = start + TimeUnit.SECONDS.toNanos(5);

        assertEquals(TimeUnit.SECONDS.toNanos(5), EtcdProvider.remainingChannelStartupNanos(deadline, start));
        assertEquals(TimeUnit.SECONDS.toNanos(2), EtcdProvider.remainingChannelStartupNanos(deadline, start + TimeUnit.SECONDS.toNanos(3)));
        assertEquals(0, EtcdProvider.remainingChannelStartupNanos(deadline, deadline));
        assertEquals(0, EtcdProvider.remainingChannelStartupNanos(deadline, deadline + 1));
    }

    @Test void acquisitionWaitDeadlineIsInclusiveAndMonotonic() {
        long deadline = TimeUnit.SECONDS.toNanos(12);
        assertEquals(false, EtcdProvider.waitBudgetExpired(deadline, deadline - 1));
        assertEquals(true, EtcdProvider.waitBudgetExpired(deadline, deadline));
        assertEquals(true, EtcdProvider.waitBudgetExpired(deadline, deadline + 1));
    }

    @Test void grantLockAndVerificationShareOneWaitDeadline() {
        long started = TimeUnit.SECONDS.toNanos(20);
        long deadline = started + TimeUnit.SECONDS.toNanos(5);
        assertEquals(5_000, EtcdProvider.remainingWaitMillis(deadline, started));
        assertEquals(2_000, EtcdProvider.remainingWaitMillis(deadline, started + TimeUnit.SECONDS.toNanos(3)));
        assertEquals(0, EtcdProvider.remainingWaitMillis(deadline, deadline));
    }

    @Test void activationCannotOverwriteKeepaliveLoss() {
        EtcdLease lease = new EtcdLease(null, 7, 1_000, System.nanoTime() + 1_000_000_000L);
        lease.markLostFromKeepAlive();

        DilockException error = assertThrows(DilockException.class,
                () -> lease.activate(ByteString.copyFromUtf8("/opaque/key"), 42));

        assertEquals(DilockException.Kind.ACQUISITION_UNKNOWN, error.kind());
        assertEquals(EtcdLease.State.LOST, lease.state());
    }

    @Test void delayedKeepaliveCannotRestoreExpiredConfirmedValidity() {
        EtcdLease lease = new EtcdLease(null, 8, 1_000, System.nanoTime() - 1);

        lease.confirmDeadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(1));

        assertEquals(EtcdLease.State.LOST, lease.state());
        assertTrue(lease.isKeepAliveStopping());
    }

    @Test void lostLeaseDoesNotClaimRemoteAbsenceWithoutCleanupAcknowledgement() {
        EtcdLease lease = new EtcdLease(null, 7, 1_000, System.nanoTime() - 1);

        RuntimeException error = assertThrows(RuntimeException.class, lease::release);

        if (error instanceof DilockException dilockError) {
            assertEquals(DilockException.Kind.RELEASE_UNKNOWN, dilockError.kind());
        }
        assertEquals(EtcdLease.State.RELEASED_UNKNOWN, lease.state());
    }

    @Test void releaseInterruptionIsRestoredAndCancelsTheRpc() {
        SettableFuture<String> rpc = SettableFuture.create();
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> EtcdProvider.await(rpc, 1_000));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(rpc.isCancelled());
        } finally {
            Thread.interrupted();
        }
    }

    @Test void cleanupRpcRunsWithPreexistingInterruptAndRestoresTheFlag() throws Exception {
        SettableFuture<String> rpc = SettableFuture.create();
        rpc.set("acknowledged");
        Thread.currentThread().interrupt();
        try {
            assertEquals("acknowledged", EtcdProvider.awaitCleanup(rpc, 1_000));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(!rpc.isCancelled());
        } finally {
            Thread.interrupted();
        }
    }

    @Test void cleanupRpcContinuesAfterInterruptArrivesDuringWait() throws Exception {
        SettableFuture<String> rpc = SettableFuture.create();
        Thread caller = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(25);
                caller.interrupt();
                Thread.sleep(25);
                rpc.set("acknowledged");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        });
        interrupter.start();
        try {
            assertEquals("acknowledged", EtcdProvider.awaitCleanup(rpc, 1_000));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(!rpc.isCancelled());
        } finally {
            Thread.interrupted();
            interrupter.join();
        }
    }

    @Test void repeatedUnknownReleaseDoesNotRetryCleanup() {
        EtcdLease lease = new EtcdLease(null, 7, 1_000, System.nanoTime() + 1_000_000_000L);
        lease.activate(ByteString.copyFromUtf8("/opaque/key"), 42);

        assertThrows(NullPointerException.class, lease::release);
        DilockException error = assertThrows(DilockException.class, lease::release);

        assertEquals(DilockException.Kind.RELEASE_UNKNOWN, error.kind());
        assertEquals(EtcdLease.State.RELEASED_UNKNOWN, lease.state());
    }
}
