package org.dilock.etcd;

import org.dilock.redis.AcquireOptions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EtcdLiveReleaseTest {
    @Test void currentEtcdAdapterReleasesLiveNativeLockAndDoesNotReplayConfirmedRelease() throws Exception {
        String endpoint = System.getenv("DILOCK_ETCD_ENDPOINT");
        String namespace = System.getenv("DILOCK_ETCD_NAMESPACE");
        String epoch = System.getenv("DILOCK_ETCD_EPOCH");
        Assumptions.assumeTrue(endpoint != null && namespace != null && epoch != null,
                "set DILOCK_ETCD_ENDPOINT, DILOCK_ETCD_NAMESPACE and DILOCK_ETCD_EPOCH for live etcd release");

        var options = new EtcdProviderOptions(endpoint, namespace, epoch, 5_000, 5_000,
                new AcquireOptions(5_000, 15_000), null);
        try (EtcdProvider provider = EtcdProvider.connect(options)) {
            var lock = provider.lock("current-release-" + java.util.UUID.randomUUID());
            EtcdLease first = lock.acquire(new AcquireOptions(5_000, 15_000));
            long firstFence = first.fence().value();
            assertEquals(EtcdLease.State.HELD, first.state());
            assertTrue(firstFence > 0);
            assertEquals(EtcdLease.ReleaseOutcome.RELEASED, first.release());
            assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED, first.release());

            EtcdLease successor = lock.acquire(new AcquireOptions(5_000, 15_000));
            assertTrue(successor.fence().value() > firstFence);
            assertEquals(EtcdLease.ReleaseOutcome.RELEASED, successor.release());
            assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED, successor.release());
        }
    }

    @Test void expiredLeaseReleaseConfirmsMissingExactKeyBeforeReturningAlreadyNotOwned() throws Exception {
        String endpoint = System.getenv("DILOCK_ETCD_ENDPOINT");
        String namespace = System.getenv("DILOCK_ETCD_NAMESPACE");
        String epoch = System.getenv("DILOCK_ETCD_EPOCH");
        Assumptions.assumeTrue(endpoint != null && namespace != null && epoch != null,
                "set DILOCK_ETCD_ENDPOINT, DILOCK_ETCD_NAMESPACE and DILOCK_ETCD_EPOCH for live etcd release");

        var options = new EtcdProviderOptions(endpoint, namespace, epoch, 250, 1_000,
                new AcquireOptions(2_000, 2_000), null);
        try (EtcdProvider provider = EtcdProvider.connect(options)) {
            var lock = provider.lock("expired-release-" + java.util.UUID.randomUUID());
            EtcdLease expired = lock.acquire(2_000, 2_000, false);
            Thread.sleep(2_500);

            assertEquals(EtcdLease.State.LOST, expired.state());
            assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED, expired.release());

            EtcdLease successor = lock.acquire(2_000, 2_000);
            assertEquals(EtcdLease.ReleaseOutcome.RELEASED, successor.release());
        }
    }
}
