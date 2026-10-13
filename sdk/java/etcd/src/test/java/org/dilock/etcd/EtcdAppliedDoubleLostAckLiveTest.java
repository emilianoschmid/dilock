package org.dilock.etcd;

import org.dilock.redis.AcquireOptions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EtcdAppliedDoubleLostAckLiveTest {
    static boolean isEnabled(Map<String, String> environment) {
        String faultProxyEndpoint = environment.get("DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT");
        String namespace = environment.get("DILOCK_ETCD_NAMESPACE");
        String epoch = environment.get("DILOCK_ETCD_EPOCH");
        return faultProxyEndpoint != null && !faultProxyEndpoint.isBlank()
                && namespace != null && !namespace.isBlank()
                && epoch != null && !epoch.isBlank();
    }

    @Test void appliedUnlockLostAcknowledgementPreservesReleaseOutcomeAndSuccessorFence() throws Exception {
        String endpoint = System.getenv("DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT");
        String namespace = System.getenv("DILOCK_ETCD_NAMESPACE");
        String epoch = System.getenv("DILOCK_ETCD_EPOCH");
        Assumptions.assumeTrue(isEnabled(System.getenv()),
                "set DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT, DILOCK_ETCD_NAMESPACE and DILOCK_ETCD_EPOCH for the lost-ACK fault proxy");

        var options = new EtcdProviderOptions(endpoint, namespace, epoch, 2_000, 2_000,
                new AcquireOptions(5_000, 15_000), null);
        try (EtcdProvider provider = EtcdProvider.connect(options)) {
            var lock = provider.lock("applied-double-lost-cleanup-ack");
            EtcdLease owner = lock.acquire(new AcquireOptions(5_000, 15_000));
            long ownerFence = owner.fence().value();

            // The fault proxy always loses the first successful Unlock ACK and
            // can either acknowledge or lose the subsequent applied Revoke.
            // In both modes, cleanup is confirmed without claiming Unlock was
            // acknowledged; the successor must receive a greater native fence.
            assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED, owner.release());
            assertEquals(EtcdLease.State.RELEASED, owner.state());
            assertEquals(EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED, owner.release());

            EtcdLease successor = lock.acquire(new AcquireOptions(5_000, 15_000));
            assertTrue(successor.fence().value() > ownerFence);
            assertEquals(EtcdLease.ReleaseOutcome.RELEASED, successor.release());
        }
    }
}
