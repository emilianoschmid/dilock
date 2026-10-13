package org.dilock.etcd;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

final class EtcdDefaultConnectLiveTest {
    @Test void defaultProviderConnectValidatesPreProvisionedMetadataAfterColdChannelStartup() {
        String endpoint = System.getenv("DILOCK_ETCD_ENDPOINT");
        String namespace = System.getenv("DILOCK_ETCD_NAMESPACE");
        String epoch = System.getenv("DILOCK_ETCD_EPOCH");
        Assumptions.assumeTrue(endpoint != null && namespace != null && epoch != null,
                "set DILOCK_ETCD_ENDPOINT, DILOCK_ETCD_NAMESPACE and DILOCK_ETCD_EPOCH for live etcd connect");

        try (EtcdProvider provider = EtcdProvider.connect(endpoint, namespace, epoch)) {
            assertNotNull(provider);
        }
    }
}
