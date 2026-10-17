package org.dilock.consul;

import com.fasterxml.jackson.databind.JsonNode;
import org.dilock.redis.DilockException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsulContractTest {
    @Test void namespaceMetadataMustHaveExactlyTheFiveProfileFields() throws Exception {
        JsonNode exact = ConsulProvider.JSON.readTree("""
                {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",
                 "profile":"consul-kv-session-v1","coordination_epoch":"epoch-a","fixed_lease_ms":null}
                """);
        JsonNode extra = ConsulProvider.JSON.readTree("""
                {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",
                 "profile":"consul-kv-session-v1","coordination_epoch":"epoch-a","fixed_lease_ms":null,
                 "future_field":"unsupported"}
                """);
        assertEquals(true, ConsulProvider.isExactMetadata(exact, "epoch-a"));
        assertEquals(false, ConsulProvider.isExactMetadata(extra, "epoch-a"));
    }

    @Test void sessionTtlIsValidatedAndRoundedUpBeforeNetworkUse() {
        assertEquals(11, ConsulProviderOptions.validateTtl(10_001));
        assertEquals(30, ConsulProviderOptions.validateTtl(30_000));
        assertThrows(IllegalArgumentException.class, () -> ConsulProviderOptions.validateTtl(9_999));
        assertThrows(IllegalArgumentException.class, () -> ConsulProviderOptions.validateTtl(86_400_001));
    }

    @Test void fenceRequiresPositiveRetainedKeyLockIndex() {
        assertEquals(new ConsulFence("epoch-a", 4), new ConsulFence("epoch-a", 4));
        assertThrows(IllegalArgumentException.class, () -> new ConsulFence("epoch-a", 0));
    }

    @Test void releaseFalseNeedsAValidLinearizableNonOwnerObservation() throws Exception {
        JsonNode otherOwner = ConsulProvider.JSON.readTree("""
                [{"Key":"dilock/locks/resource","Session":"successor","LockIndex":2}]
                """);
        JsonNode malformed = ConsulProvider.JSON.readTree("[{\"Session\":\"successor\"}]");
        assertEquals(ConsulLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                ConsulLease.classifyReleaseFalse(otherOwner, "dilock/locks/resource", "stale"));
        assertEquals(ConsulLease.ReleaseOutcome.ALREADY_NOT_OWNED,
                ConsulLease.classifyReleaseFalse(null, "dilock/locks/resource", "stale"));
        assertThrows(DilockException.class,
                () -> ConsulLease.classifyReleaseFalse(malformed, "dilock/locks/resource", "stale"));
        assertThrows(DilockException.class,
                () -> ConsulLease.classifyReleaseFalse(otherOwner, "dilock/locks/resource", "successor"));
    }

    @Test void releaseFailureStillAttemptsSessionDestruction() {
        boolean[] destroyed = {false};
        assertThrows(IllegalStateException.class, () -> ConsulLease.releaseWithSessionCleanup(() -> {
            throw new IllegalStateException("owner read failed");
        }, () -> destroyed[0] = true));
        assertTrue(destroyed[0]);
    }
}
