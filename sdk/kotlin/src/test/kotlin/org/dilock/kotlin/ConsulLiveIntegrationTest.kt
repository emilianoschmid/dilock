package org.dilock.kotlin

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID

/** Opt-in live test of the Consul facade over the shared Java session adapter. */
class ConsulLiveIntegrationTest {
    @Test
    fun independentProvidersContendAndAdvanceConsulLockIndex() {
        val endpoint = System.getenv("DILOCK_TEST_CONSUL_ENDPOINT")
        val namespace = System.getenv("DILOCK_TEST_NAMESPACE")
        val epoch = System.getenv("DILOCK_TEST_EPOCH")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_CONSUL_ENDPOINT to opt in to live Consul")
        assumeTrue(!namespace.isNullOrBlank(), "set DILOCK_TEST_NAMESPACE to an explicitly provisioned namespace")
        assumeTrue(!epoch.isNullOrBlank(), "set DILOCK_TEST_EPOCH to the provisioned coordination epoch")

        val firstProvider = DilockConsul.connect(endpoint!!, namespace!!, epoch!!)
        val secondProvider = DilockConsul.connect(endpoint, namespace, epoch)
        var first: ConsulLease? = null
        var second: ConsulLease? = null
        try {
            val resource = "kotlin-consul/${UUID.randomUUID()}"
            val policy = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10))
            val original = firstProvider.lock(resource).acquire(policy)
            first = original
            val firstFence = original.fence()
            assertEquals(epoch, firstFence.epoch)
            assertTrue(firstFence.lockIndex > 0)
            assertNull(secondProvider.lock(resource).tryAcquire(), "an independent provider must observe contention")

            assertEquals(ReleaseOutcome.RELEASED, original.release())
            first = null

            val successor = secondProvider.lock(resource).acquire(policy)
            second = successor
            val nextFence = successor.fence()
            assertEquals(epoch, nextFence.epoch)
            assertTrue(nextFence.lockIndex > firstFence.lockIndex, "retained KV LockIndex must advance on handoff")
            assertEquals(ReleaseOutcome.RELEASED, successor.release())
            second = null
        } finally {
            second?.close()
            first?.close()
            secondProvider.close()
            firstProvider.close()
        }
    }
}
