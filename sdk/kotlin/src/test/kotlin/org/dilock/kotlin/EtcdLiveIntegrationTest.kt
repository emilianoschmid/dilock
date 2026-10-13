package org.dilock.kotlin

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.time.Duration

/** Opt-in test against the native etcd Lock adapter used by real Kotlin providers. */
class EtcdLiveIntegrationTest {
    @Test fun independentProvidersQueueAndReturnMonotonicEpochScopedFences() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
        val namespace = System.getenv("DILOCK_TEST_NAMESPACE")
        val epoch = System.getenv("DILOCK_TEST_EPOCH")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_ETCD_ENDPOINT to opt in to live etcd")
        assumeTrue(!namespace.isNullOrBlank(), "set DILOCK_TEST_NAMESPACE to an explicitly provisioned namespace")
        assumeTrue(!epoch.isNullOrBlank(), "set DILOCK_TEST_EPOCH to the provisioned coordination epoch")

        val settings = ProviderSettings(
            endpoint = endpoint!!,
            namespace = namespace!!,
            epoch = epoch!!,
            // The first gRPC call includes cold channel setup in Docker. Keep
            // the test bounded while allowing the client to establish its channel.
            rpcTimeout = Duration.ofSeconds(5),
            cleanupTimeout = Duration.ofSeconds(2),
            defaultAcquisition = AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(30)),
        )
        val firstProvider = DilockEtcd.connect(settings)
        var secondProvider: EtcdProvider? = null
        var first: EtcdLease? = null
        var second: EtcdLease? = null
        try {
            secondProvider = DilockEtcd.connect(settings)
            val acquiredFirst = firstProvider.lock("kotlin-live-fence").acquire()
            first = acquiredFirst
            val firstFence = acquiredFirst.fence()
            assertEquals(epoch, firstFence.epoch())
            assertTrue(firstFence.value() > 0)

            val started = CompletableDeferred<Unit>()
            val queued = async(Dispatchers.IO) {
                started.complete(Unit)
                checkNotNull(secondProvider).lock("kotlin-live-fence").acquire(
                    AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(10))
                )
            }
            started.await()
            delay(250)
            assertFalse(queued.isCompleted, "a second provider must queue behind the active native Lock owner")

            first.release()
            first = null
            val acquiredSecond = withTimeout(5_000) { queued.await() }
            second = acquiredSecond
            val secondFence = acquiredSecond.fence()
            assertEquals(epoch, secondFence.epoch())
            assertTrue(secondFence.value() > firstFence.value(), "etcd create_revision must advance on handoff")
            acquiredSecond.release()
            second = null
        } finally {
            second?.close()
            first?.close()
            secondProvider?.close()
            firstProvider.close()
        }
    }

    @Test fun staleExpiredOwnerReleaseDoesNotUnlockEtcdSuccessor() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
        val namespace = System.getenv("DILOCK_TEST_NAMESPACE")
        val epoch = System.getenv("DILOCK_TEST_EPOCH")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_ETCD_ENDPOINT to opt in to live etcd")
        assumeTrue(!namespace.isNullOrBlank(), "set DILOCK_TEST_NAMESPACE to an explicitly provisioned namespace")
        assumeTrue(!epoch.isNullOrBlank(), "set DILOCK_TEST_EPOCH to the provisioned coordination epoch")

        val settings = ProviderSettings(
            endpoint = endpoint!!,
            namespace = namespace!!,
            epoch = epoch!!,
            rpcTimeout = Duration.ofSeconds(5),
            cleanupTimeout = Duration.ofSeconds(2),
            defaultAcquisition = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(6)),
        )
        val firstProvider = DilockEtcd.connect(settings)
        var secondProvider: EtcdProvider? = null
        var expiredOwner: EtcdLease? = null
        var successor: EtcdLease? = null
        try {
            secondProvider = DilockEtcd.connect(settings)
            val lockName = "kotlin-stale-expired-release-${java.util.UUID.randomUUID()}"
            val owner = firstProvider.lock(lockName).acquire(
                EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(6), autoRenew = false)
            )
            expiredOwner = owner

            delay(6_500)
            val acquiredSuccessor = checkNotNull(secondProvider).lock(lockName).acquire(
                EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(6), autoRenew = false)
            )
            successor = acquiredSuccessor

            assertEquals(ReleaseOutcome.ALREADY_NOT_OWNED, owner.release())
            expiredOwner = null
            // A stale Unlock must not remove the new generation's native etcd lock.
            assertEquals(ReleaseOutcome.RELEASED, acquiredSuccessor.release())
            successor = null
        } finally {
            successor?.close()
            expiredOwner?.close()
            secondProvider?.close()
            firstProvider.close()
        }
    }

    @Test fun optInHeldLeaseRenewalKeepsEtcdOwnershipPastInitialTtl() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
        val namespace = System.getenv("DILOCK_TEST_NAMESPACE")
        val epoch = System.getenv("DILOCK_TEST_EPOCH")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_ETCD_ENDPOINT to opt in to live etcd")
        assumeTrue(!namespace.isNullOrBlank(), "set DILOCK_TEST_NAMESPACE to an explicitly provisioned namespace")
        assumeTrue(!epoch.isNullOrBlank(), "set DILOCK_TEST_EPOCH to the provisioned coordination epoch")

        val settings = ProviderSettings(
            endpoint = endpoint!!,
            namespace = namespace!!,
            epoch = epoch!!,
            rpcTimeout = Duration.ofSeconds(5),
            cleanupTimeout = Duration.ofSeconds(2),
            defaultAcquisition = AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(30)),
        )
        val firstProvider = DilockEtcd.connect(settings)
        var secondProvider: EtcdProvider? = null
        var owner: EtcdLease? = null
        var successor: EtcdLease? = null
        try {
            secondProvider = DilockEtcd.connect(settings)
            owner = firstProvider.lock("kotlin-live-auto-renew").acquire(
                EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(6), autoRenew = true)
            )
            delay(6_500)
            owner.assertValid()

            val queued = async(Dispatchers.IO) {
                checkNotNull(secondProvider).lock("kotlin-live-auto-renew").acquire(
                    AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(10))
                )
            }
            delay(250)
            assertFalse(queued.isCompleted, "renewed server lease must retain ownership beyond its initial TTL")

            owner.release()
            owner = null
            successor = withTimeout(5_000) { queued.await() }
            assertTrue(successor.fence().value() > 0)
            successor.release()
            successor = null
        } finally {
            successor?.close()
            owner?.close()
            secondProvider?.close()
            firstProvider.close()
        }
    }
}
