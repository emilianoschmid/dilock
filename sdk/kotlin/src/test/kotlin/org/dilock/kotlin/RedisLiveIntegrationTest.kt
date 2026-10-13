package org.dilock.kotlin

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.dilock.redis.RedisProvider as JavaRedisProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.RedisClient
import java.net.URI
import java.time.Duration
import java.util.UUID

/** Opt-in test against the Redis adapter used by real Kotlin providers; no fake transport is used. */
class RedisLiveIntegrationTest {
    @Test fun independentProvidersContendAndLateOwnerReleasePreservesSuccessor() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_REDIS_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_REDIS_ENDPOINT to opt in to live Redis")

        val uri = URI.create(endpoint!!)
        require(uri.scheme == "redis" || uri.scheme == "rediss") { "endpoint must use redis:// or rediss://" }
        val admin = RedisClient.builder()
            .hostAndPort(uri.host, if (uri.port < 0) 6379 else uri.port)
            .clientConfig(DefaultJedisClientConfig.builder(uri).connectionTimeoutMillis(1_000).socketTimeoutMillis(1_000).build())
            .build()
        val namespace = "kotlin-live-${UUID.randomUUID()}"
        val epoch = "kotlin-live-${UUID.randomUUID()}"

        try {
            assertEquals("created", JavaRedisProvider.provisionNamespace(admin, namespace, epoch))
            val settings = ProviderSettings(
                endpoint = endpoint,
                namespace = namespace,
                epoch = epoch,
                rpcTimeout = Duration.ofMillis(200),
                cleanupTimeout = Duration.ofSeconds(1),
                defaultAcquisition = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(4)),
            )

            DilockRedis.connect(settings).use { firstProvider ->
                DilockRedis.connect(settings).use { secondProvider ->
                    DilockRedis.connect(settings).use { observerProvider ->
                        val firstLock = firstProvider.lock("stale-release")
                        val secondLock = secondProvider.lock("stale-release")
                        val observerLock = observerProvider.lock("stale-release")

                        val original = firstLock.acquire(AcquirePolicy(Duration.ofMillis(1), Duration.ofMillis(700)))
                        assertNull(secondLock.tryAcquire(Duration.ofSeconds(4)), "a competing provider must see the active owner")

                        // Let Redis expire the original owner, then acquire a successor from another client.
                        delay(850)
                        val successor = secondLock.acquire(AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(4)))
                        assertEquals(org.dilock.redis.RedisLease.ReleaseOutcome.ALREADY_NOT_OWNED, original.release())
                        assertNull(observerLock.tryAcquire(Duration.ofSeconds(4)), "a stale release must not delete the successor")

                        assertEquals(org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED, successor.release())
                        val handoff = observerLock.acquire(AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(4)))
                        handoff.release()

                        val renewable = firstProvider.lock("renewal").acquire(
                            AcquirePolicy(Duration.ofMillis(1), Duration.ofMillis(700))
                        )
                        renewable.renew(Duration.ofSeconds(2))
                        delay(850)
                        val afterRenewal = secondProvider.lock("renewal").tryAcquire(Duration.ofSeconds(4))
                        assertNull(afterRenewal, "renewal must extend the Redis owner value beyond its original TTL")
                        renewable.release()
                        Unit // JUnit 5 test methods must return void rather than the release outcome.
                    }
                }
            }
        } finally {
            admin.del("dilock:namespace:$namespace:meta")
            admin.close()
        }
    }
}
