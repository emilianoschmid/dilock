package example

import kotlinx.coroutines.runBlocking
import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockRedis
import org.dilock.kotlin.ProviderSettings
import org.dilock.redis.RedisProvider as JavaRedisProvider
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.RedisClient
import java.net.URI
import java.time.Duration
import java.util.UUID

fun main() = runBlocking {
    val endpoint = System.getenv("DILOCK_TEST_REDIS_ENDPOINT") ?: error("Redis endpoint required")
    val uri = URI.create(endpoint)
    val admin = RedisClient.builder()
        .hostAndPort(uri.host, if (uri.port < 0) 6379 else uri.port)
        .clientConfig(DefaultJedisClientConfig.builder(uri).connectionTimeoutMillis(1_000).socketTimeoutMillis(1_000).build())
        .build()
    val namespace = "kotlin-redis-${UUID.randomUUID()}"
    val epoch = "kotlin-redis-${UUID.randomUUID()}"
    try {
        check(JavaRedisProvider.provisionNamespace(admin, namespace, epoch) == "created")
        val settings = ProviderSettings(endpoint, namespace, epoch,
            rpcTimeout = Duration.ofMillis(200), cleanupTimeout = Duration.ofSeconds(1),
            defaultAcquisition = AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(4)))
        DilockRedis.connect(settings).use { first ->
            DilockRedis.connect(settings).use { second ->
                val handleA = first.lock("consumer-resource")
                val handleB = second.lock("consumer-resource")
                val owner = handleA.acquire(AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(3)))
                check(handleB.tryAcquire(Duration.ofSeconds(3)) == null) { "second provider must observe contention" }
                check(owner.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
                val successor = handleB.tryAcquire(Duration.ofSeconds(3)) ?: error("successor acquisition failed")
                check(successor.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
            }
        }
        println("PASS: exact Maven Kotlin artifact, source-hidden, Redis contention/release/handoff")
    } finally {
        admin.del("dilock:namespace:$namespace:meta")
        admin.close()
    }
}
