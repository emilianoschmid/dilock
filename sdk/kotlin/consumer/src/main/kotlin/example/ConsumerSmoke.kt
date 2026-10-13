package example

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockEtcd
import org.dilock.kotlin.DilockRedis
import org.dilock.kotlin.EtcdLease
import org.dilock.kotlin.ProviderSettings
import org.dilock.redis.RedisProvider as JavaRedisProvider
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.RedisClient
import java.net.URI
import java.time.Duration
import java.util.UUID

/** Runs enabled backend checks using only locally resolved Maven artifacts. */
fun main() = runBlocking {
    val redisEndpoint = System.getenv("DILOCK_TEST_REDIS_ENDPOINT")
    if (redisEndpoint.isNullOrBlank()) {
        println("SKIP: Redis Maven consumer requires DILOCK_TEST_REDIS_ENDPOINT")
    } else {
        runRedisSmoke(redisEndpoint)
    }

    val etcdEndpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
    if (etcdEndpoint.isNullOrBlank()) {
        println("SKIP: etcd Maven consumer requires DILOCK_TEST_ETCD_ENDPOINT")
    } else {
        val namespace = System.getenv("DILOCK_TEST_NAMESPACE")
            ?: error("DILOCK_TEST_NAMESPACE is required with an etcd endpoint")
        val epoch = System.getenv("DILOCK_TEST_EPOCH")
            ?: error("DILOCK_TEST_EPOCH is required with an etcd endpoint")
        runEtcdSmoke(etcdEndpoint, namespace, epoch)
    }
}

private suspend fun runRedisSmoke(endpoint: String) {
    val uri = URI.create(endpoint)
    val admin = RedisClient.builder()
        .hostAndPort(uri.host, if (uri.port < 0) 6379 else uri.port)
        .clientConfig(DefaultJedisClientConfig.builder(uri).connectionTimeoutMillis(1_000).socketTimeoutMillis(1_000).build())
        .build()
    val namespace = "kotlin-maven-${UUID.randomUUID()}"
    val epoch = "kotlin-maven-${UUID.randomUUID()}"
    try {
        check(JavaRedisProvider.provisionNamespace(admin, namespace, epoch) == "created")
        val settings = ProviderSettings(
            endpoint = endpoint,
            namespace = namespace,
            epoch = epoch,
            rpcTimeout = Duration.ofMillis(200),
            cleanupTimeout = Duration.ofSeconds(1),
            defaultAcquisition = AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(4)),
        )
        DilockRedis.connect(settings).use { first ->
            DilockRedis.connect(settings).use { second ->
                val resourceA = first.lock("consumer-resource")
                val resourceB = second.lock("consumer-resource")
                val owner = resourceA.acquire(AcquirePolicy(Duration.ofSeconds(1), Duration.ofSeconds(3)))
                check(resourceB.tryAcquire(Duration.ofSeconds(3)) == null) {
                    "second provider must observe contention"
                }
                check(owner.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
                val successor = resourceB.tryAcquire(Duration.ofSeconds(3))
                    ?: error("successor acquisition failed")
                check(successor.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
            }
        }
        println("PASS: Maven-resolved Kotlin artifact drove independent-provider Redis contention and handoff")
    } finally {
        admin.del("dilock:namespace:$namespace:meta")
        admin.close()
    }
}

private suspend fun runEtcdSmoke(endpoint: String, namespace: String, epoch: String) = coroutineScope {
    val settings = ProviderSettings(
        endpoint = endpoint,
        namespace = namespace,
        epoch = epoch,
        rpcTimeout = Duration.ofSeconds(5),
        cleanupTimeout = Duration.ofSeconds(2),
        defaultAcquisition = AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(30)),
    )
    val firstProvider = DilockEtcd.connect(settings)
    var secondProvider: org.dilock.kotlin.EtcdProvider? = null
    var owner: EtcdLease? = null
    var successor: EtcdLease? = null
    var queued: Deferred<EtcdLease>? = null
    try {
        secondProvider = DilockEtcd.connect(settings)
        owner = firstProvider.lock("consumer-etcd-resource").acquire(
            AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(10))
        )
        val firstFence = owner.fence()
        check(firstFence.epoch() == epoch) { "first fence must use the provisioned epoch" }
        check(firstFence.value() > 0) { "first fence revision must be positive" }

        val queuedStarted = CompletableDeferred<Unit>()
        queued = async(Dispatchers.IO) {
            queuedStarted.complete(Unit)
            checkNotNull(secondProvider).lock("consumer-etcd-resource").acquire(
                AcquirePolicy(Duration.ofSeconds(6), Duration.ofSeconds(10))
            )
        }
        queuedStarted.await()
        delay(250)
        check(!queued.isCompleted) { "second provider must queue behind the first etcd owner" }

        check(owner.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
        owner = null
        successor = withTimeout(8_000) { queued.await() }
        val secondFence = successor.fence()
        check(secondFence.epoch() == epoch) { "successor fence must preserve the namespace epoch" }
        check(secondFence.value() > firstFence.value()) {
            "successor etcd create_revision must advance on handoff"
        }
        check(successor.release() == org.dilock.kotlin.ReleaseOutcome.RELEASED)
        successor = null
        println(
            "PASS: Maven-resolved Kotlin artifact drove etcd queue/release/handoff " +
                "with epoch=${secondFence.epoch()} revision=${firstFence.value()}->${secondFence.value()}"
        )
    } finally {
        queued?.cancelAndJoin()
        successor?.close()
        owner?.close()
        secondProvider?.close()
        firstProvider.close()
    }
}

