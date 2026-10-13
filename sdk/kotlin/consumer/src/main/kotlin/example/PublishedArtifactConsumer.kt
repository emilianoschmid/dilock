package example

import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockEtcd
import org.dilock.kotlin.DilockRedis
import org.dilock.kotlin.EtcdProvider
import org.dilock.kotlin.ProviderSettings
import org.dilock.kotlin.RedisProvider
import java.time.Duration

/** Compile-only consumer of the generated local Kotlin/JVM artifact. */
class PublishedArtifactConsumer {
    private val settings = ProviderSettings(
        endpoint = "redis://127.0.0.1:6379",
        namespace = "billing",
        epoch = "consumer-epoch",
        defaultAcquisition = AcquirePolicy(Duration.ofSeconds(5), Duration.ofSeconds(30))
    )

    fun redisProvider(): RedisProvider = DilockRedis.connect(settings)
    fun etcdProvider(): EtcdProvider = DilockEtcd.connect(settings)

    suspend fun useRedis(provider: RedisProvider) {
        provider.lock("invoice/42").withLease { lease -> lease.assertValid() }
    }

    suspend fun useEtcd(provider: EtcdProvider) {
        provider.lock("invoice/42").withLease { lease -> lease.fence() }
    }
}
