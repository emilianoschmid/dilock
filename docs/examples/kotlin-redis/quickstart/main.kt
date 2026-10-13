import kotlinx.coroutines.runBlocking
import java.time.Duration
import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockRedis

fun main() = runBlocking {
    val endpoint = System.getenv("REDIS_URL") ?: "redis://127.0.0.1:6379"
    val epoch = requireNotNull(System.getenv("DILOCK_COORDINATION_EPOCH")) {
        "DILOCK_COORDINATION_EPOCH must match provisioned namespace metadata"
    }

    DilockRedis.connect(endpoint, "default", epoch).use { provider ->
        provider.lock("reports/daily").withLease { lease ->
            lease.assertValid()
            rebuildDailyReport()
        }
        val explicit = AcquirePolicy(Duration.ofSeconds(12), Duration.ofSeconds(45))
        provider.lock("reports/monthly").withLease(explicit) { lease ->
            lease.assertValid()
            rebuildMonthlyReport()
        }
    }
}

private fun rebuildDailyReport() {
    // Replace with one bounded, idempotent unit of work.
}

private fun rebuildMonthlyReport() {}
