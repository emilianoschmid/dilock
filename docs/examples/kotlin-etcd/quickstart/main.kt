import kotlinx.coroutines.runBlocking
import java.time.Duration
import org.dilock.kotlin.EtcdAcquirePolicy
import org.dilock.kotlin.DilockEtcd

fun main() = runBlocking {
    val endpoint = System.getenv("ETCD_URL") ?: "http://127.0.0.1:2379"
    val epoch = requireNotNull(System.getenv("DILOCK_COORDINATION_EPOCH")) {
        "DILOCK_COORDINATION_EPOCH must match provisioned namespace metadata"
    }

    DilockEtcd.connect(endpoint, "default", epoch).use { provider ->
        provider.lock("reports/daily").withLease { lease ->
            lease.assertValid()
            val fence = lease.fence()
            rebuildDailyReport(fence.epoch(), fence.value())
        }
        val explicit = EtcdAcquirePolicy(Duration.ofSeconds(12), Duration.ofSeconds(45), autoRenew = true)
        provider.lock("reports/monthly").withLease(explicit) { lease ->
            lease.assertValid()
            val fence = lease.fence()
            rebuildMonthlyReport(fence.epoch(), fence.value())
        }
    }
}

private fun rebuildDailyReport(epoch: String, revision: Long) {
    // Atomically reject stale epoch/revision fences at the protected resource.
    println("Applying work under fence $epoch/$revision")
}

private fun rebuildMonthlyReport(epoch: String, revision: Long) {
    println("Applying work under fence $epoch/$revision")
}
