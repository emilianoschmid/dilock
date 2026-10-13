import org.dilock.scala3.*
import java.time.Duration

@main def runEtcd(): Unit =
  val endpoint = sys.env.getOrElse("DILOCK_ETCD_ENDPOINT", "http://127.0.0.1:2379")
  val epoch = sys.env.getOrElse("DILOCK_COORDINATION_EPOCH", throw new IllegalStateException("Set DILOCK_COORDINATION_EPOCH to the provisioned epoch"))
  val provider = DilockEtcd.connect(endpoint, epoch)
  try
    val result = provider.lock("reports/daily").withLease() { lease =>
      lease.assertValid()
      val fence = lease.fence
      rebuildDailyReport(fence.epoch(), fence.value())
    }
    result.fold(throw _, identity)
    val configured = EtcdAcquirePolicy(Duration.ofSeconds(12), Duration.ofSeconds(45), autoRenew = true)
    val configuredResult = provider.lock("reports/monthly").withLease(configured) { lease =>
      lease.assertValid()
      val fence = lease.fence
      rebuildMonthlyReport(fence.epoch(), fence.value())
    }
    configuredResult.fold(throw _, identity)
  finally provider.close()

def rebuildDailyReport(epoch: String, revision: Long): Unit =
  // Atomically reject stale epoch/revision fences at the protected resource.
  println(s"Applying work under fence $epoch/$revision")

def rebuildMonthlyReport(epoch: String, revision: Long): Unit =
  println(s"Applying work under fence $epoch/$revision")
