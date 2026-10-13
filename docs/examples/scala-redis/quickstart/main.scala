import org.dilock.scala3.*
import java.time.Duration

@main def runRedis(): Unit =
  val endpoint = sys.env.getOrElse("DILOCK_REDIS_URL", "redis://127.0.0.1:6379")
  val epoch = sys.env.getOrElse("DILOCK_COORDINATION_EPOCH", throw new IllegalStateException("Set DILOCK_COORDINATION_EPOCH to the provisioned epoch"))
  val provider = DilockRedis.connect(endpoint, epoch)
  try
    val result = provider.lock("reports/daily").withLease() { lease =>
      lease.assertValid()
      rebuildDailyReport()
    }
    result.fold(throw _, identity)
    val configured = AcquirePolicy(Duration.ofSeconds(12), Duration.ofSeconds(45))
    val configuredResult = provider.lock("reports/monthly").withLease(configured) { lease =>
      lease.assertValid()
      rebuildMonthlyReport()
    }
    configuredResult.fold(throw _, identity)
  finally provider.close()

def rebuildDailyReport(): Unit =
  // Replace with one bounded, idempotent unit of work.

def rebuildMonthlyReport(): Unit = ()
