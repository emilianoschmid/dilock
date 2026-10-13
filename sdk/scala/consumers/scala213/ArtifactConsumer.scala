package consumers

import java.time.Duration

import org.dilock.scala213._
import org.dilock.redis.DilockException

/** Source-hidden artifact consumer. The executable checks pure API contracts and never connects. */
object ArtifactConsumer {
  def main(args: Array[String]): Unit = {
    val redis = AcquirePolicy(Duration.ofMillis(500), Duration.ofSeconds(5)).toJava
    require(redis.waitMillis() == 500L && redis.leaseMillis() == 5000L)

    val etcd = EtcdAcquirePolicy(Duration.ofMillis(500), Duration.ofSeconds(5), autoRenew = true)
    require(etcd.waitTimeout == Duration.ofMillis(500) && etcd.lease == Duration.ofSeconds(5))
    require(etcd.autoRenew)
    println("Scala 2.13 artifact consumer policy checks passed")
    if (args.nonEmpty) {
      require(args.length == 4, "usage: ArtifactConsumer <redis-endpoint> <etcd-endpoint> <namespace> <epoch>")
      runLive(args(0), args(1), args(2), args(3))
    }
  }

  private def runLive(redisEndpoint: String, etcdEndpoint: String, namespace: String, epoch: String): Unit = {
    val redisFirst = DilockRedis.connect(redisEndpoint, namespace, epoch)
    val redisSecond = DilockRedis.connect(redisEndpoint, namespace, epoch)
    try {
      val resource = "scala213-artifact-consumer"
      val short = AcquirePolicy(Duration.ofMillis(250), Duration.ofSeconds(5))
      val first = redisFirst.lock(resource).acquire(short).fold(throw _, identity)
      require(redisSecond.lock(resource).tryAcquire(Duration.ofSeconds(5)) == Right(None),
        "Redis second provider should observe ordinary contention")
      first.release().fold(throw _, identity)
      val successor = redisSecond.lock(resource)
        .acquire(AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(5))).fold(throw _, identity)
      successor.assertValid()
      successor.release().fold(throw _, identity)
      println("PASS exact artifact Redis contention, acknowledged release, and successor")
    } finally {
      redisSecond.close()
      redisFirst.close()
    }

    val etcdFirst = DilockEtcd.connect(etcdEndpoint, namespace, epoch)
    val etcdSecond = DilockEtcd.connect(etcdEndpoint, namespace, epoch)
    try {
      val resource = "scala213-artifact-consumer"
      val firstPolicy = EtcdAcquirePolicy(Duration.ofMillis(250), Duration.ofSeconds(5), autoRenew = true)
      val first = etcdFirst.lock(resource).acquire(firstPolicy).fold(throw _, identity)
      val firstFence = first.fence
      require(firstFence.epoch() == epoch && firstFence.value() > 0L)
      val blocked = etcdSecond.lock(resource)
        .acquire(EtcdAcquirePolicy(Duration.ofMillis(250), Duration.ofSeconds(5)))
      require(blocked.left.toOption.exists(_.kind() == DilockException.Kind.ACQUISITION_TIMEOUT),
        "etcd second provider should observe the bounded acquisition timeout")
      first.release().fold(throw _, identity)
      val successor = etcdSecond.lock(resource)
        .acquire(EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(5), autoRenew = true))
        .fold(throw _, identity)
      val successorFence = successor.fence
      require(successorFence.epoch() == epoch && successorFence.value() > firstFence.value())
      successor.release().fold(throw _, identity)
      println(s"PASS exact artifact etcd timeout, acknowledged handoff, and fence=${firstFence.value()}->${successorFence.value()}")
    } finally {
      etcdSecond.close()
      etcdFirst.close()
    }
  }

  def useRedis(provider: RedisProvider, resource: String): Either[DilockException, String] =
    provider.lock(resource).withLease() { lease =>
      lease.assertValid()
      lease.renew(Duration.ofSeconds(5))
      "redis-scope"
    }

  def useEtcd(provider: EtcdProvider, resource: String): Either[DilockException, Long] =
    provider.lock(resource).withLease(EtcdAcquirePolicy(autoRenew = true)) { lease =>
      lease.assertValid()
      lease.fence.value()
    }
}
