import java.time.Duration

import org.dilock.scala3.{AcquirePolicy, DilockEtcd, DilockRedis, EtcdAcquirePolicy}
import org.dilock.redis.DilockException

/**
 * Source-hidden package consumer for Redis and etcd.
 * Run from a separate project with only the package JAR and declared Java dependencies.
 */
object ArtifactConsumer:
  private val holder = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10))
  private val contender = AcquirePolicy(Duration.ofMillis(350), Duration.ofSeconds(10))

  private def expectTimeout(result: Either[DilockException, ?]): Unit = result match
    case Left(error) if error.kind() == DilockException.Kind.ACQUISITION_TIMEOUT => ()
    case Left(error) => throw new AssertionError(s"expected acquisition timeout, got ${error.kind()}", error)
    case Right(_) => throw new AssertionError("independent provider acquired an already held resource")

  def main(args: Array[String]): Unit =
    require(args.length == 4, "usage: ArtifactConsumer <redis-endpoint> <etcd-endpoint> <namespace> <epoch>")
    val Array(redisEndpoint, etcdEndpoint, namespace, epoch) = args
    verifyRedis(redisEndpoint, namespace, epoch)
    verifyEtcd(etcdEndpoint, namespace, epoch)

  private def verifyRedis(endpoint: String, namespace: String, epoch: String): Unit =
    val owner = DilockRedis.connect(endpoint, namespace, epoch)
    val successorProvider = DilockRedis.connect(endpoint, namespace, epoch)
    try
      val resource = "scala3-package-consumer"
      val first = owner.lock(resource).acquire(holder).fold(throw _, identity)
      expectTimeout(successorProvider.lock(resource).acquire(contender))
      first.release().fold(throw _, identity)
      val successor = successorProvider.lock(resource).acquire(holder).fold(throw _, identity)
      successor.assertValid()
      successor.release().fold(throw _, identity)
      println("PASS source-hidden Scala 3 package Redis: independent-provider contention, acknowledged release, successor, validity")
    finally
      successorProvider.close()
      owner.close()

  private def verifyEtcd(endpoint: String, namespace: String, epoch: String): Unit =
    val owner = DilockEtcd.connect(endpoint, namespace, epoch)
    val successorProvider = DilockEtcd.connect(endpoint, namespace, epoch)
    try
      val resource = "scala3-package-consumer"
      val first = owner.lock(resource)
        .acquire(EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10), autoRenew = true))
        .fold(throw _, identity)
      val before = first.fence
      require(before.epoch() == epoch && before.value() > 0L)
      expectTimeout(successorProvider.lock(resource).acquire(
        EtcdAcquirePolicy(Duration.ofMillis(350), Duration.ofSeconds(10))))
      first.release().fold(throw _, identity)
      val successor = successorProvider.lock(resource)
        .acquire(EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10), autoRenew = true))
        .fold(throw _, identity)
      val after = successor.fence
      require(after.epoch() == epoch && after.value() > before.value())
      successor.release().fold(throw _, identity)
      println(s"PASS source-hidden Scala 3 package etcd: contention, acknowledged handoff, successor fence ${before.value()} -> ${after.value()}")
    finally
      successorProvider.close()
      owner.close()
