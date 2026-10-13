import java.time.Duration
import org.dilock.redis.DilockException
import org.dilock.scala3.{AcquirePolicy, DilockEtcd, DilockRedis}

/** External-package live consumer for one disposable Redis or etcd authority. */
object LiveQuickstart:
  private val policy = AcquirePolicy(Duration.ofMillis(250), Duration.ofSeconds(10))
  private val successorPolicy = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10))

  def main(args: Array[String]): Unit =
    require(args.length == 4, "usage: LiveQuickstart <redis|etcd> <endpoint> <namespace> <epoch>")
    val Array(backend, endpoint, namespace, epoch) = args
    backend match
      case "redis" => runRedis(endpoint, namespace, epoch)
      case "etcd" => runEtcd(endpoint, namespace, epoch)
      case other => throw new IllegalArgumentException(s"unsupported backend: $other")

  private def requireTimedOut(result: Either[DilockException, ?]): Unit = result match
    case Left(error) if error.kind() == DilockException.Kind.ACQUISITION_TIMEOUT => ()
    case Left(error) => throw new AssertionError(s"expected acquisition timeout, got ${error.kind()}", error)
    case Right(_) => throw new AssertionError("second handle acquired while first handle held the resource")

  private def runRedis(endpoint: String, namespace: String, epoch: String): Unit =
    val provider = DilockRedis.connect(endpoint, namespace, epoch)
    try
      val firstHandle = provider.lock("scala-live-consumer")
      val secondHandle = provider.lock("scala-live-consumer")
      val first = firstHandle.acquire(successorPolicy).fold(throw _, identity)
      requireTimedOut(secondHandle.acquire(policy))
      first.release().fold(throw _, identity)
      val successor = secondHandle.acquire(successorPolicy).fold(throw _, identity)
      successor.assertValid()
      successor.release().fold(throw _, identity)
      println("PASS redis shared-provider contention, release, and successor")
    finally provider.close()

  private def runEtcd(endpoint: String, namespace: String, epoch: String): Unit =
    val provider = DilockEtcd.connect(endpoint, namespace, epoch)
    try
      val firstHandle = provider.lock("scala-live-consumer")
      val secondHandle = provider.lock("scala-live-consumer")
      val first = firstHandle.acquire(successorPolicy).fold(throw _, identity)
      val firstFence = first.fence
      require(firstFence.epoch() == epoch && firstFence.value() > 0, "first etcd fence must expose configured epoch and positive revision")
      requireTimedOut(secondHandle.acquire(policy))
      first.release().fold(throw _, identity)
      val successor = secondHandle.acquire(successorPolicy).fold(throw _, identity)
      val successorFence = successor.fence
      require(successorFence.epoch() == epoch, "successor fence epoch must match configured epoch")
      require(successorFence.value() > firstFence.value(), "successor fence revision must increase")
      successor.release().fold(throw _, identity)
      println(s"PASS etcd shared-provider contention, release, successor, fence=${firstFence.value()}->${successorFence.value()}")
    finally provider.close()
