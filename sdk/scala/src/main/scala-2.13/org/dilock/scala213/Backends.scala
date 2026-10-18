package org.dilock.scala213

import java.time.Duration

import org.dilock.etcd.{EtcdFence, EtcdProviderOptions => JavaEtcdProviderOptions}
import org.dilock.redis.{DilockException, RedisProviderOptions => JavaRedisProviderOptions}

/** Immutable etcd acquisition policy, including renewal of the native lease. */
final case class EtcdAcquirePolicy(
    waitTimeout: Duration = Duration.ofSeconds(5),
    lease: Duration = Duration.ofSeconds(30),
    autoRenew: Boolean = false
) {
  private[scala213] def javaArguments: (Long, Long, Boolean) =
    (Durations.wholeMillis(waitTimeout), Durations.wholeMillis(lease), autoRenew)
}

object EtcdAcquirePolicy {
  val default: EtcdAcquirePolicy = EtcdAcquirePolicy()
}

object DilockRedis {
  def connect(endpoint: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, epoch))

  def connect(endpoint: String, namespace: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, namespace, epoch))

  def connect(options: JavaRedisProviderOptions): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(options))
}

final class RedisProvider private[scala213] (
    private val underlying: org.dilock.redis.RedisProvider
) extends AutoCloseable {
  def lock(resource: String): RedisLock = new RedisLock(underlying.lock(resource))
  override def close(): Unit = underlying.close()
}

final class RedisLock private[scala213] (
    private val underlying: org.dilock.redis.RedisLock
) {
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, RedisLease] =
    try Right(new RedisLease(underlying.acquire(policy.toJava)))
    catch { case error: DilockException => Left(error) }

  def tryAcquire(lease: Duration = AcquirePolicy.default.lease): Either[DilockException, Option[RedisLease]] =
    try Right(Option(underlying.tryAcquire(Durations.wholeMillis(lease)).orElse(null)).map(new RedisLease(_)))
    catch { case error: DilockException => Left(error) }

  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: RedisLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease => Scope.withRelease(lease)(_.release())(work) }
}

final class RedisLease private[scala213] (
    private val underlying: org.dilock.redis.RedisLease
) extends AutoCloseable {
  def assertValid(): Unit = underlying.assertValid()
  def renew(lease: Duration): Unit = underlying.renew(Durations.wholeMillis(lease))
  def release(): Either[DilockException, org.dilock.redis.RedisLease.ReleaseOutcome] =
    try Right(underlying.release())
    catch { case error: DilockException => Left(error) }
  override def close(): Unit = underlying.close()
}

object DilockEtcd {
  def connect(endpoint: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, epoch))

  def connect(endpoint: String, namespace: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, namespace, epoch))

  def connect(options: JavaEtcdProviderOptions): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(options))
}

final class EtcdProvider private[scala213] (
    private val underlying: org.dilock.etcd.EtcdProvider
) extends AutoCloseable {
  def lock(resource: String): EtcdLock = new EtcdLock(underlying.lock(resource))
  override def close(): Unit = underlying.close()
}

final class EtcdLock private[scala213] (
    private val underlying: org.dilock.etcd.EtcdLock
) {
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, EtcdLease] =
    try Right(new EtcdLease(underlying.acquire(policy.toJava)))
    catch { case error: DilockException => Left(error) }

  def acquire(policy: EtcdAcquirePolicy): Either[DilockException, EtcdLease] = {
    val (waitMillis, leaseMillis, autoRenew) = policy.javaArguments
    try Right(new EtcdLease(underlying.acquire(waitMillis, leaseMillis, autoRenew)))
    catch { case error: DilockException => Left(error) }
  }

  def tryAcquire(): Either[DilockException, Option[EtcdLease]] =
    try Right(Option(underlying.tryAcquire().orElse(null)).map(new EtcdLease(_)))
    catch { case error: DilockException => Left(error) }

  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: EtcdLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease => Scope.withRelease(lease)(_.release())(work) }

  def withLease[A](policy: EtcdAcquirePolicy)(work: EtcdLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease => Scope.withRelease(lease)(_.release())(work) }
}

final class EtcdLease private[scala213] (
    private val underlying: org.dilock.etcd.EtcdLease
) extends AutoCloseable {
  def assertValid(): Unit = underlying.assertValid()
  def fence: EtcdFence = underlying.fence()
  def release(): Either[DilockException, org.dilock.etcd.EtcdLease.ReleaseOutcome] =
    try Right(underlying.release())
    catch { case error: DilockException => Left(error) }
  override def close(): Unit = underlying.close()
}

/** Scope helper that preserves user failures and reports unacknowledged release. */
object Scope {
  def withRelease[L, A](lease: L)(release: L => Either[DilockException, _])(work: L => A): Either[DilockException, A] = {
    var workFailure: Throwable = null
    var result: A = null.asInstanceOf[A]
    try result = work(lease)
    catch { case failure: Throwable => workFailure = failure }

    var cleanupFailure: Throwable = null
    try {
      release(lease) match {
        case Left(failure) => cleanupFailure = failure
        case Right(outcome) if outcome == org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED =>
        case Right(outcome) if outcome == org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED =>
        case Right(outcome) => cleanupFailure = new DilockException(
          DilockException.Kind.LEASE_LOST,
          "Scoped lease release did not return RELEASED: " + outcome
        )
      }
    } catch { case failure: Throwable => cleanupFailure = failure }

    if (workFailure != null) {
      if (cleanupFailure != null && (cleanupFailure ne workFailure)) workFailure.addSuppressed(cleanupFailure)
      throw workFailure
    }
    cleanupFailure match {
      case null => Right(result)
      case failure: DilockException => Left(failure)
      case failure => throw failure
    }
  }
}

private object Durations {
  def wholeMillis(value: Duration): Long = {
    require(!value.isNegative && !value.isZero && value.getNano % 1000000 == 0,
      "duration must be a positive whole number of milliseconds")
    value.toMillis
  }
}
