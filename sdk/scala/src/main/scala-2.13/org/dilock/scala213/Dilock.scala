package org.dilock.scala213

import java.time.Duration

import org.dilock.etcd.EtcdFence
import org.dilock.redis.DilockException

/** Immutable per-attempt acquisition budgets. Durations must be positive whole milliseconds. */
final case class AcquirePolicy(
    waitTimeout: Duration = Duration.ofSeconds(5),
    lease: Duration = Duration.ofSeconds(30)
) {
  def toJava: org.dilock.redis.AcquireOptions =
    new org.dilock.redis.AcquireOptions(waitTimeout, lease)
}

object AcquirePolicy {
  val default: AcquirePolicy = AcquirePolicy()
}

/** etcd-only acquisition policy; auto-renewal is opt-in and never stops paused work after loss. */
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

/** Entry points to the shared Java Redis engine. Providers own client pools and must be closed. */
object DilockRedis {
  /** Connects to a pre-provisioned default namespace using finite standard budgets. */
  def connect(endpoint: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, epoch))

  /** Connects to a pre-provisioned namespace using finite standard budgets. */
  def connect(endpoint: String, namespace: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, namespace, epoch))

  /** Uses explicit Java provider settings and does not create or repair namespace metadata. */
  def connect(options: org.dilock.redis.RedisProviderOptions): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(options))
}

/** Entry points to etcd's shared Java native-Lock adapter. */
object DilockEtcd {
  /** Connects to a pre-provisioned default namespace using finite standard budgets. */
  def connect(endpoint: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, epoch))

  /** Connects to a pre-provisioned namespace using finite standard budgets. */
  def connect(endpoint: String, namespace: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, namespace, epoch))

  /** Uses explicit Java provider settings and does not create or repair namespace metadata. */
  def connect(options: org.dilock.etcd.EtcdProviderOptions): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(options))
}

/** Reusable Redis provider and client pool. A provider may be shared by concurrent callers. */
final class RedisProvider private[scala213] (private val underlying: org.dilock.redis.RedisProvider)
    extends AutoCloseable {
  def lock(resource: String): RedisLock = new RedisLock(underlying.lock(resource))
  /** Performs bounded provider cleanup; idempotent. */
  override def close(): Unit = underlying.close()
}

/** Reusable lock handle. Redis does not provide strong fencing. */
final class RedisLock private[scala213] (private val underlying: org.dilock.redis.RedisLock) {
  /** Acquires with finite defaults or explicit policy. Backend failures are typed; interruption propagates. */
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, RedisLease] =
    try Right(new RedisLease(underlying.acquire(policy.toJava)))
    catch {
      case error: DilockException => Left(error)
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error
    }

  /** One attempt; None means ordinary contention only. */
  def tryAcquire(lease: Duration = AcquirePolicy.default.lease)
      : Either[DilockException, Option[RedisLease]] =
    try Right(Option(underlying.tryAcquire(Durations.wholeMillis(lease)).orElse(null)).map(new RedisLease(_)))
    catch {
      case error: DilockException => Left(error)
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error
    }

  /** Runs work and acknowledges release; user failures propagate, cleanup failures become Left. */
  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: RedisLease => A)
      : Either[DilockException, A] =
    acquire(policy).flatMap(lease => Scope.withRelease(lease)(_.release())(work))
}

/** One Redis owner generation. Explicit release reports its acknowledged outcome. */
final class RedisLease private[scala213] (private val underlying: org.dilock.redis.RedisLease)
    extends AutoCloseable {
  /** Checks local validity only; lease expiry cannot stop a paused process. */
  def assertValid(): Unit = underlying.assertValid()
  /** Atomically renews this owner generation. */
  def renew(lease: Duration): Unit = underlying.renew(Durations.wholeMillis(lease))
  def release(): Either[DilockException, org.dilock.redis.RedisLease.ReleaseOutcome] =
    try Right(underlying.release())
    catch { case error: DilockException => Left(error) }
  /** Best-effort bounded cleanup; inspect release() when acknowledgement matters. */
  override def close(): Unit = underlying.close()
}

/** Reusable etcd provider and gRPC channel. A provider may be shared by concurrent callers. */
final class EtcdProvider private[scala213] (private val underlying: org.dilock.etcd.EtcdProvider)
    extends AutoCloseable {
  def lock(resource: String): EtcdLock = new EtcdLock(underlying.lock(resource))
  /** Performs bounded provider cleanup; idempotent. */
  override def close(): Unit = underlying.close()
}

/** Reusable lock handle for etcd's native Lock profile. */
final class EtcdLock private[scala213] (private val underlying: org.dilock.etcd.EtcdLock) {
  /** Acquires using common finite defaults. */
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, EtcdLease] =
    try Right(new EtcdLease(underlying.acquire(policy.toJava)))
    catch {
      case error: DilockException => Left(error)
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error
    }

  /** Acquires with etcd-specific lease renewal, disabled by default. */
  def acquire(policy: EtcdAcquirePolicy): Either[DilockException, EtcdLease] = {
    val (waitMillis, leaseMillis, autoRenew) = policy.javaArguments
    try Right(new EtcdLease(underlying.acquire(waitMillis, leaseMillis, autoRenew)))
    catch {
      case error: DilockException => Left(error)
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error
    }
  }

  /** etcd native Lock has no safe try-lock operation; its explicit capability error is returned. */
  def tryAcquire(lease: Duration = AcquirePolicy.default.lease): Either[DilockException, Option[EtcdLease]] =
    try Right(Option(underlying.tryAcquire(Durations.wholeMillis(lease)).orElse(null)).map(new EtcdLease(_)))
    catch { case error: DilockException => Left(error) }

  /** Runs work and acknowledges release; user failures propagate, cleanup failures become Left. */
  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: EtcdLease => A)
      : Either[DilockException, A] =
    acquire(policy).flatMap(lease => Scope.withRelease(lease)(_.release())(work))

  /** Runs work with optional etcd lease renewal. */
  def withLease[A](policy: EtcdAcquirePolicy)(work: EtcdLease => A)
      : Either[DilockException, A] =
    acquire(policy).flatMap(lease => Scope.withRelease(lease)(_.release())(work))
}

/** One etcd lock generation with an epoch-scoped revision fence. */
final class EtcdLease private[scala213] (private val underlying: org.dilock.etcd.EtcdLease)
    extends AutoCloseable {
  /** Checks local validity; the protected resource must atomically enforce fences. */
  def assertValid(): Unit = underlying.assertValid()
  /** Returns the exact-key-verified etcd create-revision fence. */
  def fence: EtcdFence = underlying.fence()
  def release(): Either[DilockException, org.dilock.etcd.EtcdLease.ReleaseOutcome] =
    try Right(underlying.release())
    catch { case error: DilockException => Left(error) }
  /** Best-effort cleanup; inspect release() when acknowledgement matters. */
  override def close(): Unit = underlying.close()
}

private[scala213] object Durations {
  def wholeMillis(value: Duration): Long = {
    require(!value.isNegative && !value.isZero && value.getNano % 1000000 == 0,
      "duration must be a positive whole number of milliseconds")
    value.toMillis
  }
}

/** Scope helpers preserve user failures and surface explicit release uncertainty. */
private[scala213] object Scope {
  def withRelease[L, A](lease: L)(release: L => Either[DilockException, Any])
      (work: L => A): Either[DilockException, A] = {
    var workFailure: Throwable = null
    var result: A = null.asInstanceOf[A]
    try result = work(lease)
    catch { case failure: Throwable => workFailure = failure }

    var cleanupFailure: Throwable = null
    try {
      release(lease) match {
        case Left(failure) => cleanupFailure = failure
        case Right(outcome) if OutcomeAcknowledged(outcome) => ()
        case Right(outcome) => cleanupFailure = new DilockException(
          DilockException.Kind.LEASE_LOST,
          s"Scoped lease release did not return RELEASED: $outcome")
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

private[scala213] object OutcomeAcknowledged {
  def apply(outcome: Any): Boolean =
    outcome == org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED ||
      outcome == org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED
}
