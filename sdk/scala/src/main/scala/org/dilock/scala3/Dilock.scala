package org.dilock.scala3

import java.time.Duration
import org.dilock.redis.{AcquireOptions as JavaAcquireOptions, DilockException}
import org.dilock.redis.RedisLease.ReleaseOutcome
import org.dilock.redis.{RedisProviderOptions as JavaRedisProviderOptions}
import org.dilock.etcd.{EtcdProviderOptions as JavaEtcdProviderOptions, EtcdFence}

/** Immutable policy for one acquisition. Java duration precision is whole milliseconds. */
final case class AcquirePolicy(waitTimeout: Duration = Duration.ofSeconds(5), lease: Duration = Duration.ofSeconds(30)):
  def toJava: JavaAcquireOptions = new JavaAcquireOptions(waitTimeout, lease)

object AcquirePolicy:
  val default: AcquirePolicy = AcquirePolicy()

/** Immutable etcd acquisition policy, including renewal of the granted lease while held. */
final case class EtcdAcquirePolicy(
    waitTimeout: Duration = Duration.ofSeconds(5),
    lease: Duration = Duration.ofSeconds(30),
    autoRenew: Boolean = false
):
  private[scala3] def javaArguments: (Long, Long, Boolean) =
    (wholeMillis(waitTimeout), wholeMillis(lease), autoRenew)

object EtcdAcquirePolicy:
  val default: EtcdAcquirePolicy = EtcdAcquirePolicy()

/** Entry points for the production Java Redis engine. */
object DilockRedis:
  /** Connects to a pre-provisioned default namespace with the finite standard budgets. */
  def connect(endpoint: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, epoch))

  /** Connects to a pre-provisioned namespace with the finite standard budgets. */
  def connect(endpoint: String, namespace: String, epoch: String): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, namespace, epoch))

  /** Uses complete immutable provider settings; it never creates or repairs namespace metadata. */
  def connect(options: JavaRedisProviderOptions): RedisProvider =
    new RedisProvider(org.dilock.redis.RedisProvider.connect(options))

/** Entry points for etcd's production native Lock adapter. */
object DilockEtcd:
  /** Connects to a pre-provisioned default namespace with the finite standard budgets. */
  def connect(endpoint: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, epoch))

  /** Connects to a pre-provisioned namespace with the finite standard budgets. */
  def connect(endpoint: String, namespace: String, epoch: String): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, namespace, epoch))

  /** Uses complete immutable provider settings; it never creates or repairs namespace metadata. */
  def connect(options: JavaEtcdProviderOptions): EtcdProvider =
    new EtcdProvider(org.dilock.etcd.EtcdProvider.connect(options))

/** Owns one Java Redis provider/client pool. It may be shared by concurrent callers. */
final class RedisProvider private[scala3] (private val underlying: org.dilock.redis.RedisProvider)
    extends AutoCloseable:
  /** Creates a reusable resource handle sharing this provider's client pool. */
  def lock(resource: String): RedisLock = new RedisLock(underlying.lock(resource))
  /** Performs bounded provider cleanup; idempotent. */
  override def close(): Unit = underlying.close()

/** Reusable handle to one resource. Redis does not provide strong fencing. */
final class RedisLock private[scala3] (private val underlying: org.dilock.redis.RedisLock):
  /** Acquires with finite defaults or a complete per-attempt policy. Backend errors remain typed. */
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, RedisLease] =
    try Right(new RedisLease(underlying.acquire(policy.toJava)))
    catch
      case error: DilockException => Left(error)
      case error: InterruptedException => Thread.currentThread().interrupt(); throw error

  /** One attempt; None means ordinary Redis contention only. */
  def tryAcquire(lease: Duration = AcquirePolicy.default.lease): Either[DilockException, Option[RedisLease]] =
    try Right(Option(underlying.tryAcquire(wholeMillis(lease)).orElse(null)).map(new RedisLease(_)))
    catch
      case error: DilockException => Left(error)
      case error: InterruptedException => Thread.currentThread().interrupt(); throw error

  /** Releases in a finally block and preserves the work error; uncertain scope cleanup is observed by Java. */
  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: RedisLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease =>
      withLeaseRelease(lease)(_.release())(work)
    }

/** One Redis owner generation; explicit release returns its acknowledged outcome. */
final class RedisLease private[scala3] (private val underlying: org.dilock.redis.RedisLease)
    extends AutoCloseable:
  /** Checks local validity only; expiry cannot stop a paused process. */
  def assertValid(): Unit = underlying.assertValid()
  /** Atomically renews this owner or throws a typed failure. */
  def renew(lease: Duration): Unit = underlying.renew(wholeMillis(lease))
  /** Performs bounded owner-token-checked release. */
  def release(): Either[DilockException, org.dilock.redis.RedisLease.ReleaseOutcome] =
    try Right(underlying.release()) catch case error: DilockException => Left(error)
  /** Best-effort bounded cleanup; inspect release for acknowledgement. */
  override def close(): Unit = underlying.close()

/** Owns one etcd provider/channel. It may be shared by concurrent callers. */
final class EtcdProvider private[scala3] (private val underlying: org.dilock.etcd.EtcdProvider)
    extends AutoCloseable:
  /** Creates a reusable resource handle sharing this provider's gRPC channel. */
  def lock(resource: String): EtcdLock = new EtcdLock(underlying.lock(resource))
  /** Performs bounded provider cleanup; idempotent. */
  override def close(): Unit = underlying.close()

/** Reusable handle to one resource under etcd's native Lock profile. */
final class EtcdLock private[scala3] (private val underlying: org.dilock.etcd.EtcdLock):
  /** Acquires with finite defaults or a complete per-attempt policy. */
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, EtcdLease] =
    try Right(new EtcdLease(underlying.acquire(policy.toJava)))
    catch
      case error: DilockException => Left(error)
      case error: InterruptedException => Thread.currentThread().interrupt(); throw error

  /** Acquires with the etcd-specific renewal policy; Redis renewal is unsupported. */
  def acquire(policy: EtcdAcquirePolicy): Either[DilockException, EtcdLease] =
    val (waitMillis, leaseMillis, autoRenew) = policy.javaArguments
    try Right(new EtcdLease(underlying.acquire(waitMillis, leaseMillis, autoRenew)))
    catch
      case error: DilockException => Left(error)
      case error: InterruptedException => Thread.currentThread().interrupt(); throw error

  /** etcd native Lock has no safe try-lock operation; returns its explicit capability failure. */
  def tryAcquire(lease: Duration = AcquirePolicy.default.lease): Either[DilockException, Option[EtcdLease]] =
    try Right(Option(underlying.tryAcquire(wholeMillis(lease)).orElse(null)).map(new EtcdLease(_)))
    catch case error: DilockException => Left(error)

  /** Releases in a finally block and preserves the work error; uncertain scope cleanup is observed by Java. */
  def withLease[A](policy: AcquirePolicy = AcquirePolicy.default)(work: EtcdLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease =>
      withLeaseRelease(lease)(_.release())(work)
    }

  /** Runs work under an etcd-specific policy, optionally renewing while the lease is held. */
  def withLease[A](policy: EtcdAcquirePolicy)(work: EtcdLease => A): Either[DilockException, A] =
    acquire(policy).flatMap { lease =>
      withLeaseRelease(lease)(_.release())(work)
    }

/** One etcd native lock generation with an epoch-scoped revision fence. */
final class EtcdLease private[scala3] (private val underlying: org.dilock.etcd.EtcdLease)
    extends AutoCloseable:
  /** Checks local validity; the protected resource must enforce returned fences atomically. */
  def assertValid(): Unit = underlying.assertValid()
  /** Returns an etcd create-revision fence after exact-key verification. */
  def fence: EtcdFence = underlying.fence()
  /** Performs bounded native Unlock and lease Revoke. */
  def release(): Either[DilockException, org.dilock.etcd.EtcdLease.ReleaseOutcome] =
    try Right(underlying.release()) catch case error: DilockException => Left(error)
  /** Best-effort bounded cleanup; inspect release for acknowledgement. */
  override def close(): Unit = underlying.close()

private def wholeMillis(value: Duration): Long =
  require(!value.isNegative && !value.isZero && value.getNano % 1_000_000 == 0,
    "duration must be a positive whole number of milliseconds")
  value.toMillis

/** Keeps a block failure primary if best-effort scope cleanup also fails. */
private[scala3] def withLeaseScope[L <: AutoCloseable, A](lease: L)(work: L => A): A =
  var workFailure: Throwable = null
  try work(lease)
  catch
    case failure: Throwable =>
      workFailure = failure
      throw failure
  finally
    try lease.close()
    catch
      case cleanupFailure: Throwable =>
        if workFailure == null then throw cleanupFailure
        if cleanupFailure ne workFailure then workFailure.addSuppressed(cleanupFailure)

/** Maps SDK cleanup failures to Left while letting every user-block failure propagate unchanged. */
private[scala3] def withLeaseResult[L <: AutoCloseable, A](lease: L)(work: L => A): Either[DilockException, A] =
  var workFailure: Throwable = null
  try
    val result = withLeaseScope(lease) { value =>
      try work(value)
      catch
        case failure: Throwable =>
          workFailure = failure
          throw failure
    }
    Right(result)
  catch
    case failure: DilockException if failure ne workFailure => Left(failure)

/** Awaits the explicit release acknowledgement; best-effort close is insufficient for a scope. */
private[scala3] def withLeaseRelease[L, A](lease: L)(
    release: L => Either[DilockException, ?]
)(work: L => A): Either[DilockException, A] =
  var workFailure: Throwable = null
  var result: A = null.asInstanceOf[A]
  try result = work(lease)
  catch case failure: Throwable => workFailure = failure

  var cleanupFailure: Throwable = null
  try
    release(lease) match
      case Left(failure) => cleanupFailure = failure
      case Right(outcome) if releaseWasAcknowledged(outcome) => ()
      case Right(outcome) =>
        cleanupFailure = new DilockException(
          DilockException.Kind.LEASE_LOST,
          s"Scoped lease release did not return RELEASED: $outcome"
        )
  catch case failure: Throwable => cleanupFailure = failure

  if workFailure != null then
    if cleanupFailure != null && (cleanupFailure ne workFailure) then
      workFailure.addSuppressed(cleanupFailure)
    throw workFailure

  cleanupFailure match
    case null => Right(result)
    case failure: DilockException => Left(failure)
    case failure => throw failure

private[scala3] def releaseWasAcknowledged(outcome: Any): Boolean =
  outcome == ReleaseOutcome.RELEASED || outcome == org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED
