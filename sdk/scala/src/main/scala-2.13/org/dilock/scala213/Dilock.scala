package org.dilock.scala213

import java.time.Duration
import org.dilock.redis.{AcquireOptions => JavaAcquireOptions, DilockException}

/** Immutable policy for one PostgreSQL advisory-lock acquisition. */
final case class AcquirePolicy(
    waitTimeout: Duration = Duration.ofSeconds(5),
    lease: Duration = Duration.ofSeconds(30)
) {
  def toJava: JavaAcquireOptions = new JavaAcquireOptions(waitTimeout, lease)
}
// End of the Scala 2.13 facade.
object AcquirePolicy { val default: AcquirePolicy = AcquirePolicy() }

object DilockPostgres {
  def connect(jdbcUrl: String, epoch: String): PostgresProvider =
    new PostgresProvider(org.dilock.postgres.PostgresProvider.connect(jdbcUrl, epoch))
  def connect(jdbcUrl: String, namespace: String, epoch: String): PostgresProvider =
    new PostgresProvider(org.dilock.postgres.PostgresProvider.connect(jdbcUrl, namespace, epoch))
}
final class PostgresProvider private[scala213] (
    private val underlying: org.dilock.postgres.PostgresProvider
) extends AutoCloseable {
  def lock(resource: String): PostgresLock = new PostgresLock(underlying.lock(resource))
  override def close(): Unit = underlying.close()
}
final class PostgresLock private[scala213] (
    private val underlying: org.dilock.postgres.PostgresLock
) {
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, PostgresLease] =
    try Right(new PostgresLease(underlying.acquire(policy.toJava)))
    catch { case error: DilockException => Left(error) }

  def tryAcquire(): Either[DilockException, Option[PostgresLease]] =
    try {
      val lease = underlying.tryAcquire().orElse(null)
      Right(Option(lease).map(new PostgresLease(_)))
    } catch { case error: DilockException => Left(error) }
}
final class PostgresLease private[scala213] (
    private val underlying: org.dilock.postgres.PostgresLease
) extends AutoCloseable {
  def assertValid(): Unit = underlying.assertValid()
  /** Liveness check only; it cannot extend a PostgreSQL advisory lock. */
  def renew(): Unit = underlying.renew()
  def release(): org.dilock.postgres.PostgresLease.ReleaseOutcome = underlying.release()
  override def close(): Unit = underlying.close()
}
