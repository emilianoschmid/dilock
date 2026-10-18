package org.dilock.scala3

import org.dilock.redis.DilockException

/** Scala facade over the experimental Java PostgreSQL advisory-session provider. */
object DilockPostgres:
  def connect(jdbcUrl: String, namespace: String, epoch: String): PostgresProvider =
    new PostgresProvider(org.dilock.postgres.PostgresProvider.connect(jdbcUrl, namespace, epoch))
  def connect(jdbcUrl: String, epoch: String): PostgresProvider =
    new PostgresProvider(org.dilock.postgres.PostgresProvider.connect(jdbcUrl, epoch))

final class PostgresProvider private[scala3] (private val underlying: org.dilock.postgres.PostgresProvider)
    extends AutoCloseable:
  def lock(resource: String): PostgresLock = new PostgresLock(underlying.lock(resource))
  override def close(): Unit = underlying.close()

final class PostgresLock private[scala3] (private val underlying: org.dilock.postgres.PostgresLock):
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, PostgresLease] =
    try Right(new PostgresLease(underlying.acquire(policy.toJava)))
    catch case error: DilockException => Left(error)
  def tryAcquire(): Either[DilockException, Option[PostgresLease]] =
    try Right(Option(underlying.tryAcquire().orElse(null)).map(new PostgresLease(_)))
    catch case error: DilockException => Left(error)

final class PostgresLease private[scala3] (private val underlying: org.dilock.postgres.PostgresLease)
    extends AutoCloseable:
  def assertValid(): Unit = underlying.assertValid()
  /** Liveness check only; it cannot extend a PostgreSQL advisory-lock TTL. */
  def renew(): Unit = underlying.renew()
  def release(): org.dilock.postgres.PostgresLease.ReleaseOutcome = underlying.release()
  override def close(): Unit = underlying.close()
