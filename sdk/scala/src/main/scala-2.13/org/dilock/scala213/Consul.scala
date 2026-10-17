package org.dilock.scala213

import org.dilock.redis.DilockException

/** Scala entry points for the Consul KV session profile. */
object DilockConsul {
  def connect(endpoint: String, namespace: String, epoch: String): ConsulProvider =
    new ConsulProvider(org.dilock.consul.ConsulProvider.connect(endpoint, namespace, epoch))
  def connect(options: org.dilock.consul.ConsulProviderOptions): ConsulProvider =
    new ConsulProvider(org.dilock.consul.ConsulProvider.connect(options))
}

final class ConsulProvider private[scala213] (private val underlying: org.dilock.consul.ConsulProvider)
    extends AutoCloseable {
  def lock(resource: String): ConsulLock = new ConsulLock(underlying.lock(resource))
  override def close(): Unit = underlying.close()
}

final class ConsulLock private[scala213] (private val underlying: org.dilock.consul.ConsulLock) {
  def acquire(policy: AcquirePolicy = AcquirePolicy.default): Either[DilockException, ConsulLease] =
    try Right(new ConsulLease(underlying.acquire(policy.toJava)))
    catch { case error: DilockException => Left(error) }
  def tryAcquire(): Either[DilockException, Option[ConsulLease]] =
    try Right(Option(underlying.tryAcquire().orElse(null)).map(new ConsulLease(_)))
    catch { case error: DilockException => Left(error) }
}

final class ConsulLease private[scala213] (private val underlying: org.dilock.consul.ConsulLease)
    extends AutoCloseable {
  def assertValid(): Unit = underlying.assertValid()
  def fence: org.dilock.consul.ConsulFence = underlying.fence()
  def release(): Either[DilockException, org.dilock.consul.ConsulLease.ReleaseOutcome] =
    try Right(underlying.release()) catch { case error: DilockException => Left(error) }
  override def close(): Unit = underlying.close()
}
