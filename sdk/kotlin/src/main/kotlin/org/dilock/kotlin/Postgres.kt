package org.dilock.kotlin

import org.dilock.postgres.PostgresProvider as JavaPostgresProvider
import org.dilock.postgres.PostgresProviderOptions as JavaPostgresOptions
import org.dilock.redis.AcquireOptions as JavaAcquireOptions
import java.time.Duration

/** Kotlin facade over the experimental Java PostgreSQL advisory-session provider. */
object DilockPostgres {
    @JvmStatic fun connect(jdbcUrl: String, epoch: String): PostgresProvider =
        PostgresProvider(JavaPostgresProvider.connect(jdbcUrl, epoch))

    @JvmStatic fun connect(jdbcUrl: String, namespace: String, epoch: String): PostgresProvider =
        PostgresProvider(JavaPostgresProvider.connect(jdbcUrl, namespace, epoch))

    @JvmStatic fun connect(options: JavaPostgresOptions): PostgresProvider =
        PostgresProvider(JavaPostgresProvider.connect(options))
}

class PostgresProvider internal constructor(private val delegate: JavaPostgresProvider) : AutoCloseable {
    fun lock(resource: String): PostgresLock = PostgresLock(delegate.lock(resource))
    override fun close() = delegate.close()
}

class PostgresLock internal constructor(private val delegate: org.dilock.postgres.PostgresLock) {
    fun acquire(policy: AcquirePolicy = AcquirePolicy.DEFAULT): PostgresLease =
        PostgresLease(delegate.acquire(policy.toJava()))

    fun tryAcquire(): PostgresLease? = delegate.tryAcquire().orElse(null)?.let(::PostgresLease)
}

class PostgresLease internal constructor(private val delegate: org.dilock.postgres.PostgresLease) : AutoCloseable {
    fun assertValid() = delegate.assertValid()
    /** Liveness check only; PostgreSQL advisory locks have no TTL to extend. */
    fun renew() = delegate.renew()
    fun release(): org.dilock.postgres.PostgresLease.ReleaseOutcome = delegate.release()
    override fun close() = delegate.close()
}
