package org.dilock.kotlin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.dilock.etcd.EtcdFence
import org.dilock.redis.AcquireOptions as JavaAcquireOptions
import org.dilock.redis.RedisProviderOptions as JavaRedisProviderOptions
import org.dilock.etcd.EtcdProviderOptions as JavaEtcdProviderOptions
import org.dilock.redis.CleanupObserver
import org.dilock.redis.DilockException
import java.time.Duration
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resumeWithException

/** Immutable policy for one acquisition. The durations use whole milliseconds. */
data class AcquirePolicy(val wait: Duration = Duration.ofSeconds(5), val lease: Duration = Duration.ofSeconds(30)) {
    init { toJava() }

    internal fun toJava(): JavaAcquireOptions = JavaAcquireOptions(wait, lease)

    companion object {
        /** The finite default wait and lease policy. */
        @JvmField val DEFAULT = AcquirePolicy()
    }
}

/** etcd acquisition policy with optional renewal of the granted server lease while held. */
data class EtcdAcquirePolicy(
    val wait: Duration = Duration.ofSeconds(5),
    val lease: Duration = Duration.ofSeconds(30),
    val autoRenew: Boolean = false,
) {
    /** Validates whole-millisecond timing before the Java etcd adapter is called. */
    internal fun javaArguments(): Triple<Long, Long, Boolean> =
        Triple(wholeMillis(wait, "wait"), wholeMillis(lease, "lease"), autoRenew)

    companion object {
        /** The finite default wait and lease policy; renewal is off by default. */
        @JvmField val DEFAULT = EtcdAcquirePolicy()
    }
}

/** Complete connection policy shared by either backend. The namespace must be pre-provisioned. */
data class ProviderSettings(
    val endpoint: String,
    val namespace: String = "default",
    val epoch: String,
    val rpcTimeout: Duration = Duration.ofSeconds(2),
    val cleanupTimeout: Duration = Duration.ofSeconds(2),
    val defaultAcquisition: AcquirePolicy = AcquirePolicy.DEFAULT,
    val cleanupObserver: CleanupObserver? = null,
) {
    internal fun toRedisJava(): JavaRedisProviderOptions = JavaRedisProviderOptions(
        endpoint, namespace, epoch, rpcTimeout, cleanupTimeout, defaultAcquisition.toJava(), cleanupObserver
    )

    internal fun toEtcdJava(): JavaEtcdProviderOptions = JavaEtcdProviderOptions(
        endpoint, namespace, epoch, rpcTimeout, cleanupTimeout, defaultAcquisition.toJava(), cleanupObserver
    )
}

/** Acknowledged result of releasing a Kotlin lease, shared across both backends. */
enum class ReleaseOutcome {
    /** This release removed the matching owner. */
    RELEASED,
    /** The owner was already absent or had been replaced by a successor. */
    ALREADY_NOT_OWNED,
}

/** Constructors for the Redis facade. The Java provider owns its clients until [RedisProvider.close]. */
object DilockRedis {
    /** Connects to the default namespace using standard RPC and acquisition budgets. */
    @JvmStatic fun connect(endpoint: String, epoch: String): RedisProvider =
        RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, epoch))

    /** Connects to a pre-provisioned namespace using standard RPC and acquisition budgets. */
    @JvmStatic fun connect(endpoint: String, namespace: String, epoch: String): RedisProvider =
        RedisProvider(org.dilock.redis.RedisProvider.connect(endpoint, namespace, epoch))

    /** Connects using the complete immutable Java provider policy. No metadata is created or repaired. */
    @JvmStatic fun connect(options: JavaRedisProviderOptions): RedisProvider =
        RedisProvider(org.dilock.redis.RedisProvider.connect(options))

    /** Connects using the complete Kotlin-native provider policy. */
    @JvmStatic fun connect(settings: ProviderSettings): RedisProvider = connect(settings.toRedisJava())
}

/** Constructors for the native etcd Lock profile. The Java provider owns its channel until close. */
object DilockEtcd {
    /** Connects to the default namespace using standard RPC and acquisition budgets. */
    @JvmStatic fun connect(endpoint: String, epoch: String): EtcdProvider =
        EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, epoch))

    /** Connects to a pre-provisioned namespace using standard RPC and acquisition budgets. */
    @JvmStatic fun connect(endpoint: String, namespace: String, epoch: String): EtcdProvider =
        EtcdProvider(org.dilock.etcd.EtcdProvider.connect(endpoint, namespace, epoch))

    /** Connects using the complete immutable Java provider policy. No metadata is created or repaired. */
    @JvmStatic fun connect(options: JavaEtcdProviderOptions): EtcdProvider =
        EtcdProvider(org.dilock.etcd.EtcdProvider.connect(options))

    /** Connects using the complete Kotlin-native provider policy. */
    @JvmStatic fun connect(settings: ProviderSettings): EtcdProvider = connect(settings.toEtcdJava())
}

/** Shared surface for backend-bound locks. Each handle reuses its provider transport. */
sealed interface LockHandle {
    /** Acquires on a worker thread; coroutine cancellation interrupts the Java acquisition. */
    suspend fun acquire(policy: AcquirePolicy = AcquirePolicy.DEFAULT): LeaseHandle
}

/** Shared lease operations exposed by the Kotlin facade. */
sealed interface LeaseHandle : AutoCloseable {
    /** Checks local lease validity; this does not stop a paused process or fence external writes. */
    fun assertValid()

    /** Performs and awaits bounded explicit cleanup; uncertainty remains a typed Java DilockException. */
    suspend fun release(): ReleaseOutcome

    /** Bounded best-effort cleanup for synchronous Kotlin `use`. */
    override fun close()
}

/** Owns one Java Redis provider and its client pool. Safe for concurrent use. */
class RedisProvider internal constructor(private val delegate: org.dilock.redis.RedisProvider) : AutoCloseable {
    /** Returns a reusable handle; handles from this provider share one client pool. */
    fun lock(resource: String): RedisLock = RedisLock(delegate.lock(resource))

    /** Bounded, idempotent provider cleanup. */
    override fun close() = delegate.close()
}

/** Reusable handle for one Redis resource. */
class RedisLock internal constructor(private val delegate: org.dilock.redis.RedisLock) : LockHandle {
    override suspend fun acquire(policy: AcquirePolicy): RedisLease =
        interruptibleResource({ RedisLease(delegate.acquire(policy.toJava())) }, RedisLease::close)

    /** Performs one non-waiting Redis attempt; null means ordinary contention only. */
    suspend fun tryAcquire(lease: Duration = AcquirePolicy.DEFAULT.lease): RedisLease? =
        interruptibleResource(
            block = {
                val result: Optional<org.dilock.redis.RedisLease> = delegate.tryAcquire(wholeMillis(lease, "lease"))
                result.orElse(null)?.let(::RedisLease)
            },
            discard = { it?.close() },
        )

    /** Runs a suspending block while held and attempts bounded cleanup in a non-cancellable IO context. */
    suspend fun <T> withLease(policy: AcquirePolicy = AcquirePolicy.DEFAULT, block: suspend (RedisLease) -> T): T =
        scopedAcquire({ acquire(policy) }, { requireReleaseAcknowledged(it.release()) }, block)
}

/** One Redis owner generation. It is not a fencing token or a process-local mutex. */
class RedisLease internal constructor(internal val delegate: org.dilock.redis.RedisLease) : LeaseHandle {
    override fun assertValid() = delegate.assertValid()
    /** Performs bounded owner-token-checked release. */
    override suspend fun release(): ReleaseOutcome = boundedCleanupIo {
        redisReleaseOutcome(delegate.release())
    }
    /** Best-effort close reports uncertain cleanup through the configured Java observer. */
    override fun close() = delegate.close()
    /** Atomic owner-checked extension; transport failures and lost ownership throw typed errors. */
    suspend fun renew(lease: Duration) = interruptibleIo { delegate.renew(wholeMillis(lease, "lease")) }
}

/** Owns one etcd provider and gRPC channel. Safe for concurrent use. */
class EtcdProvider internal constructor(private val delegate: org.dilock.etcd.EtcdProvider) : AutoCloseable {
    /** Returns a reusable handle; handles from this provider share its channel. */
    fun lock(resource: String): EtcdLock = EtcdLock(delegate.lock(resource))
    /** Bounded, idempotent provider cleanup. */
    override fun close() = delegate.close()
}

/** Reusable handle for one etcd native lock name. */
class EtcdLock internal constructor(private val delegate: org.dilock.etcd.EtcdLock) : LockHandle {
    override suspend fun acquire(policy: AcquirePolicy): EtcdLease =
        interruptibleResource({ EtcdLease(delegate.acquire(policy.toJava())) }, EtcdLease::close)

    /** Acquires with an etcd-specific policy, including optional held-lease renewal. */
    suspend fun acquire(policy: EtcdAcquirePolicy): EtcdLease =
        interruptibleResource({
            val (waitMillis, leaseMillis, autoRenew) = policy.javaArguments()
            EtcdLease(delegate.acquire(waitMillis, leaseMillis, autoRenew))
        }, EtcdLease::close)

    /** etcd native Lock has no safe try-lock operation; this fails explicitly without an RPC. */
    suspend fun tryAcquire(@Suppress("UNUSED_PARAMETER") lease: Duration = AcquirePolicy.DEFAULT.lease): Nothing =
        interruptibleIo { delegate.tryAcquire(wholeMillis(lease, "lease")); error("unreachable") }

    /** Runs a suspending block while held and attempts bounded cleanup in a non-cancellable IO context. */
    suspend fun <T> withLease(policy: AcquirePolicy = AcquirePolicy.DEFAULT, block: suspend (EtcdLease) -> T): T =
        scopedAcquire({ acquire(policy) }, { requireReleaseAcknowledged(it.release()) }, block)

    /** Runs a suspending block under etcd's optional held-lease renewal policy. */
    suspend fun <T> withLease(policy: EtcdAcquirePolicy, block: suspend (EtcdLease) -> T): T =
        scopedAcquire({ acquire(policy) }, { requireReleaseAcknowledged(it.release()) }, block)
}

/** One etcd native lock generation. [fence] still requires resource-side atomic enforcement. */
class EtcdLease internal constructor(internal val delegate: org.dilock.etcd.EtcdLease) : LeaseHandle {
    override fun assertValid() = delegate.assertValid()
    /** Returns an epoch-scoped etcd revision fence. */
    fun fence(): EtcdFence = delegate.fence()
    /** Performs bounded native Unlock and lease Revoke. */
    override suspend fun release(): ReleaseOutcome = boundedCleanupIo {
        etcdReleaseOutcome(delegate.release())
    }
    /** Best-effort close reports uncertain cleanup through the configured Java observer. */
    override fun close() = delegate.close()
}

internal suspend fun <L, T> scopedAcquire(
    acquire: suspend () -> L,
    release: suspend (L) -> Unit,
    block: suspend (L) -> T,
): T {
    val lease = acquire()
    var workFailure: Throwable? = null
    try {
        return block(lease)
    } catch (failure: Throwable) {
        workFailure = failure
        throw failure
    } finally {
        try {
            // `close()` intentionally swallows unknown acknowledgement and only emits diagnostics.
            // A structured `withLease` scope must surface that uncertainty to its caller.
            withContext(NonCancellable + Dispatchers.IO) { release(lease) }
        } catch (cleanupFailure: Throwable) {
            val primary = workFailure
            if (primary == null) throw cleanupFailure
            if (cleanupFailure !== primary) primary.addSuppressed(cleanupFailure)
        }
    }
}

/** A structured scope is successful only after release confirms that this owner was removed. */
internal fun requireReleaseAcknowledged(outcome: ReleaseOutcome) {
    if (outcome == ReleaseOutcome.RELEASED) return
    throw DilockException(
        DilockException.Kind.LEASE_LOST,
        "Scoped lease release did not return RELEASED: $outcome",
    )
}

internal fun redisReleaseOutcome(outcome: org.dilock.redis.RedisLease.ReleaseOutcome): ReleaseOutcome =
    if (outcome == org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED) {
        ReleaseOutcome.RELEASED
    } else {
        ReleaseOutcome.ALREADY_NOT_OWNED
    }

internal fun etcdReleaseOutcome(outcome: org.dilock.etcd.EtcdLease.ReleaseOutcome): ReleaseOutcome =
    if (outcome == org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED) {
        ReleaseOutcome.RELEASED
    } else {
        ReleaseOutcome.ALREADY_NOT_OWNED
    }

internal suspend fun <T> interruptibleIo(block: () -> T): T = runInterruptible(Dispatchers.IO, block = block)

/**
 * Runs a blocking acquisition on IO and interrupts it on cancellation. If a successful result is
 * cancelled before it reaches the caller, [discard] closes the lease instead of orphaning it.
 */
internal suspend fun <T> interruptibleResource(
    block: () -> T,
    discard: (T) -> Unit,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
): T =
    suspendCancellableCoroutine { continuation ->
        val worker = AtomicReference<Thread?>()
        continuation.invokeOnCancellation { worker.get()?.interrupt() }
        ioDispatcher.dispatch(continuation.context, Runnable {
            // Publish the worker before checking cancellation. If cancellation won after
            // an earlier check but before publication, the callback could not interrupt
            // this thread and a blocking acquisition would continue until its RPC timeout.
            worker.set(Thread.currentThread())
            if (!continuation.isActive) {
                worker.set(null)
                Thread.interrupted()
                return@Runnable
            }
            val resource = try {
                block()
            } catch (failure: Throwable) {
                worker.set(null)
                Thread.interrupted()
                continuation.resumeWithException(failure)
                return@Runnable
            }
            worker.set(null)
            // Cancellation interrupts the dispatcher thread; do not leak that flag to its next task.
            Thread.interrupted()
            continuation.resume(resource) { cause, cancelledResource, context ->
                // Cancellation callbacks must stay non-blocking; lease close has a bounded
                // network budget, so hand it to an IO worker after ownership is discarded.
                Dispatchers.IO.dispatch(context, Runnable {
                    try {
                        discard(cancelledResource)
                    } catch (cleanupFailure: Throwable) {
                        if (cleanupFailure !== cause) cause.addSuppressed(cleanupFailure)
                    }
                })
            }
        })
    }

/** Release owns an independent cleanup budget, so caller cancellation must not interrupt its RPC. */
internal suspend fun <T> boundedCleanupIo(block: () -> T): T =
    withContext(NonCancellable + Dispatchers.IO) { block() }

private fun wholeMillis(duration: Duration, name: String): Long {
    require(!duration.isNegative && !duration.isZero && duration.nano % 1_000_000 == 0) {
        "$name must be a positive whole-millisecond duration"
    }
    return duration.toMillis()
}
