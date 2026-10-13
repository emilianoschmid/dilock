// Local-only style example. Production Kotlin/JVM should wrap the Java engine.
// This fixture has no TTL, transport, renewal or coroutine cancellation.
import java.io.Closeable

enum class ReleaseOutcome { Released, AlreadyNotOwned }

class MemoryStore {
    private val owners = mutableMapOf<String, Any>()
    @Synchronized fun claim(resource: String, owner: Any): Boolean {
        if (owners.containsKey(resource)) return false
        owners[resource] = owner
        return true
    }
    @Synchronized fun release(resource: String, owner: Any): ReleaseOutcome {
        if (owners[resource] !== owner) return ReleaseOutcome.AlreadyNotOwned
        owners.remove(resource)
        return ReleaseOutcome.Released
    }
    @Synchronized fun expireForTest(resource: String) { owners.remove(resource) }
}

/** One local acquisition; use() closes synchronously, not over remote async I/O. */
class Lease internal constructor(
    private val store: MemoryStore,
    private val resource: String,
    private val owner: Any,
) : Closeable {
    private var closed = false
    @Synchronized fun release(): ReleaseOutcome {
        if (closed) return ReleaseOutcome.AlreadyNotOwned
        closed = true
        return store.release(resource, owner)
    }
    override fun close() { release() }
}

class ResourceLock internal constructor(private val store: MemoryStore, private val resource: String) {
    init { require(resource.isNotEmpty()) { "empty resource" } }
    fun tryAcquire(): Lease? {
        val owner = Any()
        return if (store.claim(resource, owner)) Lease(store, resource, owner) else null
    }
}
class Provider(private val store: MemoryStore) {
    fun lock(resource: String) = ResourceLock(store, resource)
}

fun main() {
    val store = MemoryStore()
    val lock = Provider(store).lock("example/resource")
    val first = checkNotNull(lock.tryAcquire())
    check(lock.tryAcquire() == null)
    store.expireForTest("example/resource")
    val second = checkNotNull(lock.tryAcquire())
    check(first.release() == ReleaseOutcome.AlreadyNotOwned)
    check(lock.tryAcquire() == null)
    check(second.release() == ReleaseOutcome.Released)
    check(second.release() == ReleaseOutcome.AlreadyNotOwned)
    class WorkFailure : RuntimeException()
    try {
        checkNotNull(lock.tryAcquire()).use { throw WorkFailure() }
    } catch (_: WorkFailure) { }
    checkNotNull(lock.tryAcquire()).use { }
    check(runCatching { Provider(store).lock("") }.isFailure)
    println("PASS kotlin: busy, stale owner, idempotence, use, validation")
}
