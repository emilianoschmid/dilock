package org.dilock.kotlin.worker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.dilock.kotlin.DilockEtcd
import org.dilock.kotlin.DilockRedis
import org.dilock.kotlin.EtcdLease as KotlinEtcdLease
import org.dilock.kotlin.EtcdProvider as KotlinEtcdProvider
import org.dilock.kotlin.RedisLease as KotlinRedisLease
import org.dilock.kotlin.RedisProvider as KotlinRedisProvider
import org.dilock.redis.DilockException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private const val MAX_LINE_BYTES = 64 * 1024
private const val MAX_ACTIVE_REQUESTS = 16
private const val MAX_HELD_LEASES = 16
private const val SHUTDOWN_SETTLE_MS = 1_000L
private const val SHUTDOWN_CLEANUP_MS = 1_500L
private val json = ObjectMapper()

/** Internal test-only JSONL adapter. It is not part of the published SDK artifacts. */
fun main(args: Array<String>) = runBlocking {
    require(args.size == 1) { "usage: dilock-kotlin-worker <config.json>" }
    val config = json.readTree(Files.readString(Path.of(args[0])))
    val endpoint = requiredText(config, "endpoint")
    val namespace = requiredText(config, "namespace")
    val epoch = requiredText(config, "epoch")
    val backend = requiredText(config, "backend")
    val providers = when (backend) {
        "redis" -> Providers(redis = DilockRedis.connect(endpoint, namespace, epoch))
        "etcd" -> Providers(etcd = DilockEtcd.connect(endpoint, namespace, epoch))
        else -> error("backend must be redis or etcd")
    }
    val worker = JsonlWorker(backend, providers)
    worker.run()
}

private data class Providers(
    val redis: KotlinRedisProvider? = null,
    val etcd: KotlinEtcdProvider? = null,
)

private class PendingRequest(val job: Job, val possiblyDispatched: AtomicBoolean)

private class JsonlWorker(private val backend: String, private val providers: Providers) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val activeIds = ConcurrentHashMap.newKeySet<String>()
    private val pending = ConcurrentHashMap<String, PendingRequest>()
    private val leases = ConcurrentHashMap<String, Any>()
    private val released = ConcurrentHashMap<String, String>()
    private val heldLeaseSlots = java.util.concurrent.atomic.AtomicInteger()
    private val outputLock = Any()
    private val stopping = AtomicBoolean(false)
    private val shutdownCleanupDone = AtomicBoolean(false)

    suspend fun run() {
        try {
            while (!stopping.get()) {
                val line = readBoundedLine() ?: break
                val request = try {
                    parseRequest(line)
                } catch (_: Exception) {
                    System.err.println("worker: malformed JSON input")
                    continue
                }
                val id = request.path("id").takeIf(JsonNode::isTextual)?.asText().orEmpty()
                val op = request.path("op").takeIf(JsonNode::isTextual)?.asText().orEmpty()
                if (id.isEmpty() || id.length > 128 || op.isEmpty()) {
                    System.err.println("worker: malformed request envelope")
                    continue
                }
                if (!activeIds.add(id)) {
                    write(envelope(id, "invalid_argument"))
                    continue
                }
                if (op !in setOf("cancel_request", "shutdown") && activeIds.size > MAX_ACTIVE_REQUESTS) {
                    activeIds.remove(id)
                    write(envelope(id, "invalid_argument").put("error", "worker active-request limit reached"))
                    continue
                }
                when (op) {
                    "cancel_request" -> cancel(id, request)
                    "shutdown" -> shutdown(id)
                    else -> dispatch(id, op, request)
                }
            }
        } finally {
            stopAndClose()
        }
    }

    private fun dispatch(id: String, op: String, request: JsonNode) {
        val possiblyDispatched = AtomicBoolean(false)
        val responded = AtomicBoolean(false)
        val job = scope.launch(start = CoroutineStart.LAZY) {
            try {
                process(id, op, request, possiblyDispatched, responded)
            } catch (error: Throwable) {
                val result = envelope(id, mapError(error))
                if (isCancellation(error) || error is InterruptedException) {
                    result.put("remote_outcome_unknown", remoteOutcomeUnknown(error, possiblyDispatched.get()))
                }
                if (responded.compareAndSet(false, true)) write(result)
            } finally {
                pending.remove(id)
                activeIds.remove(id)
            }
        }
        pending[id] = PendingRequest(job, possiblyDispatched)
        job.invokeOnCompletion {
            // Covers cancellation before the coroutine body gets scheduled.
            if (it is CancellationException && responded.compareAndSet(false, true)) {
                write(envelope(id, "cancelled").put("remote_outcome_unknown", possiblyDispatched.get()))
                pending.remove(id)
                activeIds.remove(id)
            }
        }
        job.start()
    }

    private fun cancel(id: String, request: JsonNode) {
        val targetId = request.path("target_id").takeIf(JsonNode::isTextual)?.asText().orEmpty()
        val target = pending[targetId]
        val cancelled = target?.let { requestPending ->
            if (requestPending.job.isActive) {
                requestPending.job.cancel(CancellationException("cancel_request received"))
                true
            } else false
        } ?: false
        val response = envelope(id, "ok")
        response.put("cancelled", cancelled)
        write(response)
        activeIds.remove(id)
    }

    private suspend fun process(
        id: String,
        op: String,
        request: JsonNode,
        possiblyDispatched: AtomicBoolean,
        responded: AtomicBoolean,
    ) {
        val response = when (op) {
            "hello" -> hello(id)
            "acquire", "try_acquire" -> acquire(id, op, request, possiblyDispatched)
            "renew" -> renew(id, request)
            "release" -> release(id, request)
            "inspect" -> inspect(id, request)
            else -> envelope(id, "invalid_argument")
        }
        if (responded.compareAndSet(false, true)) write(response)
    }

    private fun hello(id: String): ObjectNode = envelope(id, "ok").also { out ->
        out.put("sdk_version", "0.1.0")
        out.put("protocol", 1)
        out.put("backend", backend)
        out.put("server_version", "unknown")
        out.set<ObjectNode>("capabilities", json.createObjectNode()
            .put("renewal", backend == "redis")
            .put("auto_renew", false)
            .put("fencing", backend == "etcd")
            .put("try_acquire", backend == "redis"))
    }

    private suspend fun acquire(id: String, op: String, request: JsonNode, dispatched: AtomicBoolean): ObjectNode {
        val resource = requiredText(request, "resource")
        val leaseMs = positiveLong(request, "lease_ms", 30_000L)
        val waitMs = nonNegativeLong(request, "wait_ms", 5_000L)
        if (request.path("auto_renew").asBoolean(false)) return envelope(id, "unsupported_capability")
        if (op == "try_acquire" && backend == "etcd") return envelope(id, "unsupported_capability")
        if (!reserveLeaseSlot()) {
            return envelope(id, "invalid_argument").put("error", "worker held-lease limit reached")
        }
        var retainSlot = false
        try {
            dispatched.set(true)
            val lease: Any? = when (backend) {
                "redis" -> withContext(Dispatchers.IO) {
                    val lock = requireNotNull(providers.redis).lock(resource)
                    if (op == "try_acquire") lock.tryAcquire(java.time.Duration.ofMillis(leaseMs))
                    else lock.acquire(org.dilock.kotlin.AcquirePolicy(java.time.Duration.ofMillis(waitMs), java.time.Duration.ofMillis(leaseMs)))
                }
                "etcd" -> withContext(Dispatchers.IO) {
                    val lock = requireNotNull(providers.etcd).lock(resource)
                    lock.acquire(org.dilock.kotlin.AcquirePolicy(java.time.Duration.ofMillis(waitMs), java.time.Duration.ofMillis(leaseMs)))
                }
                else -> error("unsupported backend")
            }
            if (lease == null) return envelope(id, "busy")
            val localId = UUID.randomUUID().toString()
            leases[localId] = lease
            retainSlot = true
            return envelope(id, "acquired").also { out ->
                out.put("lease_id", localId)
                when (lease) {
                    is KotlinRedisLease -> out.put("effective_lease_ms", leaseMs)
                    is KotlinEtcdLease -> {
                        out.put("effective_lease_ms", leaseMs)
                        val leaseFence = lease.fence()
                        val fence = json.createObjectNode()
                        fence.put("epoch", leaseFence.epoch())
                        fence.put("value", leaseFence.value().toString())
                        out.set<ObjectNode>("fence", fence)
                    }
                }
            }
        } finally {
            if (!retainSlot) heldLeaseSlots.decrementAndGet()
        }
    }

    private suspend fun renew(id: String, request: JsonNode): ObjectNode {
        val leaseId = requiredText(request, "lease_id")
        val lease = leases[leaseId] ?: return if (released.containsKey(leaseId)) envelope(id, "already_not_owned") else envelope(id, "invalid_argument")
        val leaseMs = positiveLong(request, "lease_ms", 30_000L)
        return when (lease) {
            is KotlinRedisLease -> {
                lease.renew(java.time.Duration.ofMillis(leaseMs))
                envelope(id, "renewed")
            }
            else -> envelope(id, "unsupported_capability")
        }
    }

    private suspend fun release(id: String, request: JsonNode): ObjectNode {
        val leaseId = requiredText(request, "lease_id")
        released[leaseId]?.let { return envelope(id, it) }
        val lease = leases[leaseId] ?: return envelope(id, "invalid_argument")
        val outcome = when (lease) {
            is KotlinRedisLease -> lease.release().toString()
            is KotlinEtcdLease -> lease.release().toString()
            else -> error("unknown lease type")
        }
        val status = if (outcome == "RELEASED") "released" else "already_not_owned"
        if (leases.remove(leaseId, lease)) heldLeaseSlots.decrementAndGet()
        released[leaseId] = status
        return envelope(id, status)
    }

    private fun inspect(id: String, request: JsonNode): ObjectNode {
        val leaseId = requiredText(request, "lease_id")
        val lease = leases[leaseId] ?: return envelope(id, "invalid_argument")
        val state = when (lease) {
            is KotlinRedisLease -> localState(lease)
            is KotlinEtcdLease -> localState(lease)
            else -> error("unknown lease type")
        }
        return envelope(id, "ok").put("state", state.lowercase())
    }

    private suspend fun shutdown(id: String) {
        if (!stopping.compareAndSet(false, true)) {
            write(envelope(id, "invalid_argument"))
            activeIds.remove(id)
            return
        }
        activeIds.remove(id)
        val waiting = pending.values.map { it.job }
        waiting.forEach { it.cancel(CancellationException("worker shutdown")) }
        withTimeoutOrNull(SHUTDOWN_SETTLE_MS) { waiting.joinAll() }
        val unsettledRequests = waiting.count { !it.isCompleted }

        val attempted = java.util.concurrent.atomic.AtomicInteger()
        val failures = ConcurrentHashMap.newKeySet<String>()
        val cleanupScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val cleanups = leases.entries.toList().map { entry ->
            cleanupScope.launch {
                attempted.incrementAndGet()
                try {
                    val outcome = when (val lease = entry.value) {
                        is KotlinRedisLease -> lease.release().toString()
                        is KotlinEtcdLease -> lease.release().toString()
                        else -> error("unknown lease type")
                    }
                    if (leases.remove(entry.key, entry.value)) heldLeaseSlots.decrementAndGet()
                    released[entry.key] = if (outcome == "RELEASED") "released" else "already_not_owned"
                } catch (_: Throwable) {
                    failures.add(entry.key)
                }
            }
        }
        val cleanupFinished = withTimeoutOrNull(SHUTDOWN_CLEANUP_MS) {
            cleanups.joinAll()
            true
        } ?: false
        val unfinished = cleanups.count { !it.isCompleted }
        if (!cleanupFinished) cleanups.forEach { it.cancel() }
        cleanupScope.coroutineContext[Job]?.cancel()
        shutdownCleanupDone.set(true)
        val response = envelope(id, "ok")
        response.put("cleanup_attempts", attempted.get())
        response.put("outstanding_cleanup_attempts", failures.size + unfinished + unsettledRequests)
        write(response)
        scope.coroutineContext[Job]?.cancel()
    }

    private suspend fun stopAndClose() {
        val jobs = pending.values.map { it.job }
        jobs.forEach { it.cancel() }
        withTimeoutOrNull(2_000) { jobs.forEach { it.cancelAndJoin() } }
        if (!shutdownCleanupDone.get()) {
            val attempts = leases.values.toList()
            attempts.forEach { lease ->
                try {
                    when (lease) {
                        is KotlinRedisLease -> lease.release()
                        is KotlinEtcdLease -> lease.release()
                    }
                } catch (_: Throwable) {
                    System.err.println("worker: outstanding lease cleanup was not confirmed")
                }
            }
        }
        withContext(Dispatchers.IO) {
            providers.redis?.close()
            providers.etcd?.close()
        }
        scope.coroutineContext[Job]?.cancel()
    }

    private fun reserveLeaseSlot(): Boolean {
        while (true) {
            val current = heldLeaseSlots.get()
            if (current >= MAX_HELD_LEASES) return false
            if (heldLeaseSlots.compareAndSet(current, current + 1)) return true
        }
    }

    private fun readBoundedLine(): String? {
        val bytes = ByteArray(MAX_LINE_BYTES + 1)
        var size = 0
        var oversized = false
        while (true) {
            val value = System.`in`.read()
            if (value < 0) {
                if (size == 0 && !oversized) return null
                break
            }
            if (value == '\n'.code) break
            if (size < bytes.size) bytes[size++] = value.toByte() else oversized = true
        }
        if (oversized || size > MAX_LINE_BYTES) throw IllegalStateException("worker input line exceeds 65536 bytes")
        var length = size
        if (length > 0 && bytes[length - 1] == '\r'.code.toByte()) length--
        return bytes.copyOf(length).toString(Charsets.UTF_8)
    }

    private fun write(response: ObjectNode) = synchronized(outputLock) {
        try {
            System.out.println(json.writeValueAsString(response))
            System.out.flush()
        } catch (_: Exception) {
            System.err.println("worker: failed encoding response")
        }
    }
}

private fun localState(lease: org.dilock.kotlin.LeaseHandle): String = try {
    lease.assertValid()
    "held"
} catch (_: DilockException) {
    "lost"
}

private fun envelope(id: String, status: String): ObjectNode = json.createObjectNode().put("id", id).put("status", status)
internal fun parseRequest(line: String): ObjectNode {
    val request = json.readTree(line)
    require(request != null && request.isObject) { "request must be a JSON object" }
    val allowed = setOf("id", "op", "resource", "lease_id", "target_id", "wait_ms", "lease_ms", "auto_renew")
    require(request.fieldNames().asSequence().all(allowed::contains)) { "request has unknown fields" }
    requiredText(request, "id").also { require(it.length <= 128) { "id exceeds 128 characters" } }
    val op = requiredText(request, "op")
    require(op in setOf("hello", "acquire", "try_acquire", "renew", "release", "inspect", "cancel_request", "shutdown")) { "unknown operation" }
    when (op) {
        "acquire", "try_acquire" -> requiredText(request, "resource")
        "release", "inspect", "renew" -> requiredText(request, "lease_id")
        "cancel_request" -> requiredText(request, "target_id")
    }
    request.get("wait_ms")?.also { require(it.isIntegralNumber && it.canConvertToLong() && it.asLong() >= 0) { "wait_ms must be a non-negative integer" } }
    request.get("lease_ms")?.also { require(it.isIntegralNumber && it.canConvertToLong() && it.asLong() > 0) { "lease_ms must be a positive integer" } }
    request.get("auto_renew")?.also { require(it.isBoolean) { "auto_renew must be boolean" } }
    return request as ObjectNode
}
private fun requiredText(node: JsonNode, name: String): String = node.path(name).takeIf(JsonNode::isTextual)?.asText()?.takeIf(String::isNotEmpty)
    ?: throw IllegalArgumentException("$name is required")
private fun positiveLong(node: JsonNode, name: String, default: Long): Long = integer(node, name, default).also {
    require(it > 0 && it <= 86_400_000) { "$name must be in 1..86400000" }
}
private fun nonNegativeLong(node: JsonNode, name: String, default: Long): Long = integer(node, name, default).also {
    require(it >= 0 && it <= 86_400_000) { "$name must be in 0..86400000" }
}
private fun integer(node: JsonNode, name: String, default: Long): Long {
    val field = node.get(name) ?: return default
    require(field.isIntegralNumber && field.canConvertToLong()) { "$name must be an integer" }
    return field.asLong()
}
private fun mapError(error: Throwable): String {
    val root = generateSequence(error) { it.cause }.firstOrNull { it is DilockException }
    if (root is DilockException) return when (root.kind()) {
        DilockException.Kind.BACKEND_UNAVAILABLE -> "backend_unavailable"
        DilockException.Kind.PROTOCOL_MISMATCH -> "protocol_mismatch"
        DilockException.Kind.ACQUISITION_UNKNOWN -> "acquisition_unknown"
        DilockException.Kind.ACQUISITION_TIMEOUT -> "timeout"
        DilockException.Kind.RELEASE_UNKNOWN -> "release_unknown"
        DilockException.Kind.LEASE_LOST -> "already_not_owned"
        DilockException.Kind.UNSUPPORTED_CAPABILITY -> "unsupported_capability"
    }
    return if (error is IllegalArgumentException) "invalid_argument" else if (isCancellation(error) || error is InterruptedException) "cancelled" else "internal_error"
}
private fun isCancellation(error: Throwable): Boolean = generateSequence(error) { it.cause }.any { it is CancellationException }
private fun remoteOutcomeUnknown(error: Throwable, possiblyDispatched: Boolean): Boolean {
    val cause = generateSequence(error) { it.cause }.firstOrNull { it is InterruptedException }
    return when (cause) {
        is org.dilock.etcd.EtcdAcquisitionCancelledException -> cause.remoteOutcomeUnknown()
        is InterruptedException -> cause.message?.contains("after send") == true
        else -> possiblyDispatched
    }
}
