package org.dilock.kotlin.worker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockRedis
import org.dilock.redis.RedisProvider as JavaRedisProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.RedisClient
import java.net.URI
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class RedisWorkerProcessIntegrationTest {
    @Test
    fun readerRemainsResponsiveDuringAcquireCancellationExpiryAndShutdown() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_REDIS_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_REDIS_ENDPOINT to run process integration")

        val uri = URI.create(endpoint!!)
        val admin = RedisClient.builder()
            .hostAndPort(uri.host, if (uri.port < 0) 6379 else uri.port)
            .clientConfig(DefaultJedisClientConfig.builder(uri).connectionTimeoutMillis(1_000).socketTimeoutMillis(2_500).build())
            .build()
        val namespace = "kotlin-worker-${UUID.randomUUID()}"
        val epoch = "kotlin-worker-${UUID.randomUUID()}"
        val resource = "pending-cancel-${UUID.randomUUID()}"
        var ownerProvider: org.dilock.kotlin.RedisProvider? = null
        var owner: org.dilock.kotlin.RedisLease? = null
        var session: WorkerProcess? = null
        try {
            assertEquals("created", JavaRedisProvider.provisionNamespace(admin, namespace, epoch))
            ownerProvider = DilockRedis.connect(endpoint, namespace, epoch)
            owner = ownerProvider.lock(resource).acquire(
                AcquirePolicy(Duration.ofSeconds(1), Duration.ofMillis(5_000))
            )
            val expiresAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            session = WorkerProcess.start(endpoint, namespace, epoch)
            session.send("hello-0", "hello")
            assertEquals("ok", session.await("hello-0", 8_000).path("status").asText())

            session.send("acquire-1", "acquire", mapOf(
                "resource" to resource,
                "wait_ms" to 8_000,
                "lease_ms" to 4_000,
            ))
            delay(100)
            session.send("hello-1", "hello")
            assertEquals("ok", session.await("hello-1", 2_000).path("status").asText())
            assertFalse(session.hasResponse("acquire-1"), "the competing acquisition must remain pending while the owner holds the key")

            // Pause Redis briefly so cancellation lands after a request is sent but before its reply.
            pauseRedis(uri, 1_200)
            delay(100)
            session.send("cancel-1", "cancel_request", mapOf("target_id" to "acquire-1"))
            val cancelReply = session.await("cancel-1", 2_000)
            assertEquals("ok", cancelReply.path("status").asText())
            assertTrue(cancelReply.path("cancelled").asBoolean())
            val cancelled = session.await("acquire-1", 4_000)
            assertEquals("cancelled", cancelled.path("status").asText())
            assertTrue(cancelled.path("remote_outcome_unknown").asBoolean(), "an interrupted in-flight Redis request must be reported as remotely uncertain")

            val remainingNanos = expiresAt - System.nanoTime()
            if (remainingNanos > 0) delay(TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 150)
            session.send("successor-1", "acquire", mapOf(
                "resource" to resource,
                "wait_ms" to 1_500,
                "lease_ms" to 4_000,
            ))
            val successor = session.await("successor-1", 4_000)
            assertEquals("acquired", successor.path("status").asText())
            assertNotNull(successor.path("lease_id").takeIf(JsonNode::isTextual))

            assertEquals("already_not_owned", owner.release().name.lowercase())
            session.send("shutdown-1", "shutdown")
            val shutdown = session.await("shutdown-1", 4_000)
            assertEquals("ok", shutdown.path("status").asText())
            assertEquals(1, shutdown.path("cleanup_attempts").asInt())
            assertEquals(0, shutdown.path("outstanding_cleanup_attempts").asInt())
            session.awaitExit(5_000)
            session = null
            Unit
        } finally {
            session?.kill()
            try { owner?.release() } catch (_: RuntimeException) { }
            ownerProvider?.close()
            admin.del("dilock:namespace:$namespace:meta")
            admin.close()
        }
    }

    @Test
    fun processRejectsExcessActiveAcquiresAndHeldLeases() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_REDIS_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_REDIS_ENDPOINT to run process integration")
        val uri = URI.create(endpoint!!)
        val admin = RedisClient.builder()
            .hostAndPort(uri.host, if (uri.port < 0) 6379 else uri.port)
            .clientConfig(DefaultJedisClientConfig.builder(uri).connectionTimeoutMillis(1_000).socketTimeoutMillis(1_000).build())
            .build()
        val namespace = "kotlin-cap-${UUID.randomUUID()}"
        val epoch = "kotlin-cap-${UUID.randomUUID()}"
        var session: WorkerProcess? = null
        var holderProvider: org.dilock.kotlin.RedisProvider? = null
        var holder: org.dilock.kotlin.RedisLease? = null
        try {
            assertEquals("created", JavaRedisProvider.provisionNamespace(admin, namespace, epoch))
            session = WorkerProcess.start(endpoint, namespace, epoch)
            session.send("hello-0", "hello")
            assertEquals("ok", session.await("hello-0", 8_000).path("status").asText())
            val leaseIds = mutableListOf<String>()
            repeat(16) { index ->
                val id = "lease-$index"
                session.send(id, "acquire", mapOf("resource" to "resource-$index", "wait_ms" to 4_000, "lease_ms" to 30_000))
                val acquired = session.await(id)
                assertEquals("acquired", acquired.path("status").asText())
                leaseIds += acquired.path("lease_id").asText()
            }
            session.send("lease-overflow", "acquire", mapOf("resource" to "overflow", "wait_ms" to 0, "lease_ms" to 4_000))
            val capacity = session.await("lease-overflow")
            assertEquals("invalid_argument", capacity.path("status").asText())
            assertEquals("worker held-lease limit reached", capacity.path("error").asText())

            for ((index, leaseId) in leaseIds.withIndex()) {
                session.send("release-$index", "release", mapOf("lease_id" to leaseId))
                assertEquals("released", session.await("release-$index").path("status").asText())
            }

            holderProvider = DilockRedis.connect(endpoint, namespace, epoch)
            holder = holderProvider.lock("active-limit").acquire(AcquirePolicy(Duration.ofSeconds(1), Duration.ofMillis(30_000)))
            repeat(16) { index ->
                session.send("wait-$index", "acquire", mapOf(
                    "resource" to "active-limit",
                    "wait_ms" to 8_000,
                    "lease_ms" to 30_000,
                ))
            }
            session.send("active-overflow", "hello")
            val activeCapacity = session.await("active-overflow")
            assertEquals("invalid_argument", activeCapacity.path("status").asText())
            assertEquals("worker active-request limit reached", activeCapacity.path("error").asText())
            repeat(16) { index ->
                session.send("cancel-wait-$index", "cancel_request", mapOf("target_id" to "wait-$index"))
                assertTrue(session.await("cancel-wait-$index").path("cancelled").asBoolean())
                val cancelled = session.await("wait-$index")
                assertEquals("cancelled", cancelled.path("status").asText())
                assertTrue(cancelled.path("remote_outcome_unknown").isBoolean)
            }
            holder.release()
            holder = null

            session.send("shutdown-cap", "shutdown")
            val shutdown = session.await("shutdown-cap")
            assertEquals(0, shutdown.path("cleanup_attempts").asInt())
            assertEquals(0, shutdown.path("outstanding_cleanup_attempts").asInt())
            session.awaitExit(5_000)
            session = null
            Unit
        } finally {
            session?.kill()
            try { holder?.release() } catch (_: RuntimeException) { }
            holderProvider?.close()
            admin.del("dilock:namespace:$namespace:meta")
            admin.close()
        }
    }
}

private fun pauseRedis(uri: URI, milliseconds: Int) {
    Socket(uri.host, if (uri.port < 0) 6379 else uri.port).use { socket ->
        socket.soTimeout = 1_000
        val command = listOf("CLIENT", "PAUSE", milliseconds.toString(), "ALL")
        val output = socket.getOutputStream()
        output.write("*${command.size}\r\n".toByteArray())
        for (part in command) {
            val bytes = part.toByteArray(Charsets.UTF_8)
            output.write("$${bytes.size}\r\n".toByteArray())
            output.write(bytes)
            output.write("\r\n".toByteArray())
        }
        output.flush()
        val response = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.UTF_8)).readLine()
        assertEquals("+OK", response)
    }
}

internal class WorkerProcess private constructor(private val process: Process, private val configFile: java.nio.file.Path) {
    private val mapper = ObjectMapper()
    private val lines = LinkedBlockingQueue<String>()
    private val received = HashMap<String, JsonNode>()
    private val writer = process.outputWriter(Charsets.UTF_8)
    private val diagnostics = LinkedBlockingQueue<String>()
    private val stdoutPump = thread(isDaemon = true, name = "kotlin-worker-stdout") {
        process.inputReader(Charsets.UTF_8).useLines { stream -> stream.forEach(lines::put) }
    }
    private val stderrPump = thread(isDaemon = true, name = "kotlin-worker-stderr") {
        process.errorReader(Charsets.UTF_8).useLines { stream -> stream.forEach(diagnostics::put) }
    }

    fun send(id: String, op: String, fields: Map<String, Any> = emptyMap()) {
        val request = mapper.createObjectNode().put("id", id).put("op", op)
        fields.forEach { (key, value) ->
            when (value) {
                is String -> request.put(key, value)
                is Int -> request.put(key, value)
                is Long -> request.put(key, value)
                is Boolean -> request.put(key, value)
                else -> error("unsupported test JSON value")
            }
        }
        synchronized(writer) {
            writer.write(mapper.writeValueAsString(request))
            writer.newLine()
            writer.flush()
        }
    }

    fun await(id: String, timeoutMs: Long = 4_000): JsonNode {
        received.remove(id)?.let { return it }
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        while (true) {
            val remaining = deadline - System.nanoTime()
            check(remaining > 0) { timeoutMessage(id) }
            val line = lines.poll(remaining, TimeUnit.NANOSECONDS) ?: error(timeoutMessage(id))
            val message = mapper.readTree(line)
            val responseId = message.path("id").asText()
            if (responseId == id) return message
            received[responseId] = message
        }
    }

    fun hasResponse(id: String): Boolean = received.containsKey(id)

    private fun timeoutMessage(id: String): String {
        if (!process.isAlive) stderrPump.join(100)
        val recentDiagnostics = mutableListOf<String>()
        diagnostics.drainTo(recentDiagnostics)
        val exit = if (process.isAlive) "running" else "exited=${process.exitValue()}"
        return "timed out waiting for worker response id=$id ($exit); stderr=${recentDiagnostics.joinToString(" | ").ifEmpty { "<empty>" }}"
    }

    fun awaitExit(timeoutMs: Long) {
        assertTrue(process.waitFor(timeoutMs, TimeUnit.MILLISECONDS), "worker process must exit within the bound")
        assertEquals(0, process.exitValue())
        stdoutPump.join(500)
        Files.deleteIfExists(configFile)
    }

    fun kill() {
        process.destroyForcibly()
        process.waitFor(2, TimeUnit.SECONDS)
        Files.deleteIfExists(configFile)
    }

    companion object {
        fun start(endpoint: String, namespace: String, epoch: String, backend: String = "redis"): WorkerProcess {
            val config = Files.createTempFile("dilock-kotlin-worker-", ".json")
            val mapper = ObjectMapper()
            mapper.writeValue(config.toFile(), mapOf(
                "backend" to backend,
                "endpoint" to endpoint,
                "namespace" to namespace,
                "epoch" to epoch,
            ))
            val java = java.nio.file.Path.of(System.getProperty("java.home"), "bin", if (System.getProperty("os.name").startsWith("Windows")) "java.exe" else "java").toString()
            val classpath = System.getProperty("dilock.worker.runtimeClasspath")
                ?: error("Gradle did not supply worker runtime classpath")
            val process = ProcessBuilder(java, "-cp", classpath, "org.dilock.kotlin.worker.ConformanceWorkerKt", config.toString())
                .start()
            return WorkerProcess(process, config)
        }
    }
}
