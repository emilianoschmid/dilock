package org.dilock.kotlin.worker

import com.google.protobuf.ByteString
import io.etcd.jetcd.api.DeleteRangeRequest
import io.etcd.jetcd.api.KVGrpc
import io.etcd.jetcd.api.PutRequest
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.dilock.kotlin.AcquirePolicy
import org.dilock.kotlin.DilockEtcd
import org.dilock.kotlin.EtcdProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class EtcdWorkerProcessIntegrationTest {
    @Test
    fun processRemainsResponsiveToCancellationAndSuccessorAcquiresAfterCancellation() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_ETCD_ENDPOINT to run process integration")
        val namespace = "kotlin-worker-${UUID.randomUUID()}"
        val epoch = "kotlin-worker-${UUID.randomUUID()}"
        val resource = "cancel-${UUID.randomUUID()}"
        provisionNamespace(endpoint!!, namespace, epoch)
        var ownerProvider: EtcdProvider? = null
        var owner: org.dilock.kotlin.EtcdLease? = null
        var session: WorkerProcess? = null
        try {
            ownerProvider = DilockEtcd.connect(endpoint, namespace, epoch)
            owner = ownerProvider.lock(resource).acquire(
                AcquirePolicy(Duration.ofSeconds(4), Duration.ofSeconds(8))
            )
            session = WorkerProcess.start(endpoint, namespace, epoch, "etcd")
            session.send("hello-0", "hello")
            // A fresh worker also creates its first etcd gRPC channel before
            // reading this command; keep the startup handshake above the RPC budget.
            assertEquals("ok", session.await("hello-0", 8_000).path("status").asText())

            session.send("wait-1", "acquire", mapOf(
                "resource" to resource,
                "wait_ms" to 8_000,
                "lease_ms" to 5_000,
            ))
            delay(300)
            session.send("hello-1", "hello")
            assertEquals("ok", session.await("hello-1", 2_000).path("status").asText())
            assertFalse(session.hasResponse("wait-1"), "competing etcd Lock must remain pending while the other provider holds the resource")

            session.send("cancel-1", "cancel_request", mapOf("target_id" to "wait-1"))
            val cancelReply = session.await("cancel-1", 3_000)
            assertEquals("ok", cancelReply.path("status").asText())
            assertTrue(cancelReply.path("cancelled").asBoolean())
            val cancelled = session.await("wait-1", 5_000)
            assertEquals("cancelled", cancelled.path("status").asText())
            assertTrue(cancelled.path("remote_outcome_unknown").isBoolean, "cancellation must preserve whether remote acquisition outcome is uncertain")

            owner.release()
            owner = null
            session.send("successor-1", "acquire", mapOf(
                "resource" to resource,
                "wait_ms" to 3_000,
                "lease_ms" to 5_000,
            ))
            val successor = session.await("successor-1", 6_000)
            assertEquals("acquired", successor.path("status").asText())
            session.send("successor-release", "release", mapOf("lease_id" to successor.path("lease_id").asText()))
            assertEquals("released", session.await("successor-release").path("status").asText())

            session.send("shutdown-1", "shutdown")
            val shutdown = session.await("shutdown-1", 4_000)
            assertEquals("ok", shutdown.path("status").asText())
            assertEquals(0, shutdown.path("outstanding_cleanup_attempts").asInt())
            session.awaitExit(5_000)
            session = null
        } finally {
            session?.kill()
            try { owner?.release() } catch (_: RuntimeException) { }
            ownerProvider?.close()
            deleteNamespace(endpoint, namespace)
        }
    }

    @Test
    fun processReportsUnsupportedRenewalAndHandlesExpiryLifecycleAndBoundedShutdown() = runBlocking {
        val endpoint = System.getenv("DILOCK_TEST_ETCD_ENDPOINT")
        assumeTrue(!endpoint.isNullOrBlank(), "set DILOCK_TEST_ETCD_ENDPOINT to run process integration")
        val namespace = "kotlin-worker-${UUID.randomUUID()}"
        val epoch = "kotlin-worker-${UUID.randomUUID()}"
        val resource = "expiry-${UUID.randomUUID()}"
        provisionNamespace(endpoint!!, namespace, epoch)
        var session: WorkerProcess? = null
        try {
            session = WorkerProcess.start(endpoint, namespace, epoch, "etcd")
            session.send("hello-0", "hello")
            assertEquals("ok", session.await("hello-0", 10_000).path("status").asText())
            session.send("first", "acquire", mapOf("resource" to resource, "wait_ms" to 4_000, "lease_ms" to 3_000))
            val first = session.await("first", 8_000)
            assertEquals("acquired", first.path("status").asText())
            val firstLeaseId = first.path("lease_id").asText()
            assertTrue(first.path("fence").path("value").asText().toLong() > 0)

            session.send("inspect-first", "inspect", mapOf("lease_id" to firstLeaseId))
            assertEquals("held", session.await("inspect-first").path("state").asText())
            session.send("renew-first", "renew", mapOf("lease_id" to firstLeaseId, "lease_ms" to 5_000))
            assertEquals("unsupported_capability", session.await("renew-first").path("status").asText())
            session.send("try-unsupported", "try_acquire", mapOf("resource" to "no-try", "wait_ms" to 0, "lease_ms" to 3_000))
            assertEquals("unsupported_capability", session.await("try-unsupported").path("status").asText())

            session.send("second", "acquire", mapOf("resource" to resource, "wait_ms" to 8_000, "lease_ms" to 10_000))
            delay(300)
            session.send("reader-during-second", "hello")
            assertEquals("ok", session.await("reader-during-second", 2_000).path("status").asText())
            val second = session.await("second", 9_000)
            assertEquals("acquired", second.path("status").asText(), "etcd server-side TTL expiry must make the resource available to its queued successor")
            assertTrue(second.path("fence").path("value").asText().toLong() > first.path("fence").path("value").asText().toLong())

            session.send("inspect-expired-first", "inspect", mapOf("lease_id" to firstLeaseId))
            assertEquals("lost", session.await("inspect-expired-first").path("state").asText())
            session.send("release-expired-first", "release", mapOf("lease_id" to firstLeaseId))
            val staleRelease = session.await("release-expired-first")
            assertTrue(staleRelease.path("status").asText() in setOf("released", "already_not_owned"), "expired lease cleanup must return a confirmed release classification")
            session.send("repeat-stale-release", "release", mapOf("lease_id" to firstLeaseId))
            assertEquals(staleRelease.path("status").asText(), session.await("repeat-stale-release").path("status").asText())
            session.send("probe-successor-still-held", "acquire", mapOf("resource" to resource, "wait_ms" to 300, "lease_ms" to 5_000))
            assertEquals("timeout", session.await("probe-successor-still-held", 5_000).path("status").asText(), "releasing the expired predecessor must not unlock its successor")
            session.send("release-second", "release", mapOf("lease_id" to second.path("lease_id").asText()))
            assertEquals("released", session.await("release-second").path("status").asText())
            session.send("repeat-release-second", "release", mapOf("lease_id" to second.path("lease_id").asText()))
            assertEquals("released", session.await("repeat-release-second").path("status").asText())

            session.send("shutdown-lease", "acquire", mapOf("resource" to "shutdown-${UUID.randomUUID()}", "wait_ms" to 1_000, "lease_ms" to 5_000))
            val held = session.await("shutdown-lease", 6_000)
            assertEquals("acquired", held.path("status").asText())
            session.send("shutdown", "shutdown")
            val shutdown = session.await("shutdown", 5_000)
            assertEquals("ok", shutdown.path("status").asText())
            assertEquals(1, shutdown.path("cleanup_attempts").asInt())
            assertEquals(0, shutdown.path("outstanding_cleanup_attempts").asInt())
            session.awaitExit(5_000)
            session = null
        } finally {
            session?.kill()
            deleteNamespace(endpoint, namespace)
        }
    }
}

private fun provisionNamespace(endpoint: String, namespace: String, epoch: String) {
    val channel = channel(endpoint)
    try {
        KVGrpc.newFutureStub(channel).withDeadlineAfter(15, TimeUnit.SECONDS).put(PutRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("/dilock/namespaces/$namespace/meta"))
            .setValue(ByteString.copyFromUtf8("""{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"$epoch","fixed_lease_ms":null}"""))
            .build()).get(16, TimeUnit.SECONDS)
    } finally {
        channel.shutdownNow()
    }
}

private fun deleteNamespace(endpoint: String, namespace: String) {
    val channel = channel(endpoint)
    try {
        KVGrpc.newFutureStub(channel).withDeadlineAfter(15, TimeUnit.SECONDS).deleteRange(DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("/dilock/namespaces/$namespace/meta"))
            .build()).get(16, TimeUnit.SECONDS)
    } finally {
        channel.shutdownNow()
    }
}

private fun channel(endpoint: String) = ManagedChannelBuilder.forTarget(
    endpoint.removePrefix("http://").removePrefix("https://")
).usePlaintext().build()
