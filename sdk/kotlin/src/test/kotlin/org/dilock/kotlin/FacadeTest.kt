package org.dilock.kotlin

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.dilock.redis.AcquireOptions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class FacadeTest {
    @Test fun defaultPolicyMatchesJavaContract() {
        assertEquals(AcquireOptions.DEFAULT, AcquirePolicy.DEFAULT.toJava())
    }

    @Test fun etcdPolicyPreservesTimingsAndRenewalFlag() {
        assertEquals(
            Triple(5_000L, 30_000L, false),
            EtcdAcquirePolicy.DEFAULT.javaArguments(),
        )
        assertEquals(
            Triple(1_250L, 7_500L, true),
            EtcdAcquirePolicy(Duration.ofMillis(1_250), Duration.ofMillis(7_500), true).javaArguments(),
        )
    }

    @Test fun etcdPolicyRejectsSubMillisecondDurationsBeforeAcquisition() {
        assertThrows(IllegalArgumentException::class.java) {
            EtcdAcquirePolicy(Duration.ofNanos(1), Duration.ofSeconds(1)).javaArguments()
        }
    }

    @Test fun explicitDurationsRejectSubMillisecondPrecision() {
        assertThrows(IllegalArgumentException::class.java) {
            AcquirePolicy(Duration.ofNanos(1), Duration.ofSeconds(1))
        }
    }

    @Test fun interruptibleWorkerStopsOnCoroutineCancellation() = runBlocking {
        val started = CountDownLatch(1)
        val interrupted = CompletableDeferred<Boolean>()
        val job = launch(Dispatchers.Default) {
            try {
                interruptibleIo {
                    started.countDown()
                    try { CountDownLatch(1).await() }
                    catch (error: InterruptedException) { interrupted.complete(true); throw error }
                }
            } catch (_: CancellationException) { }
        }
        check(started.await(2, TimeUnit.SECONDS))
        job.cancel()
        job.join()
        assertEquals(true, interrupted.await())
    }

    @Test fun cancelledCallerClosesLeaseReturnedAfterTheWorkerWasInterrupted() = runBlocking {
        val started = CountDownLatch(1)
        val allowReturn = CountDownLatch(1)
        val lease = TestLease()
        val job = launch(Dispatchers.Default) {
            interruptibleResource({
                started.countDown()
                // Model a backend response that commits just as cancellation interrupts its waiter.
                while (true) {
                    try {
                        allowReturn.await()
                        break
                    } catch (_: InterruptedException) {
                        // The transport may still deliver a successful response after cancellation.
                    }
                }
                lease
            }, AutoCloseable::close)
        }

        check(started.await(2, TimeUnit.SECONDS))
        job.cancel()
        allowReturn.countDown()
        job.join()
        check(lease.closed.await(2, TimeUnit.SECONDS))
        assertEquals(1, lease.closeCount.get())
    }

    @Test fun cancellationBeforeWorkerStartsDoesNotSendAcquisition() = runBlocking {
        val dispatcher = QueuedDispatcher()
        val calls = AtomicInteger()
        val job = launch(Dispatchers.Default) {
            interruptibleResource({ calls.incrementAndGet(); TestLease() }, AutoCloseable::close, dispatcher)
        }

        val worker = dispatcher.take()
        job.cancel()
        worker.run()
        job.join()
        assertEquals(0, calls.get())
    }

    @Test fun cancelledWorkerClearsInterruptFlagBeforeReturning() = runBlocking {
        val dispatcher = QueuedDispatcher()
        val calls = AtomicInteger()
        val job = launch(Dispatchers.Default) {
            interruptibleResource({ calls.incrementAndGet(); TestLease() }, AutoCloseable::close, dispatcher)
        }
        val worker = dispatcher.take()
        val interruptLeaked = java.util.concurrent.atomic.AtomicBoolean(true)
        val finished = CountDownLatch(1)
        val runner = Thread {
            try {
                Thread.currentThread().interrupt()
                job.cancel()
                worker.run()
                interruptLeaked.set(Thread.currentThread().isInterrupted)
            } finally {
                Thread.interrupted()
                finished.countDown()
            }
        }

        runner.start()
        check(finished.await(2, TimeUnit.SECONDS))
        runner.join()
        job.join()
        assertEquals(false, interruptLeaked.get())
        assertEquals(0, calls.get())
    }

    @Test fun explicitReleaseCompletesBoundedCleanupWhenCallerIsCancelled() = runBlocking {
        val started = CountDownLatch(1)
        val finish = CountDownLatch(1)
        val interrupted = AtomicInteger()
        val completed = AtomicInteger()
        val job = launch(Dispatchers.Default) {
            try {
                boundedCleanupIo {
                    started.countDown()
                    try { finish.await() }
                    catch (error: InterruptedException) { interrupted.incrementAndGet(); throw error }
                    completed.incrementAndGet()
                }
            } catch (_: CancellationException) { }
        }
        check(started.await(2, TimeUnit.SECONDS))
        job.cancel()
        // Cancellation is deferred until the release operation's own bounded budget completes.
        check(!job.isCompleted)
        finish.countDown()
        job.join()
        assertEquals(0, interrupted.get())
        assertEquals(1, completed.get())
    }

    @Test fun scopedLeaseClosesWhenBlockThrows() {
        val lease = TestLease()
        val failure = assertThrows(IllegalStateException::class.java) {
            runBlocking {
                scopedAcquire({ lease }, { it.close() }) { throw IllegalStateException("work failed") }
            }
        }

        assertEquals("work failed", failure.message)
        assertEquals(1, lease.closeCount.get())
    }

    @Test fun scopedCleanupFailureIsSuppressedBehindBlockFailure() {
        val workFailure = IllegalStateException("work failed")
        val cleanupFailure = IllegalArgumentException("cleanup observer failed")
        val lease = FailingCloseLease(cleanupFailure)

        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking { scopedAcquire({ lease }, { it.close() }) { throw workFailure } }
        }

        assertEquals(workFailure.javaClass, thrown.javaClass)
        assertEquals(workFailure.message, thrown.message)
        assertEquals(1, thrown.suppressed.size)
        assertEquals(cleanupFailure.javaClass, thrown.suppressed.single().javaClass)
        assertEquals(cleanupFailure.message, thrown.suppressed.single().message)
    }

    @Test fun scopedCleanupFailureIsVisibleWhenBlockSucceeds() {
        val cleanupFailure = IllegalArgumentException("cleanup observer failed")
        val lease = FailingCloseLease(cleanupFailure)

        val thrown = assertThrows(IllegalArgumentException::class.java) {
            runBlocking { scopedAcquire({ lease }, { it.close() }) { "completed" } }
        }

        assertEquals(cleanupFailure.javaClass, thrown.javaClass)
        assertEquals(cleanupFailure.message, thrown.message)
    }

    @Test fun scopedReleaseAcknowledgementFailureIsVisibleWhenBlockSucceeds() {
        val releaseFailure = IllegalStateException("release acknowledgement unknown")
        val lease = TestLease()

        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking { scopedAcquire({ lease }, { throw releaseFailure }) { "completed" } }
        }

        assertEquals(releaseFailure.javaClass, thrown.javaClass)
        assertEquals(releaseFailure.message, thrown.message)
        assertEquals(0, lease.closeCount.get())
    }

    @Test fun backendReleaseOutcomesMapToBackendNeutralResults() {
        assertEquals(
            ReleaseOutcome.RELEASED,
            redisReleaseOutcome(org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED),
        )
        assertEquals(
            ReleaseOutcome.ALREADY_NOT_OWNED,
            redisReleaseOutcome(org.dilock.redis.RedisLease.ReleaseOutcome.ALREADY_NOT_OWNED),
        )
        assertEquals(
            ReleaseOutcome.RELEASED,
            etcdReleaseOutcome(org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED),
        )
        assertEquals(
            ReleaseOutcome.ALREADY_NOT_OWNED,
            etcdReleaseOutcome(org.dilock.etcd.EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED),
        )
    }

    @Test fun scopedReleaseRejectsAlreadyNotOwnedOutcome() {
        val failure = assertThrows(org.dilock.redis.DilockException::class.java) {
            requireReleaseAcknowledged(ReleaseOutcome.ALREADY_NOT_OWNED)
        }

        assertEquals(org.dilock.redis.DilockException.Kind.LEASE_LOST, failure.kind())
        requireReleaseAcknowledged(ReleaseOutcome.RELEASED)
    }

    @Test fun scopedReleaseRejectsAlreadyNotOwnedBehindTheWorkFailure() {
        val workFailure = IllegalStateException("work failed")
        val lease = TestLease()

        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking {
                scopedAcquire({ lease }, { requireReleaseAcknowledged(ReleaseOutcome.ALREADY_NOT_OWNED) }) {
                    throw workFailure
                }
            }
        }

        assertEquals(workFailure, thrown)
        assertEquals(1, thrown.suppressed.size)
        assertEquals(org.dilock.redis.DilockException.Kind.LEASE_LOST,
            (thrown.suppressed.single() as org.dilock.redis.DilockException).kind())
    }

    @Test fun scopedLeaseClosesWhenBlockIsCancelled() = runBlocking {
        val lease = TestLease()
        val acquired = CompletableDeferred<Unit>()
        val job = launch {
            scopedAcquire({ lease.also { acquired.complete(Unit) } }, { it.close() }) { awaitCancellation() }
        }
        acquired.await()
        job.cancelAndJoin()
        assertEquals(1, lease.closeCount.get())
    }

    private class TestLease : AutoCloseable {
        val closeCount = AtomicInteger()
        val closed = CountDownLatch(1)
        override fun close() { closeCount.incrementAndGet(); closed.countDown() }
    }

    private class FailingCloseLease(private val failure: Throwable) : AutoCloseable {
        override fun close() { throw failure }
    }

    private class QueuedDispatcher : CoroutineDispatcher() {
        private val tasks = LinkedBlockingQueue<Runnable>()
        override fun dispatch(context: kotlin.coroutines.CoroutineContext, block: Runnable) {
            tasks.add(block)
        }
        fun take(): Runnable = checkNotNull(tasks.poll(2, TimeUnit.SECONDS)) { "worker was not queued" }
    }
}
