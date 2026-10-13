package org.dilock.scala3

import java.time.Duration
import java.util.concurrent.{AbstractExecutorService, ConcurrentHashMap, Future, TimeUnit}
import munit.FunSuite
import org.dilock.redis.AcquireOptions
import org.dilock.redis.DilockException
import org.dilock.redis.RedisLease.ReleaseOutcome
import com.fasterxml.jackson.databind.ObjectMapper

final class FacadeSuite extends FunSuite:
  test("default acquisition policy matches the shared Java contract") {
    assertEquals(AcquirePolicy.default.toJava, AcquireOptions.DEFAULT)
  }

  test("whole-millisecond policies preserve both complete budgets") {
    val javaPolicy = AcquirePolicy(
      waitTimeout = Duration.ofMillis(1_250),
      lease = Duration.ofMillis(7_500)
    ).toJava

    assertEquals(javaPolicy.waitMillis(), 1_250L)
    assertEquals(javaPolicy.leaseMillis(), 7_500L)
  }

  test("etcd policy preserves auto-renew when forwarding the Java acquisition arguments") {
    val policy = EtcdAcquirePolicy(
      waitTimeout = Duration.ofMillis(1_250),
      lease = Duration.ofMillis(7_500),
      autoRenew = true
    )

    assertEquals(policy.javaArguments, (1_250L, 7_500L, true))
  }

  test("provider settings and policies reject sub-millisecond durations before network calls") {
    intercept[IllegalArgumentException] {
      AcquirePolicy(waitTimeout = Duration.ofNanos(1), lease = Duration.ofSeconds(1)).toJava
    }
  }

  test("policy conversion rejects a zero lease before any provider exists") {
    intercept[IllegalArgumentException] {
      AcquirePolicy(waitTimeout = Duration.ZERO, lease = Duration.ZERO).toJava
    }
  }

  test("scope cleanup failure is suppressed behind the original work failure") {
    val workFailure = new IllegalStateException("work failed")
    val cleanupFailure = new IllegalArgumentException("cleanup observer failed")
    val lease = new AutoCloseable:
      override def close(): Unit = throw cleanupFailure

    val thrown = intercept[IllegalStateException] {
      withLeaseScope(lease)(_ => throw workFailure)
    }

    assertEquals(thrown, workFailure)
    assertEquals(thrown.getSuppressed.toList, List(cleanupFailure))
  }

  test("scope cleanup failure propagates when the work succeeds") {
    val cleanupFailure = new IllegalArgumentException("cleanup observer failed")
    val lease = new AutoCloseable:
      override def close(): Unit = throw cleanupFailure

    val thrown = intercept[IllegalArgumentException] {
      withLeaseScope(lease)(_ => "completed")
    }

    assertEquals(thrown, cleanupFailure)
  }

  test("a typed DilockException from user work is not converted into Left") {
    val workFailure = new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "user work failed")
    val lease = new AutoCloseable:
      override def close(): Unit = ()

    val thrown = intercept[DilockException] {
      withLeaseResult(lease)(_ => throw workFailure)
    }

    assertEquals(thrown, workFailure)
    assertEquals(thrown.getSuppressed.toList, Nil)
  }

  test("a typed cleanup failure is returned as Left when user work succeeds") {
    val cleanupFailure = new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "cleanup was not acknowledged")
    val lease = new AutoCloseable:
      override def close(): Unit = throw cleanupFailure

    assertEquals(withLeaseResult(lease)(_ => "completed"), Left(cleanupFailure))
  }

  test("a release outcome with unknown acknowledgement is returned as Left after successful work") {
    val releaseFailure = new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "release was not acknowledged")
    assertEquals(
      withLeaseRelease("lease")(_ => Left(releaseFailure))(_ => "completed"),
      Left(releaseFailure)
    )
  }

  test("release acknowledgement failure is suppressed behind user work failure") {
    val workFailure = new IllegalStateException("work failed")
    val releaseFailure = new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "release was not acknowledged")
    val thrown = intercept[IllegalStateException] {
      withLeaseRelease("lease")(_ => Left(releaseFailure))(_ => throw workFailure)
    }

    assertEquals(thrown, workFailure)
    assertEquals(thrown.getSuppressed.toList, List(releaseFailure))
  }

  test("a non-released enum outcome is reported as LEASE_LOST") {
    val result = withLeaseRelease("lease")(_ => Right(ReleaseOutcome.ALREADY_NOT_OWNED))(_ => "completed")

    assert(result.isLeft)
    assertEquals(result.left.toOption.map(_.kind()), Some(DilockException.Kind.LEASE_LOST))
    assert(releaseWasAcknowledged(ReleaseOutcome.RELEASED))
    assert(releaseWasAcknowledged(org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED))
    assert(!releaseWasAcknowledged(org.dilock.etcd.EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED))
  }

  test("a foreign enum named RELEASED is not a Dilock release acknowledgement") {
    assert(!releaseWasAcknowledged(ForeignReleaseOutcome.RELEASED))
  }

  test("a non-released enum outcome is suppressed behind user work failure") {
    val workFailure = new IllegalStateException("work failed")
    val thrown = intercept[IllegalStateException] {
      withLeaseRelease("lease")(_ => Right(ReleaseOutcome.ALREADY_NOT_OWNED))(_ => throw workFailure)
    }

    assertEquals(thrown, workFailure)
    assertEquals(thrown.getSuppressed.toList.map(_.asInstanceOf[DilockException].kind()),
      List(DilockException.Kind.LEASE_LOST))
  }

  test("worker cancellation preserves unknown remote acquisition outcomes") {
    assert(!ConformanceWorker.remoteOutcomeUnknown(new InterruptedException("acquisition interrupted before send"), true))
    assert(ConformanceWorker.remoteOutcomeUnknown(new InterruptedException("acquisition interrupted after send; remote outcome may be unknown"), true))
    assert(!ConformanceWorker.remoteOutcomeUnknown(new InterruptedException("acquisition interrupted after send"), false))
  }

  test("worker decodes resource names as strict UTF-8 without changing their bytes") {
    val line = "{\"resource\":\"factura/niño/東京\"}\r".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    assertEquals(ConformanceWorker.decodeUtf8Line(line), "{\"resource\":\"factura/niño/東京\"}")
    intercept[java.nio.charset.CharacterCodingException] {
      ConformanceWorker.decodeUtf8Line(Array(0xc3.toByte, 0x28.toByte))
    }
  }

  test("worker accepts only JSON integer duration fields in the signed 64-bit range") {
    val mapper = new ObjectMapper()
    val integer = mapper.readTree("{\"wait_ms\":0,\"lease_ms\":30000}")
    assertEquals(ConformanceWorker.nonnegativeLong(integer, "wait_ms", 5000), 0L)
    assertEquals(ConformanceWorker.positiveLong(integer, "lease_ms", 5000), 30000L)

    List("30000.5", "\"30000\"", "true", "9223372036854775808", "0", "-1").foreach { invalid =>
      val request = mapper.readTree(s"{\"lease_ms\":$invalid}")
      intercept[IllegalArgumentException] {
        ConformanceWorker.positiveLong(request, "lease_ms", 5000)
      }
    }

    List("1.25", "\"10\"", "false", "9223372036854775808", "-1").foreach { invalid =>
      val request = mapper.readTree(s"{\"wait_ms\":$invalid}")
      intercept[IllegalArgumentException] {
        ConformanceWorker.nonnegativeLong(request, "wait_ms", 5000)
      }
    }
  }

  test("worker tracks requests before execution and clears cancellation before start") {
    val executor = new QueuedExecutor
    val pending = new ConcurrentHashMap[String, Future[?]]()
    val active = ConcurrentHashMap.newKeySet[String]()
    active.add("queued")

    ConformanceWorker.submitTracked("queued", executor, pending, active) { () }

    assert(pending.containsKey("queued"))
    assert(pending.get("queued").cancel(true))
    assert(!pending.containsKey("queued"))
    assert(!active.contains("queued"))
  }

  private final class QueuedExecutor extends AbstractExecutorService:
    private var queued: Runnable = null
    override def execute(command: Runnable): Unit = queued = command
    override def shutdown(): Unit = ()
    override def shutdownNow(): java.util.List[Runnable] = java.util.Collections.emptyList[Runnable]()
    override def isShutdown: Boolean = false
    override def isTerminated: Boolean = false
    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = true
