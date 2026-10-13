package org.dilock.scala213

import java.time.Duration

import munit.FunSuite
import org.dilock.redis.DilockException

final class FacadeSuite extends FunSuite {
  test("common acquisition budgets are preserved for the Java engine") {
    val options = AcquirePolicy(Duration.ofMillis(1250), Duration.ofMillis(7500)).toJava
    assertEquals(options.waitMillis(), 1250L)
    assertEquals(options.leaseMillis(), 7500L)
    assertEquals(AcquirePolicy.default.toJava, org.dilock.redis.AcquireOptions.DEFAULT)
  }

  test("external-package consumer executes the shared Redis and etcd policy contracts") {
    _root_.consumers.CrossVersionConsumer.verifyPolicies()
  }

  test("etcd renewal policy preserves all arguments") {
    assertEquals(
      EtcdAcquirePolicy(Duration.ofMillis(1250), Duration.ofMillis(7500), autoRenew = true).javaArguments,
      (1250L, 7500L, true))
  }

  test("policies reject sub-millisecond or nonpositive durations") {
    intercept[IllegalArgumentException] {
      AcquirePolicy(Duration.ofNanos(1), Duration.ofSeconds(1)).toJava
    }
    intercept[IllegalArgumentException] {
      EtcdAcquirePolicy(Duration.ofSeconds(1), Duration.ZERO).javaArguments
    }
  }

  test("scope helper accepts acknowledged Redis and etcd releases") {
    assertEquals(
      Scope.withRelease("redis")(_ => Right(org.dilock.redis.RedisLease.ReleaseOutcome.RELEASED))(_ => 7),
      Right(7))
    assertEquals(
      Scope.withRelease("etcd")(_ => Right(org.dilock.etcd.EtcdLease.ReleaseOutcome.RELEASED))(_ => 11),
      Right(11))
  }

  test("non-released backend outcomes become LEASE_LOST") {
    val redis = Scope.withRelease("redis")(_ =>
      Right(org.dilock.redis.RedisLease.ReleaseOutcome.ALREADY_NOT_OWNED))(_ => "done")
    val etcd = Scope.withRelease("etcd")(_ =>
      Right(org.dilock.etcd.EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED))(_ => "done")
    assertEquals(redis.left.toOption.map(_.kind()), Some(DilockException.Kind.LEASE_LOST))
    assertEquals(etcd.left.toOption.map(_.kind()), Some(DilockException.Kind.LEASE_LOST))
  }

  test("release failure is returned after success and suppressed after work failure") {
    val releaseFailure = new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "unknown")
    assertEquals(Scope.withRelease("lease")(_ => Left(releaseFailure))(_ => "done"), Left(releaseFailure))

    val workFailure = new IllegalStateException("work failed")
    val thrown = intercept[IllegalStateException] {
      Scope.withRelease("lease")(_ => Left(releaseFailure))(_ => throw workFailure)
    }
    assertEquals(thrown, workFailure)
    assertEquals(thrown.getSuppressed.toList, List(releaseFailure))
  }
}
