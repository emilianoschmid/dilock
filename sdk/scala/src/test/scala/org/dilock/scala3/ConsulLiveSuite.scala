package org.dilock.scala3

import java.time.Duration
import java.util.UUID
import munit.FunSuite

final class ConsulLiveSuite extends FunSuite:
  test("independent Scala 3 providers contend and advance the retained LockIndex fence") {
    val endpoint = sys.env.get("DILOCK_TEST_CONSUL_ENDPOINT").filter(_.nonEmpty)
    val namespace = sys.env.get("DILOCK_TEST_NAMESPACE").filter(_.nonEmpty)
    val epoch = sys.env.get("DILOCK_TEST_EPOCH").filter(_.nonEmpty)
    assume(endpoint.nonEmpty, "set DILOCK_TEST_CONSUL_ENDPOINT to opt in to live Consul")
    assume(namespace.nonEmpty, "set DILOCK_TEST_NAMESPACE to a provisioned namespace")
    assume(epoch.nonEmpty, "set DILOCK_TEST_EPOCH to the provisioned coordination epoch")

    val providerA = DilockConsul.connect(endpoint.get, namespace.get, epoch.get)
    val providerB = DilockConsul.connect(endpoint.get, namespace.get, epoch.get)
    var first: Option[ConsulLease] = None
    var next: Option[ConsulLease] = None
    try
      val resource = s"scala3-consul/${UUID.randomUUID()}"
      val policy = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(10))
      val original = providerA.lock(resource).acquire(policy).fold(throw _, identity)
      first = Some(original)
      val firstFence = original.fence
      assertEquals(firstFence.epoch(), epoch.get)
      assert(firstFence.lockIndex() > 0)
      assertEquals(providerB.lock(resource).tryAcquire(), Right(None))
      assertEquals(original.release(), Right(org.dilock.consul.ConsulLease.ReleaseOutcome.RELEASED))
      first = None

      val successor = providerB.lock(resource).acquire(policy).fold(throw _, identity)
      next = Some(successor)
      val nextFence = successor.fence
      assertEquals(nextFence.epoch(), epoch.get)
      assert(nextFence.lockIndex() > firstFence.lockIndex())
      assertEquals(successor.release(), Right(org.dilock.consul.ConsulLease.ReleaseOutcome.RELEASED))
      next = None
    finally
      next.foreach(_.close())
      first.foreach(_.close())
      providerB.close()
      providerA.close()
  }
