import std/[os, options]
import dilock

proc main() =
  let endpoint = getEnv("DILOCK_CONSUL_ENDPOINT")
  let namespace = getEnv("DILOCK_CONSUL_NAMESPACE")
  let epoch = getEnv("DILOCK_CONSUL_EPOCH")
  doAssert endpoint.len > 0 and namespace.len > 0 and epoch.len > 0,
    "set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE, and DILOCK_CONSUL_EPOCH"

  let firstProvider = connectConsul(endpoint, namespace, epoch, getEnv("DILOCK_CONSUL_TOKEN"))
  defer: firstProvider.destroy()
  let secondProvider = connectConsul(endpoint, namespace, epoch, getEnv("DILOCK_CONSUL_TOKEN"))
  defer: secondProvider.destroy()
  let firstLock = firstProvider.lock("nim/live-consul-lifecycle")
  defer: firstLock.destroy()
  let secondLock = secondProvider.lock("nim/live-consul-lifecycle")
  defer: secondLock.destroy()

  let firstOption = firstLock.tryAcquire(10000)
  doAssert firstOption.isSome, "first Consul acquisition failed"
  let first = firstOption.get
  defer: first.destroy()
  let firstFence = first.fence()
  doAssert firstFence.epoch == epoch and firstFence.lockIndex > 0
  doAssert secondLock.tryAcquire(10000).isNone, "contending Nim provider acquired the held key"
  doAssert first.renew(), "Consul session renewal was not confirmed"
  doAssert first.release() == ReleaseOutcome.released, "Consul release was not confirmed"

  let successorOption = secondLock.tryAcquire(10000)
  doAssert successorOption.isSome, "Consul successor did not acquire after release"
  let successor = successorOption.get
  defer: successor.destroy()
  let successorFence = successor.fence()
  doAssert successorFence.epoch == epoch and successorFence.lockIndex > firstFence.lockIndex
  doAssert successor.release() == ReleaseOutcome.released, "Consul successor release was not confirmed"
  echo "PASS Nim Consul: contention, renewal, release, handoff, and LockIndex fencing"

main()
