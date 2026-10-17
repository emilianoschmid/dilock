import std/os
import dilock

proc main() =
  let endpoint = getEnv("DILOCK_CONSUL_ENDPOINT", "http://127.0.0.1:8500")
  let provider = connectConsul(endpoint, "billing", "local-consul-epoch",
                               aclToken = getEnv("DILOCK_CONSUL_TOKEN"))
  defer: provider.destroy()
  let lock = provider.lock("invoice/42/reconcile")
  defer: lock.destroy()

  withLease lease, lock.acquireDefault():
    doAssert lease.isValid()
    let fence = lease.fence()
    echo "Consul fence: ", fence.epoch, ":", fence.lockIndex
    let outcome = lease.release()
    if outcome != ReleaseOutcome.released:
      raise newException(IOError, "Consul release was not confirmed as released")

when isMainModule:
  main()
