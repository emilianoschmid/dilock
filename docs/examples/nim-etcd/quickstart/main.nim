import std/os
import dilock

proc main() =
  let endpoint = getEnv("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379")
  let provider = connectEtcd(endpoint, "billing", "local-etcd-epoch")
  defer: provider.destroy()
  let lock = provider.lock("invoice/42/reconcile")
  defer: lock.destroy()

  withLease lease, lock.acquireDefault():
    doAssert lease.isValid()
    let fence = lease.fence()
    echo "etcd fence: ", fence.epoch, ":", fence.createRevision
    let released = lease.release()
    if released != ReleaseOutcome.released:
      raise newException(IOError, "etcd release was not confirmed as released")

when isMainModule:
  main()
