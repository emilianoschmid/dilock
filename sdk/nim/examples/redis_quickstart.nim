import std/os
import dilock

proc main() =
  let host = getEnv("DILOCK_REDIS_HOST", "127.0.0.1")
  let provider = connectRedis(host, "billing", "local-redis-epoch")
  defer: provider.destroy()
  let lock = provider.lock("invoice/42/reconcile")
  defer: lock.destroy()

  withLease lease, lock.acquireDefault():
    doAssert lease.isValid()
    let released = lease.release()
    if released != ReleaseOutcome.released:
      raise newException(IOError, "Redis release was not confirmed as released")

when isMainModule:
  main()
