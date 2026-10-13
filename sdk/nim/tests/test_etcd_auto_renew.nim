import std/[os, unittest]
import dilock

let endpoint = getEnv("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379")
let namespace = getEnv("DILOCK_TEST_NAMESPACE", "nim-etcd-renewal")
let epoch = getEnv("DILOCK_TEST_EPOCH", "nim-local-etcd-renewal-epoch")

suite "Dilock Nim etcd automatic keepalive":
  test "auto-renew keeps a successor out beyond the initial server TTL":
    let ownerProvider = connectEtcd(endpoint, namespace, epoch, autoRenew = true)
    defer: ownerProvider.destroy()
    let successorProvider = connectEtcd(endpoint, namespace, epoch)
    defer: successorProvider.destroy()
    let ownerLock = ownerProvider.lock("renewal/resource")
    defer: ownerLock.destroy()
    let successorLock = successorProvider.lock("renewal/resource")
    defer: successorLock.destroy()

    let owner = ownerLock.acquire(1000, 3000)
    defer: owner.destroy()
    check owner.isValid()

    # etcd grants leases in whole seconds. Wait beyond the requested TTL so
    # this distinguishes provider auto-renew from ordinary lease expiry.
    sleep(4000)
    check owner.isValid()

    var timedOut = false
    var premature: EtcdLease
    try:
      premature = successorLock.acquire(750, 3000)
    except DilockError as error:
      check error.status == dsWaitTimeout
      timedOut = error.status == dsWaitTimeout

    if not premature.isNil:
      discard premature.release()
      premature.destroy()
    check timedOut

    check owner.release() == released
    let successor = successorLock.acquire(1000, 3000)
    defer: successor.destroy()
    check successor.isValid()
    check successor.release() == released
