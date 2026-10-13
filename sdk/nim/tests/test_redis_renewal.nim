import std/[options, os, strutils, unittest]
import dilock

let host = getEnv("DILOCK_REDIS_HOST", "127.0.0.1")
let port = parseInt(getEnv("DILOCK_REDIS_PORT", "6379")).int32
let namespace = getEnv("DILOCK_TEST_NAMESPACE", "nim-renewal")
let epoch = getEnv("DILOCK_TEST_EPOCH", "nim-local-renewal-epoch")

suite "Dilock Nim Redis live renewal":
  test "renewal keeps a successor out beyond the original TTL":
    let ownerProvider = connectRedis(host, namespace, epoch, port)
    defer: ownerProvider.destroy()
    let successorProvider = connectRedis(host, namespace, epoch, port)
    defer: successorProvider.destroy()
    let ownerLock = ownerProvider.lock("renewal/resource")
    defer: ownerLock.destroy()
    let successorLock = successorProvider.lock("renewal/resource")
    defer: successorLock.destroy()

    let owner = ownerLock.acquire(1000, 3000)
    defer: owner.destroy()
    check owner.isValid()
    check owner.renew(6000)

    sleep(3500)
    check owner.isValid()
    check successorLock.tryAcquire(3000).isNone

    check owner.release() == released
    let successor = successorLock.tryAcquire(3000)
    check successor.isSome
    defer: successor.get().destroy()
    check successor.get().isValid()
    check successor.get().release() == released
