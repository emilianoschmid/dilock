## Opt-in integration regression for authenticated Redis and database selection.
## Run only against a disposable Redis with namespace metadata provisioned in
## the configured database. DILOCK_TEST_REDIS_PASSWORD is required.
import std/[options, os, strutils, unittest]
import dilock

let password = getEnv("DILOCK_TEST_REDIS_PASSWORD")
if password.len == 0:
  quit("Set DILOCK_TEST_REDIS_PASSWORD for the disposable authenticated Redis test", 2)

let host = getEnv("DILOCK_REDIS_HOST", "127.0.0.1")
let port = parseInt(getEnv("DILOCK_REDIS_PORT", "6379")).int32
let username = getEnv("DILOCK_TEST_REDIS_USERNAME")
let database = parseInt(getEnv("DILOCK_TEST_REDIS_DATABASE", "2")).int32
let namespace = getEnv("DILOCK_TEST_NAMESPACE", "nim-authenticated")
let epoch = getEnv("DILOCK_TEST_EPOCH", "nim-authenticated-epoch")

suite "Dilock Nim Redis authentication and database selection":
  test "configured credentials and database reach the intended namespace":
    let provider = connectRedis(
      host,
      namespace,
      epoch,
      port = port,
      username = username,
      password = password,
      database = database,
    )
    defer: provider.destroy()

    let lock = provider.lock("connection/configuration")
    defer: lock.destroy()

    let owner = lock.acquire(2000, 10000)
    defer: owner.destroy()
    check owner.isValid()
    check owner.release() == released

    let successor = lock.tryAcquire(10000)
    check successor.isSome
    defer: successor.get().destroy()
    check successor.get().isValid()
    check successor.get().release() == released
