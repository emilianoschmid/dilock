import std/[options, os]
import dilock

proc main() =
  let connectionString = getEnv("DILOCK_POSTGRES_DSN")
  doAssert connectionString.len > 0,
    "set DILOCK_POSTGRES_DSN to a disposable PostgreSQL authority"
  let namespace = getEnv("DILOCK_TEST_NAMESPACE", "nim-postgres-live")
  let epoch = getEnv("DILOCK_TEST_EPOCH", "nim-postgres-live-epoch")

  provisionPostgresNamespace(connectionString, namespace, epoch)

  let firstProvider = connectPostgres(connectionString, namespace, epoch)
  defer: firstProvider.destroy()
  let secondProvider = connectPostgres(connectionString, namespace, epoch)
  defer: secondProvider.destroy()
  let firstLock = firstProvider.lock("nim/postgres-live-lifecycle")
  defer: firstLock.destroy()
  let secondLock = secondProvider.lock("nim/postgres-live-lifecycle")
  defer: secondLock.destroy()

  let firstOption = firstLock.tryAcquire()
  doAssert firstOption.isSome, "first PostgreSQL acquisition failed"
  let first = firstOption.get
  defer: first.destroy()
  doAssert first.isValid(), "first PostgreSQL lease was not valid"
  doAssert secondLock.tryAcquire().isNone,
    "contending Nim provider acquired the held PostgreSQL key"
  doAssert first.renew(), "PostgreSQL lease liveness check failed"
  doAssert first.release() == released,
    "first PostgreSQL release was not confirmed"

  let successorOption = secondLock.tryAcquire()
  doAssert successorOption.isSome,
    "PostgreSQL successor did not acquire after release"
  let successor = successorOption.get
  defer: successor.destroy()
  doAssert successor.isValid(), "successor PostgreSQL lease was not valid"
  doAssert successor.release() == released,
    "PostgreSQL successor release was not confirmed"
  echo "PASS Nim PostgreSQL: provision, contention, liveness, release, and handoff"

main()
