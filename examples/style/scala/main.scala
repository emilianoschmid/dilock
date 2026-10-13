// Scala 3 local-only example; the production facade should reuse Java's engine.
// No TTL, remote I/O, renewal or fencing. No Cats/ZIO dependency is required.
import scala.collection.mutable
import scala.util.Using

enum ReleaseOutcome:
  case Released, AlreadyNotOwned

final class MemoryStore:
  private val owners = mutable.Map.empty[String, Object]
  def claim(resource: String, owner: Object): Boolean = synchronized {
    if owners.contains(resource) then false
    else { owners(resource) = owner; true }
  }
  def release(resource: String, owner: Object): ReleaseOutcome = synchronized {
    if owners.get(resource).exists(_ eq owner) then
      owners.remove(resource)
      ReleaseOutcome.Released
    else ReleaseOutcome.AlreadyNotOwned
  }
  def expireForTest(resource: String): Unit = synchronized { owners.remove(resource); () }

final class Lease(store: MemoryStore, resource: String, owner: Object) extends AutoCloseable:
  private var closed = false
  def release(): ReleaseOutcome = synchronized {
    if closed then ReleaseOutcome.AlreadyNotOwned
    else { closed = true; store.release(resource, owner) }
  }
  override def close(): Unit = { release(); () }

final class ResourceLock(store: MemoryStore, resource: String):
  require(resource.nonEmpty, "empty resource")
  def tryAcquire(): Option[Lease] =
    val owner = new Object()
    Option.when(store.claim(resource, owner))(new Lease(store, resource, owner))

final class Provider(store: MemoryStore):
  def lock(resource: String): ResourceLock = new ResourceLock(store, resource)

@main def runDilockStyle(): Unit =
  val store = new MemoryStore
  val resource = new Provider(store).lock("example/resource")
  val first = resource.tryAcquire().getOrElse(sys.error("first"))
  assert(resource.tryAcquire().isEmpty)
  store.expireForTest("example/resource")
  val second = resource.tryAcquire().getOrElse(sys.error("second"))
  assert(first.release() == ReleaseOutcome.AlreadyNotOwned)
  assert(resource.tryAcquire().isEmpty)
  assert(second.release() == ReleaseOutcome.Released)
  assert(second.release() == ReleaseOutcome.AlreadyNotOwned)
  class WorkFailure extends RuntimeException
  try
    Using.resource(resource.tryAcquire().getOrElse(sys.error("scope"))) { _ => throw new WorkFailure }
  catch case _: WorkFailure => ()
  Using.resource(resource.tryAcquire().getOrElse(sys.error("cleanup failed"))) { _ => () }
  println("PASS scala: busy, stale owner, idempotence, Using.resource")
