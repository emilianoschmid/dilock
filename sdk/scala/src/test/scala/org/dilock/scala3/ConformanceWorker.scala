package org.dilock.scala3

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.dilock.redis.DilockException

import java.nio.charset.StandardCharsets
import java.nio.charset.CodingErrorAction
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{Callable, ConcurrentHashMap, ExecutorService, Executors, Future, FutureTask}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

/** Test-only adapter for the shared JSONL worker protocol. */
object ConformanceWorker:
  private val json = new ObjectMapper()
  private val maxLineBytes = 64 * 1024

  private case class Config(backend: String, endpoint: String, namespace: String, epoch: String)

  def main(args: Array[String]): Unit =
    require(args.length == 1, "usage: ConformanceWorker <config.json>")
    val config = sys.env.get("DILOCK_SCALA_WORKER_CONFIG_JSON") match
      case Some(value) => parseConfig(json.readTree(value))
      case None => parseConfig(json.readTree(Files.readString(Path.of(args(0)))))
    config.backend match
      case "redis" => runRedis(config)
      case "etcd" => runEtcd(config)
      case other => throw new IllegalArgumentException(s"unsupported backend: $other")

  private def parseConfig(node: JsonNode): Config =
    Config(node.path("backend").asText(), node.path("endpoint").asText(),
      node.path("namespace").asText(), node.path("epoch").asText())

  private def runRedis(config: Config): Unit =
    val provider = DilockRedis.connect(config.endpoint, config.namespace, config.epoch)
    val leases = new ConcurrentHashMap[String, RedisLease]()
    val released = new ConcurrentHashMap[String, String]()
    try runLoop((request, output, dispatched) => processRedis(provider, leases, released, request, output, dispatched))
    finally provider.close()

  private def runEtcd(config: Config): Unit =
    val provider = DilockEtcd.connect(config.endpoint, config.namespace, config.epoch)
    val leases = new ConcurrentHashMap[String, EtcdLease]()
    val released = new ConcurrentHashMap[String, String]()
    try runLoop((request, output, dispatched) => processEtcd(provider, leases, released, request, output, dispatched))
    finally provider.close()

  private type Handler = (JsonNode, ObjectNode => Unit, AtomicBoolean) => Unit

  private def runLoop(handle: Handler): Unit =
    val outLock = new Object()
    val active = ConcurrentHashMap.newKeySet[String]()
    val pending = new ConcurrentHashMap[String, Future[?]]()
    val executor: ExecutorService = Executors.newCachedThreadPool((task: Runnable) =>
      val thread = new Thread(task, "dilock-scala-conformance-request")
      thread.setDaemon(true)
      thread
    )
    def write(node: ObjectNode): Unit = outLock.synchronized {
      System.out.println(json.writeValueAsString(node))
      System.out.flush()
    }
    try
      var line = readBoundedLine()
      while line.nonEmpty do
        val request = try json.readTree(line.get)
        catch
          case _: Exception => System.err.println("worker: malformed JSON input"); null
        if request != null then
          val id = request.path("id").asText()
          val op = request.path("op").asText()
          if id.isEmpty || op.isEmpty then System.err.println("worker: malformed request envelope")
          else if !active.add(id) then write(envelope(id, "invalid_argument"))
          else if op == "shutdown" then
            val response = envelope(id, "ok")
            response.put("outstanding_cleanup_attempts", pending.size())
            write(response)
            active.remove(id)
            line = None
          else if op == "cancel_request" then
            val task = pending.get(request.path("target_id").asText())
            val cancelled = task != null && task.cancel(true)
            val response = envelope(id, "ok")
            response.put("cancelled", cancelled)
            write(response)
            active.remove(id)
          else
            val dispatched = new AtomicBoolean(false)
            submitTracked(id, executor, pending, active) {
              try handle(request, write, dispatched)
              catch
                case error: Throwable =>
                  val response = envelope(id, mapError(error))
                  if error.isInstanceOf[InterruptedException] then
                    response.put("remote_outcome_unknown", remoteOutcomeUnknown(error, dispatched.get()))
                  write(response)
            }
        if line.nonEmpty then line = readBoundedLine()
    finally
      pending.values().asScala.foreach(_.cancel(true))
      executor.shutdownNow()
      executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)

  private def readBoundedLine(): Option[String] =
    val output = new java.io.ByteArrayOutputStream(maxLineBytes)
    var count = 0
    var char = System.in.read()
    while char != -1 && char != '\n' do
      count += 1
      if count <= maxLineBytes then output.write(char)
      char = System.in.read()
    if char == -1 && count == 0 then None
    else if count > maxLineBytes then
      throw new IllegalArgumentException("worker input line exceeds 65536 bytes")
    else Some(decodeUtf8Line(output.toByteArray))

  private[scala3] def decodeUtf8Line(bytes: Array[Byte]): String =
    val length = if bytes.nonEmpty && bytes.last == '\r'.toByte then bytes.length - 1 else bytes.length
    StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .decode(ByteBuffer.wrap(bytes, 0, length)).toString

  private def processRedis(provider: RedisProvider, leases: ConcurrentHashMap[String, RedisLease],
      released: ConcurrentHashMap[String, String], request: JsonNode, write: ObjectNode => Unit,
      dispatched: AtomicBoolean): Unit =
    val id = request.path("id").asText()
    request.path("op").asText() match
      case "hello" =>
        val response = hello(id, "redis")
        response.set("capabilities", json.createObjectNode().put("renewal", true).put("auto_renew", false).put("fencing", false))
        write(response)
      case "acquire" | "try_acquire" =>
        if request.path("auto_renew").asBoolean(false) then throw unsupported("automatic renewal")
        val resource = requiredText(request, "resource")
        val leaseMillis = positiveLong(request, "lease_ms", 30000)
        val lock = provider.lock(resource)
        dispatched.set(true)
        val lease = if request.path("op").asText() == "try_acquire" then
          lock.tryAcquire(Duration.ofMillis(leaseMillis)).fold(throw _, identity).orNull
        else
          val waitMillis = nonnegativeLong(request, "wait_ms", 5000)
          lock.acquire(AcquirePolicy(Duration.ofMillis(waitMillis), Duration.ofMillis(leaseMillis))).fold(throw _, identity)
        if lease == null then write(envelope(id, "busy"))
        else
          val localId = UUID.randomUUID().toString
          leases.put(localId, lease)
          val response = envelope(id, "acquired")
          response.put("lease_id", localId)
          response.put("effective_lease_ms", leaseMillis)
          write(response)
      case "renew" =>
        val lease = leases.get(requiredText(request, "lease_id"))
        if lease == null then throw new IllegalArgumentException("unknown lease_id")
        val leaseMillis = positiveLong(request, "lease_ms", 30000)
        lease.renew(Duration.ofMillis(leaseMillis))
        val response = envelope(id, "renewed")
        response.put("effective_lease_ms", leaseMillis)
        write(response)
      case "release" =>
        val leaseId = requiredText(request, "lease_id")
        val prior = released.get(leaseId)
        if prior != null then write(envelope(id, prior))
        else
          val lease = leases.get(leaseId)
          if lease == null then throw new IllegalArgumentException("unknown lease_id")
          val status = lease.release().fold(throw _, outcome => outcome.toString.toLowerCase(java.util.Locale.ROOT))
          leases.remove(leaseId)
          released.put(leaseId, status)
          write(envelope(id, status))
      case "inspect" =>
        val lease = leases.get(requiredText(request, "lease_id"))
        if lease == null then throw new IllegalArgumentException("unknown lease_id")
        val response = envelope(id, "ok")
        val state = try
          lease.assertValid()
          "held"
        catch case _: Throwable => "lost"
        response.put("state", state)
        write(response)
      case other => throw new IllegalArgumentException(s"unknown op: $other")

  private def processEtcd(provider: EtcdProvider, leases: ConcurrentHashMap[String, EtcdLease],
      released: ConcurrentHashMap[String, String], request: JsonNode, write: ObjectNode => Unit,
      dispatched: AtomicBoolean): Unit =
    val id = request.path("id").asText()
    request.path("op").asText() match
      case "hello" =>
        val response = hello(id, "etcd")
        response.set("capabilities", json.createObjectNode().put("renewal", false).put("auto_renew", false)
          .put("fencing", true).put("try_acquire", false))
        write(response)
      case "try_acquire" => throw unsupported("etcd native Lock does not support try_acquire")
      case "acquire" =>
        if request.path("auto_renew").asBoolean(false) then throw unsupported("automatic renewal configuration")
        val resource = requiredText(request, "resource")
        val waitMillis = nonnegativeLong(request, "wait_ms", 5000)
        val leaseMillis = positiveLong(request, "lease_ms", 30000)
        dispatched.set(true)
        val lease = provider.lock(resource).acquire(AcquirePolicy(Duration.ofMillis(waitMillis), Duration.ofMillis(leaseMillis))).fold(throw _, identity)
        val localId = UUID.randomUUID().toString
        leases.put(localId, lease)
        val response = envelope(id, "acquired")
        response.put("lease_id", localId)
        response.put("effective_lease_ms", leaseMillis)
        val fence = json.createObjectNode()
        fence.put("epoch", lease.fence.epoch())
        fence.put("value", lease.fence.value().toString)
        response.set("fence", fence)
        write(response)
      case "renew" => throw unsupported("manual renewal through this worker")
      case "release" =>
        val leaseId = requiredText(request, "lease_id")
        val prior = released.get(leaseId)
        if prior != null then write(envelope(id, prior))
        else
          val lease = leases.get(leaseId)
          if lease == null then throw new IllegalArgumentException("unknown lease_id")
          val status = lease.release().fold(throw _, outcome => outcome.toString.toLowerCase(java.util.Locale.ROOT))
          leases.remove(leaseId)
          released.put(leaseId, status)
          write(envelope(id, status))
      case "inspect" =>
        val lease = leases.get(requiredText(request, "lease_id"))
        if lease == null then throw new IllegalArgumentException("unknown lease_id")
        val response = envelope(id, "ok")
        val state = try
          lease.assertValid()
          "held"
        catch case _: Throwable => "lost"
        response.put("state", state)
        write(response)
      case other => throw new IllegalArgumentException(s"unknown op: $other")

  private def hello(id: String, backend: String): ObjectNode =
    val response = envelope(id, "ok")
    response.put("sdk_version", "0.1.0")
    response.put("protocol", 1)
    response.put("backend", backend)
    response.put("server_version", "unknown")
    response

  private def requiredText(request: JsonNode, field: String): String =
    val value = request.path(field)
    if !value.isTextual || value.asText().isEmpty then throw new IllegalArgumentException(s"$field is required")
    value.asText()

  private[scala3] def positiveLong(request: JsonNode, field: String, default: Long): Long =
    val value = if request.has(field) then integralLong(request, field) else default
    if value <= 0 then throw new IllegalArgumentException(s"$field must be positive")
    value

  private[scala3] def nonnegativeLong(request: JsonNode, field: String, default: Long): Long =
    val value = if request.has(field) then integralLong(request, field) else default
    if value < 0 then throw new IllegalArgumentException(s"$field must be non-negative")
    value

  private def integralLong(request: JsonNode, field: String): Long =
    val node = request.path(field)
    if !node.isIntegralNumber || !node.canConvertToLong then
      throw new IllegalArgumentException(s"$field must be an integer in the signed 64-bit range")
    node.longValue()

  private def mapError(error: Throwable): String =
    error match
      case dilock: DilockException => dilock.kind().toString match
        case "BACKEND_UNAVAILABLE" => "backend_unavailable"
        case "PROTOCOL_MISMATCH" => "protocol_mismatch"
        case "ACQUISITION_UNKNOWN" => "acquisition_unknown"
        case "ACQUISITION_TIMEOUT" => "timeout"
        case "RELEASE_UNKNOWN" => "release_unknown"
        case "LEASE_LOST" => "already_not_owned"
        case "UNSUPPORTED_CAPABILITY" => "unsupported_capability"
        case _ => "internal_error"
      case _: IllegalArgumentException => "invalid_argument"
      case _: InterruptedException => Thread.currentThread().interrupt(); "cancelled"
      case _ => "internal_error"

  private[scala3] def remoteOutcomeUnknown(error: Throwable, dispatched: Boolean): Boolean =
    if !dispatched then false
    else error match
      case cancelled: org.dilock.etcd.EtcdAcquisitionCancelledException => cancelled.remoteOutcomeUnknown()
      case interrupted: InterruptedException =>
        !Option(interrupted.getMessage).exists(message => message.contains("before send") || message.contains("before acquisition"))
      case _ => true

  private def unsupported(reason: String) =
    new DilockException(DilockException.Kind.UNSUPPORTED_CAPABILITY, reason)

  private def envelope(id: String, status: String): ObjectNode =
    val response = json.createObjectNode()
    response.put("id", id)
    response.put("status", status)
    response

  /** Registers before execution and removes cancelled-before-start tasks as well as completed calls. */
  private[scala3] def submitTracked(
      id: String,
      executor: ExecutorService,
      pending: ConcurrentHashMap[String, Future[?]],
      active: java.util.Set[String]
  )(action: => Unit): Unit =
    val started = new AtomicBoolean(false)
    val cleaned = new AtomicBoolean(false)
    def cleanup(): Unit =
      if cleaned.compareAndSet(false, true) then
        pending.remove(id)
        active.remove(id)
    val task = new FutureTask[Unit](new Callable[Unit]:
      override def call(): Unit =
        started.set(true)
        try action
        finally cleanup()
    ):
      override def done(): Unit =
        if isCancelled && !started.get() then cleanup()
    pending.put(id, task)
    try executor.execute(task)
    catch
      case error: RuntimeException =>
        cleanup()
        throw error
