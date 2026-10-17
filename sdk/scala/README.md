# Dilock Scala SDK

This facade reuses the production Java Redis, etcd, and Consul engines. It does not
reimplement the wire protocol or use the local-only `examples/style/scala/`
sample as a backend. The shared Java engine implements the protocol; distinct
Scala 3 and Scala 2.13 facade sources preserve each language line's syntax.
`org.dilock:dilock-scala3_3:0.1.0` remains the Scala 3 local candidate. The
Scala 2.13 candidate uses the `dilock-scala213_2.13` binary suffix. These are
local candidates, not verified Maven Central coordinates, and publication is
not configured.

The Java core is a normal dependency. Java Redis, etcd, and Consul adapters are provided
dependencies, so consumers must add the adapter(s) they actually use:
`org.dilock:dilock-redis`, `org.dilock:dilock-etcd`, and/or `org.dilock:dilock-consul`. This keeps backend
clients optional.

## Redis

Scala 2.13 consumers use `org.dilock.scala213` and Scala 3 consumers use
`org.dilock.scala3`; the providers, handles, finite defaults, typed backend
errors, cancellation behavior and acknowledged release semantics are shared.
The Scala 2.13 API uses ordinary case classes and `Either`; `withLease` handles
lease scope and acknowledged release. Use `try/finally` or `scala.util.Using`
for provider cleanup:

```scala
import org.dilock.scala213._

val provider = DilockRedis.connect("redis://127.0.0.1:6379", "operator-epoch")
try {
  val result = provider.lock("invoice/42/reconcile").withLease() { lease =>
    lease.assertValid()
    "completed"
  }
  result.fold(throw _, identity)
} finally provider.close()
```

The namespace metadata must be provisioned before connecting. A provider owns
its Java client pool; resource handles share it. `withLease` awaits bounded
explicit release in `finally`, and returns typed acquisition or release failures as `Left`.
Only the Java Redis or etcd adapter's typed `RELEASED` outcome confirms scope
cleanup; a different enum with that name does not.
If both the work block and cleanup fail, the work failure stays primary and the
cleanup failure is attached as suppressed. A cleanup failure is returned as a
typed `Left` when it is a Dilock failure and the block succeeded.
Exceptions thrown by the user block propagate unchanged, including a
`DilockException`; only acquisition and standalone cleanup failures are mapped
to `Left`.

```scala
import java.time.Duration
import org.dilock.scala3.*

val provider = DilockRedis.connect("redis://127.0.0.1:6379", "billing", "operator-epoch")
try
  val result = provider.lock("invoice/42/reconcile").withLease() { lease =>
    lease.assertValid()
    // Protect the resource with transactions, idempotency, or resource-side fencing.
    "completed"
  }
  result match
    case Right(value) => println(value)
    case Left(error) => throw error
finally provider.close()
```

Defaults are a 5-second contention wait, a 30-second lease, and 2-second RPC
and cleanup budgets. Full Java provider policies are accepted directly:

```scala
import java.time.Duration
import org.dilock.scala3.*

val acquisition = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(20))
val options = new org.dilock.redis.RedisProviderOptions(
  "rediss://cache.example:6380", "billing", "operator-approved-epoch",
  Duration.ofSeconds(2), Duration.ofSeconds(2), acquisition.toJava, null
)
val provider = DilockRedis.connect(options)
try
  val result = provider.lock("invoice/42/reconcile").withLease(acquisition) { lease =>
    lease.assertValid()
    "completed"
  }
  result.fold(throw _, println)
finally provider.close()
```

For etcd, fully configure the native gRPC provider separately. The resource
must enforce the returned epoch/revision fence atomically:

```scala
import java.time.Duration
import org.dilock.scala3.*

val acquisition = AcquirePolicy(Duration.ofSeconds(3), Duration.ofSeconds(20))
val etcdOptions = new org.dilock.etcd.EtcdProviderOptions(
  "http://127.0.0.1:2379", "billing", "operator-approved-etcd-epoch",
  Duration.ofSeconds(2), Duration.ofSeconds(2), acquisition.toJava, null
)
val provider = DilockEtcd.connect(etcdOptions)
try
  val result = provider.lock("invoice/42/reconcile").withLease(acquisition) { lease =>
    lease.assertValid()
    val fence = lease.fence
    // The protected resource must atomically reject stale epoch/revision values.
    println(s"Enforce fence ${fence.epoch()}/${fence.value()} at the resource")
  }
  result.fold(throw _, identity)
finally provider.close()
```

`tryAcquire` returns `Right(None)` only for ordinary Redis contention. Redis does
not provide strong fencing. For etcd, `EtcdLease.fence` contains the verified
epoch/revision and the protected resource must enforce it atomically. etcd's
native Lock profile has no safe try-lock RPC and returns
`Left(UNSUPPORTED_CAPABILITY)` without treating a deadline as contention.

The etcd facade also exposes an `EtcdAcquirePolicy` so a caller can opt into
renewing the same server lease while work is running. The shared `AcquirePolicy`
leaves renewal disabled, and Redis does not expose this capability:

```scala
val renewal = EtcdAcquirePolicy(
  waitTimeout = Duration.ofSeconds(5),
  lease = Duration.ofSeconds(30),
  autoRenew = true
)
val result = provider.lock("invoice/42/reconcile").withLease(renewal) { lease =>
  lease.assertValid()
  val fence = lease.fence
  // The protected resource must atomically enforce the epoch/revision fence.
  fence.value()
}
result.fold(throw _, identity)
```

Acquisition is synchronous and preserves thread interruption; the facade does
not introduce an async runtime. Providers, handles, and leases are safe for
concurrent calls according to the Java engine's documented contracts. Closing
a provider is bounded and idempotent; automatic scope cleanup returns unknown
release outcomes as a typed `Left`. No facade can stop a process paused
beyond lease expiry.

## Validation

`sbt test` is the native build and test command, and `sbt doc` generates
Scaladoc. Use `sbt test` to run the native test suites, `sbt doc` to generate Scaladoc,
and `sbt packageBin` to build the artifacts. Registry coordinates must be
verified before publication.
