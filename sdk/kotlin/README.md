# Dilock Kotlin/JVM SDK

This Kotlin/JVM facade reuses the production Java Redis and etcd engines. It does
not contain another lock protocol, transport, or local-lock fallback. JVM 21 is
the configured target; this is not a Kotlin Multiplatform SDK. The Maven
coordinate `org.dilock:dilock-kotlin:0.1.0` is a local candidate only. No
publication is configured or authorized.

The facade depends on the Java core and Kotlin coroutines. Redis and etcd Java
adapters are compile-only optional integrations: consumers must add
`org.dilock:dilock-redis` or `org.dilock:dilock-etcd` (or both) explicitly.
This keeps backend client dependencies out of a single-backend consumer.

## Redis quickstart

The Redis namespace metadata must already have been provisioned for the same
namespace and epoch. Connect once, reuse resource handles, and scope each
acquisition with `withLease`:

```kotlin
import kotlinx.coroutines.runBlocking
import org.dilock.kotlin.DilockRedis

DilockRedis.connect("redis://127.0.0.1:6379", "billing", "operator-epoch").use { provider ->
    runBlocking {
        provider.lock("invoice/42/reconcile").withLease { lease ->
            lease.assertValid()
            // Protect the actual resource with transactions, idempotency, or resource-side fencing.
        }
    }
}
```

The defaults are a 5-second contention wait and a 30-second lease. A coroutine
cancellation interrupts the worker thread running Java acquisition; the Java
engine classifies the remote result and attempts bounded owner-specific cleanup.
If a successful acquisition response races with cancellation and cannot be
delivered to the coroutine, the facade closes that undisclosed lease.
`withLease` awaits the lease's bounded, result-bearing `release()` in a
non-cancellable IO context, so an unknown release acknowledgement fails an
otherwise successful scope. A scope accepts only the typed `RELEASED` outcome
mapped from the Java Redis or etcd adapter. Explicit `release()` is suspending
and returns the backend-neutral Kotlin
`ReleaseOutcome` (`RELEASED` or `ALREADY_NOT_OWNED`). Its bounded Java cleanup also
runs non-cancellably so caller cancellation cannot interrupt an in-flight
owner-checked release. Cancellation is delivered after that cleanup returns.
`tryAcquire` returns `null` only
for ordinary Redis contention. Provider and lease `close()` are synchronous and
bounded and therefore `use` is safe for a genuinely synchronous scope.
If the block fails and a cleanup observer also throws, the block failure remains
primary and the cleanup failure is attached as a suppressed exception. A cleanup
failure is propagated when the block itself succeeds.

## Complete configuration and etcd

```kotlin
import java.time.Duration
import org.dilock.kotlin.*

val acquisition = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(20))
val providerOptions = ProviderSettings(
    endpoint = "rediss://cache.example:6380",
    namespace = "billing",
    epoch = "operator-approved-epoch",
    rpcTimeout = Duration.ofSeconds(2),
    cleanupTimeout = Duration.ofSeconds(2),
    defaultAcquisition = acquisition
)

runBlocking {
    DilockRedis.connect(providerOptions).use { provider ->
        provider.lock("invoice/42/reconcile").withLease(acquisition) { lease -> lease.assertValid() }
    }
}
```

The same complete Kotlin-native settings apply to etcd. Its endpoint must be a
gRPC HTTP(S) authority, and the etcd namespace metadata must already exist with
profile `etcd-native-lock-v1` and the supplied epoch:

```kotlin
val etcdSettings = providerOptions.copy(endpoint = "http://127.0.0.1:2379")
runBlocking {
    DilockEtcd.connect(etcdSettings).use { provider ->
        provider.lock("invoice/42/reconcile").withLease(acquisition) { lease ->
            lease.assertValid()
            val fence = lease.fence()
            // Atomically enforce (fence.epoch, fence.revision) at the protected resource.
        }
    }
}
```

Use `DilockEtcd.connect(...)` with pre-provisioned metadata and a stable,
operator-approved epoch for etcd's native Lock profile. `EtcdLease.fence()`
returns the verified epoch/revision fence; the protected resource must enforce
it atomically. etcd has no safe try-lock RPC, so its try-acquire call fails with
`UNSUPPORTED_CAPABILITY`. Redis does not provide strong fencing. Neither
backend can stop a process paused beyond lease expiry.

Automatic renewal of a held etcd lease is opt-in and etcd-specific:

```kotlin
val renewing = EtcdAcquirePolicy(
    wait = Duration.ofSeconds(2),
    lease = Duration.ofSeconds(10),
    autoRenew = true,
)
runBlocking {
    DilockEtcd.connect(etcdSettings).use { provider ->
        provider.lock("invoice/42/reconcile").withLease(renewing) { lease ->
            lease.assertValid()
            // The resource still needs to enforce the etcd fence atomically.
        }
    }
}
```

Renewal extends etcd's server lease while the client can service keepalives; it
does not make paused work safe after lease loss and is not fencing. Redis keeps
its separate owner-checked manual `renew` operation and has no automatic-renew
policy in this facade.

The provider owns its Java client pool/channel until `close`; resource handles
reuse that transport and are safe for concurrent callers. Leases represent
independent owner generations. Java `DilockException` types and backend errors
are preserved; busy, timeout, cancellation, unsupported capability, and
uncertain outcomes remain distinct. Java calls are synchronous under the
suspending facade and run on `Dispatchers.IO`. Do not call blocking provider
construction from a latency-sensitive event loop.

## Local checks

Run `gradlew.bat test` with JDK 21. To load the pinned Dokka 2.0.0 plugin and generate
Kotlin API documentation, use `gradle -PwithDokka=true dokkaHtml`.
The documentation plugin is resolved only for that task. Supported-version and
installed-artifact consumer validation remain part of release qualification.
The Java core, Redis and etcd modules must already be installed in the local
Maven repository. Live Redis/etcd tests require provisioned disposable
authorities and are not replaced by `examples/style/kotlin/`.

The opt-in Redis integration test uses the production Java Redis adapter through
the Kotlin facade. It provisions a uniquely named test namespace, checks
independent-provider contention, TTL expiry, renewal, handoff, and stale-owner
release, then removes its metadata. Point it only at a disposable Redis
authority; when the variable is unset, JUnit skips the live test:

```powershell
$env:DILOCK_TEST_REDIS_ENDPOINT = "redis://127.0.0.1:6379"
gradle test
```

After installing the Java core and the selected backend adapter artifacts into
a local Maven repository, publish this Kotlin candidate to a local filesystem
repository and run the independent consumer against its Maven POM:

```powershell
$repo = Join-Path $env:TEMP "dilock-kotlin-maven-consumer"
New-Item -ItemType Directory -Force $repo | Out-Null
Copy-Item .tools/m2repo/org/dilock (Join-Path $repo "org") -Recurse -Force
gradle --project-dir sdk/kotlin -PdilockMavenRepo=$repo test jar sourcesJar publishMavenJavaPublicationToLocalValidationRepository
gradle --project-dir sdk/kotlin/consumer -PdilockMavenRepo=$repo run
```

The consumer resolves `org.dilock:dilock-kotlin:0.1.0` by Maven coordinates,
adds the optional Redis and etcd adapters explicitly, and runs a disposable
Redis contention/handoff check when `DILOCK_TEST_REDIS_ENDPOINT` points at a
test server. Do not point this smoke test at production Redis: it provisions
and removes unique test namespace metadata.
