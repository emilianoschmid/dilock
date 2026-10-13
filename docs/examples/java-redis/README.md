# Java Redis consumer

This standalone Maven application uses the Java SDK JAR and its declared runtime dependencies. It connects to one already-running Redis authority, validates operator-provisioned namespace metadata, and demonstrates two independent providers contending for one unique resource. The second provider sees acknowledged busy while the first lease is held, then acquires after the first owner closes its lease.

The example does not provision Redis, create namespace metadata, or write any persistent application data. Set `DILOCK_REDIS_URL`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` in the process environment. The URL may contain credentials; keep it in an environment or secret manager and out of source, command history, and logs. The configured epoch must match the existing metadata exactly. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md) for the required draft 1 record. Do not use a sample epoch or namespace in a real deployment without operator approval.

## Build and run against the local SDK JAR

From the repository root, build and install the SDK artifact only into the configured workspace-local Maven repository, then compile this consumer offline:

```powershell
$env:JAVA_HOME = 'C:\Users\emili\AppData\Local\CameraDoctor\toolchains\jdk\21'
$env:MAVEN_OPTS = '-Dmaven.repo.local=.tools/m2repo'
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml install
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml dependency:build-classpath '-Dmdep.outputFile=target/runtime-classpath.txt'
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f docs\examples\java-redis\pom.xml package
```

All three commands use only dependencies already cached in `.tools/m2repo`; they do not contact Maven Central. Run the consumer with an already provisioned Redis authority. The second command writes the resolved runtime classpath to `sdk/java/target/runtime-classpath.txt`; the launch uses that local classpath and needs no Maven execution plugin:

```powershell
$runtime = (Get-Content sdk/java/target/runtime-classpath.txt -Raw).Trim()
$classpath = "docs/examples/java-redis/target/classes;sdk/java/target/dilock-java-0.1.0.jar;$runtime"
java -cp $classpath org.dilock.examples.RedisConsumer
```

Expected output after the environment variables are set and the backend is reachable:

```text
contender: busy (acknowledged contention)
successor: acquired after owner release
```

The example requires only the metadata record to have been provisioned ahead of time; it never calls the SDK's administrative `provisionNamespace` helper.

## Lifetime, interruption, and failures

Each `RedisProvider` owns its Jedis client pool and closes it at the end of the outer try-with-resources scope. A `RedisLock` is a reusable resource handle. Each `RedisLease` belongs to one acquisition generation and is released through its own try-with-resources scope. Cleanup is synchronous and bounded by the SDK's Redis socket timeout; if cleanup cannot be acknowledged, the server-side TTL bounds the orphan.

The SDK is synchronous, and this example runs operations serially. A distributed lease does not coordinate threads within the process and is not a replacement for a local concurrency primitive. `tryAcquire` returns `Optional.empty()` only when Redis confirms ordinary contention. Backend failures, protocol mismatch, uncertain acquisition, timeout, lease loss, and unknown release are typed `DilockException` cases and must not be treated as busy. `acquire(waitMillis, leaseMillis)` uses one monotonic wait budget; expiry of that wait budget is an `ACQUISITION_TIMEOUT` error.

Thread interruption before a Redis command is sent is local cancellation. If interrupted after dispatch, the operation may have reached Redis; the SDK attempts owner-specific cleanup and reports interruption/uncertainty rather than ordinary contention. The example's `main` restores the Java interrupt flag before propagating `InterruptedException`. Applications running this flow in an executor should cancel by interrupting the task and handle both `InterruptedException` and `DilockException` explicitly.

The lease's conservative local validity deadline does not stop stale application work. Redis owner tokens prevent a late owner-checked release from deleting a successor, but are not fencing tokens. This Redis profile offers no strong fencing or failover guarantee. Protect external effects with resource-side transactions, idempotency, or fencing where the resource supports it. A local build or this short consumer run is not production-readiness evidence.
