# Java etcd public API consumer

This standalone Maven application uses the Java SDK JAR against one already-running etcd v3 authority. Two independently owned providers acquire the same unique resource in sequence. It prints each epoch-scoped `create_revision` fence, explicitly closes the first lease, and acquires a successor. etcd's native `Lock` RPC has no non-blocking try-lock operation, so a timeout is never represented as ordinary busy contention.

An operator must provision `/dilock/namespaces/<namespace>/meta` before connecting. Its UTF-8 JSON value must contain `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The SDK reads and validates metadata but does not create or modify it. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md). Use a disposable non-production authority for this short demonstration.

## Build and run offline

From the repository root, use the already installed local Maven and workspace repository:

```powershell
$env:JAVA_HOME = 'C:\Users\emili\AppData\Local\CameraDoctor\toolchains\jdk\21'
$env:MAVEN_OPTS = '-Dmaven.repo.local=.tools/m2repo'
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml install
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml dependency:build-classpath '-Dmdep.outputFile=target/runtime-classpath.txt'
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f docs\examples\java-etcd\pom.xml package
```

`-o` uses only artifacts already present under `.tools/m2repo`; none of the commands accesses Maven Central. The installed artifact is local to this checkout. For a normal consumer, use the configured Maven repository and resolve the published version there.

Set the endpoint and exact operator-provisioned metadata identity before running:

```powershell
$env:DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:2379'
$env:DILOCK_NAMESPACE = 'example'
$env:DILOCK_EPOCH = 'operator-approved-cluster-incarnation'
$runtime = (Get-Content sdk/java/target/runtime-classpath.txt -Raw).Trim()
$classpath = "docs/examples/java-etcd/target/classes;sdk/java/target/dilock-java-0.1.0.jar;$runtime"
java -cp $classpath org.dilock.examples.EtcdConsumer
```

The provider currently accepts a plaintext `http://` endpoint only. Do not place credentials in source, command history, or logs.

## Lifetime, interruption, fencing, and limits

Each provider owns a gRPC channel and keepalive scheduler and is closed with try-with-resources. Each lease is an acquisition generation and is closed in its own scope. The synchronous `acquire(waitMillis, leaseMillis)` uses etcd's queued native Lock and throws a typed acquisition timeout when its wait budget expires; it does not return “busy.” `tryAcquire` throws `UnsupportedCapabilityException` because native etcd Lock has no safe try-lock RPC. Backend failures and uncertain acquisition are typed failures, not contention. Interruption is propagated with the thread's interrupt flag restored.

The fence is `(epoch, create_revision)`. A protected resource must atomically validate the deployment-approved epoch and reject stale revisions before relying on it. Restore or replacement of the coordinator requires a coordinated epoch migration. Lease expiry and local `LOST` state cannot stop stale application work; fencing is effective only when the downstream resource enforces it. The sample does not prove external fencing, quorum loss, or failover safety.
