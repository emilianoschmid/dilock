# Java Redis quickstart

This consumer has three modes: no flag runs one short operation using defaults; `--configured` runs one resource with a complete explicit acquisition policy; `--shared-provider` acquires two distinct resources through one provider. The two-resource case illustrates handle/client reuse only: the leases are independent, acquisition is not atomic, and releasing one does not release the other.

Provision Redis namespace metadata administratively before running. No-flag and shared-provider modes expect namespace `default`; configured mode uses namespace `reports`. Provision both with approved epoch `dev-1` if you will run all three modes. Runtime connection validates metadata and never creates or repairs it. Configure `DILOCK_REDIS_ENDPOINT` and `DILOCK_EPOCH` to match your environment.

From the repository root, install local candidate artifacts and run with Java 21:

```powershell
$env:JAVA_HOME = "C:\Users\<user>\path\to\jdk-21"
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml -pl core,redis -am install
Push-Location docs\examples\java-redis\quickstart
& ..\..\..\..\.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s ..\..\..\..\.tools\maven-settings.xml compile org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath "-Dmdep.outputFile=target/classpath.txt"
$cp = (Get-Content target\classpath.txt -Raw).Trim()
$env:DILOCK_REDIS_ENDPOINT = "redis://127.0.0.1:6379"
$env:DILOCK_EPOCH = "dev-1"
java -cp "target/classes;$cp" Quickstart
java -cp "target/classes;$cp" Quickstart --configured
java -cp "target/classes;$cp" Quickstart --shared-provider
Pop-Location
```

Defaults use a finite 5 s wait and 30 s lease. Configured mode supplies a full 10 s/60 s policy, 2 s RPC and cleanup budgets, and an observer. `try (var lease = ...)` performs bounded best-effort cleanup; call `release()` when the acknowledgement matters. A lease is not a thread/process mutex and expiry cannot stop stale work. This quickstart shows a plain Redis URI; use a secured Redis URI and deployment-approved TLS/auth configuration for protected environments. Redis single-authority leases do not promise strong fencing across unsafe failover.
