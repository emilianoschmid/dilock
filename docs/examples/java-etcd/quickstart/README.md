# Java etcd quickstart

No flag runs one short resource using defaults; `--configured` supplies a complete explicit policy for one resource; `--shared-provider` acquires two distinct resources from one provider/channel. Shared connection reuse is not a transaction: the acquisitions and leases are independent and not atomic.

Provision namespace metadata with the etcd native-lock profile and approved `dev-1` epoch before running. Default and shared-provider modes use namespace `default`; configured mode uses `reports`. Provision both metadata records to run all three modes. The SDK validates existing metadata and does not create or repair it. Set `DILOCK_ETCD_ENDPOINT` and `DILOCK_EPOCH` for your environment.

From the repository root, install local candidate artifacts and run with Java 21:

```powershell
$env:JAVA_HOME = "C:\Users\<user>\path\to\jdk-21"
.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s .tools\maven-settings.xml -f sdk\java\pom.xml -pl core,etcd -am install
Push-Location docs\examples\java-etcd\quickstart
& ..\..\..\..\.tools\apache-maven-3.10.0\bin\mvn.cmd -o -s ..\..\..\..\.tools\maven-settings.xml compile org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath "-Dmdep.outputFile=target/classpath.txt"
$cp = (Get-Content target\classpath.txt -Raw).Trim()
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_EPOCH = "dev-1"
java -cp "target/classes;$cp" Quickstart
java -cp "target/classes;$cp" Quickstart --configured
java -cp "target/classes;$cp" Quickstart --shared-provider
Pop-Location
```

The defaults bound wait to 5 s and request a 30 s lease; configured mode uses 10 s/60 s and 2 s RPC/cleanup budgets. The example prints etcd create revisions. Enforce `(epoch, create_revision)` atomically at the protected resource when stale-writer protection is required; lease expiry cannot stop paused application work. This quickstart supports a plaintext HTTP endpoint only: its public configuration does not demonstrate client TLS certificates, custom trust roots, or etcd username/password authentication. Do not expose such an endpoint to untrusted networks; configure secured transport through a separately reviewed integration before production use. Native etcd lock does not expose safe immediate try-acquire.
