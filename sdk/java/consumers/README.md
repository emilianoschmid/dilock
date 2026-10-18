# Exact Maven artifact consumer

`package_consumer.py` builds the Maven JAR in offline mode from a fresh copy of the current source, creates source and Javadoc classifier archives, copies the exact runtime JAR to a separate external consumer workspace, then compiles and runs the consumer fixtures against that archive and the pinned Maven runtime dependency classpath.

The fixtures verify the installed identity, Redis, etcd, PostgreSQL, lease, and fence API surfaces without connecting to an authority. The PostgreSQL-only fixture also verifies that the JDBC driver is present without leaking unrelated backend dependencies. Backend behavior is validated separately by the real-server integration and cross-SDK conformance suites.

Run from the repository root with a new, empty scratch directory outside the checkout:

```powershell
python sdk/java/consumers/package_consumer.py `
  --output validation/implementation/m6-java-package-consumer.json `
  --scratch $env:TEMP/dilock-java-package-consumer
```

The report includes the exact JAR SHA-256, commands, status, and the retained external consumer workspace. Existing reports and non-empty workspaces are never overwritten.

The driver accepts `--maven`, `--java`, `--javac`, and `--jar` to select toolchains; the corresponding environment overrides are `DILOCK_MAVEN`, `DILOCK_JAVA`, `DILOCK_JAVAC`, and `DILOCK_JAR`. Maven defaults to the repository-provisioned toolchain when present, then `PATH`. Its dependency cache defaults to `REPO/.tools/m2repo`; override it with `--maven-repo` or `DILOCK_MAVEN_REPO`. A minimal offline settings file is generated in scratch by default; pass `--maven-settings` or `DILOCK_MAVEN_SETTINGS` only when a settings file is needed. All Maven invocations retain `-o` and use the selected local repository, so a missing dependency fails instead of fetching it.
