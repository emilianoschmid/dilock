# Redis-only artifact consumer

This independent Gradle project verifies that the published Kotlin facade can be used with only the Redis adapter on the runtime classpath. It intentionally does not depend on `dilock-etcd`.

Install the Java core/Redis artifacts and Kotlin facade into a local Maven repository, then run this project with that repository:

```powershell
gradle --project-dir sdk/kotlin/consumer-redis-only `
  -PdilockMavenRepo=C:\path\to\local-maven-repository run
```

Set `DILOCK_TEST_REDIS_ENDPOINT` to a disposable Redis endpoint. The consumer provisions a unique namespace marker, checks two-provider contention, acknowledged release and successor handoff, then removes the marker. It does not clean or flush any other Redis data.
