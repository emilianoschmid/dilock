# Groovy with Redis

`quickstart/main.groovy` uses the Groovy closure facade over the Java Redis engine. It requires the Dilock Java core, Redis adapter, and Groovy facade on the classpath.

Install the Java reactor locally, then run `mvn -f sdk/groovy/pom.xml test`. With those artifacts on the Groovy classpath and `DILOCK_REDIS_URL`, `DILOCK_NAMESPACE` (default `default`), and `DILOCK_COORDINATION_EPOCH` set, run `groovy docs/examples/groovy-redis/quickstart/main.groovy`. Matching Redis namespace metadata must be provisioned first. The closure scopes close the lease and provider. For full configuration, see [the Groovy guide](../../../sdk/groovy/README.md); resource handles reuse a provider's pool but acquisitions are independent.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
