# Groovy with etcd

`quickstart/main.groovy` uses the Groovy closure facade over the Java native etcd engine. It requires the Dilock Java core, etcd adapter, and Groovy facade on the classpath.

Install the Java reactor locally, then run `mvn -f sdk/groovy/pom.xml test`. With those artifacts on the Groovy classpath and `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE` (default `default`), and `DILOCK_COORDINATION_EPOCH` set, run `groovy docs/examples/groovy-etcd/quickstart/main.groovy`. Matching etcd metadata with profile `etcd-native-lock-v1` must be provisioned first. Enforce the epoch/revision fence atomically at the protected resource. This is a facade over the Java backend. For full configuration, see [the Groovy guide](../../../sdk/groovy/README.md); handles reuse one provider channel but own separate acquisitions.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
