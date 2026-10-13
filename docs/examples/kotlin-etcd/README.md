# Kotlin with etcd

`quickstart/main.kt` uses the Kotlin coroutine facade over the Java native etcd engine. Add the Dilock Java core, etcd adapter, Kotlin facade, and coroutines dependency to a consumer project; the etcd Java adapter is an explicit optional dependency.

The repository's Kotlin SDK can be checked with `gradle --project-dir sdk/kotlin test` after installing the Java reactor into the workspace Maven repository. Set `ETCD_URL` and `DILOCK_COORDINATION_EPOCH`, then run this entry point from a Kotlin/JVM 21 consumer project with its dependencies configured. etcd metadata for `default` must already exist with profile `etcd-native-lock-v1` and that epoch. The protected resource must atomically enforce the epoch/revision fence. This facade reuses the Java engine. For full settings, see [the Kotlin guide](../../../sdk/kotlin/README.md); additional handles reuse the channel but each acquisition has independent ownership.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
