# Kotlin with Redis

`quickstart/main.kt` uses the Kotlin coroutine facade over the Java Redis engine. Add the Dilock Java core, Redis adapter, Kotlin facade, and coroutines dependency to a consumer project; the Redis Java adapter is an explicit optional dependency of the Kotlin facade.

The repository's Kotlin SDK can be checked with `gradle --project-dir sdk/kotlin test` after installing the Java reactor into the workspace Maven repository. Set `REDIS_URL` and `DILOCK_COORDINATION_EPOCH`, then run this entry point from a Kotlin/JVM 21 consumer project with its dependencies configured. Metadata for `default` must already be provisioned with that epoch. `use` scopes provider cleanup; `withLease` awaits release. For full settings, see [the Kotlin guide](../../../sdk/kotlin/README.md). Multiple handles from one provider reuse its client pool while acquiring independently.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
