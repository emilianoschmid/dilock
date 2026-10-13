# Scala with Redis

`quickstart/main.scala` is a Scala 3 consumer of the Java Redis engine through the Scala facade. Runtime dependencies are the Dilock Java core, Redis adapter, and Scala 3 facade.

Install the Java reactor in the workspace Maven repository, then run `sbt test` from `sdk/scala`; the repository quickstart consumer is compiled by that test build. Set `DILOCK_REDIS_URL` and `DILOCK_COORDINATION_EPOCH` and run this entry point from a Scala 3 consumer project. Metadata for `default` must already be provisioned with the matching epoch. The facade does not implement a separate Redis protocol. For fully configured policies, see [the Scala guide](../../../sdk/scala/README.md). Handles from one provider share its Java pool; their acquisitions remain separate.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
