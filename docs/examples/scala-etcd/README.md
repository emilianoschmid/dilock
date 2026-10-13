# Scala with etcd

`quickstart/main.scala` is a Scala 3 consumer of the native etcd engine through the Scala facade. Runtime dependencies are the Dilock Java core, etcd adapter, and Scala 3 facade.

Install the Java reactor in the workspace Maven repository, then run `sbt test` from `sdk/scala`; the repository quickstart consumer is compiled by that test build. Set `DILOCK_ETCD_ENDPOINT` and `DILOCK_COORDINATION_EPOCH` and run this entry point from a Scala 3 consumer project. etcd metadata for `default` must already exist with profile `etcd-native-lock-v1` and the matching epoch. The protected resource must atomically enforce the epoch/revision fence. The facade reuses Java protocol code. For fully configured policies, see [the Scala guide](../../../sdk/scala/README.md); handles share a channel but not an acquisition.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
