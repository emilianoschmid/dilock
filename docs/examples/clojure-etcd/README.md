# Clojure with etcd

`quickstart/main.clj` uses the Clojure resource-scope macros over the Java native etcd engine. It depends on the Java core, etcd adapter, and Clojure facade.

Install the Java reactor in a local Maven repository, then run `clojure -M:test` from `sdk/clojure`. Run this source from that project with `clojure -M -e '(load-file "../../docs/examples/clojure-etcd/quickstart/main.clj")'`. Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE` (default `default`), and `DILOCK_COORDINATION_EPOCH`. etcd metadata with profile `etcd-native-lock-v1` must already match; enforce its epoch/revision fence atomically at the protected resource. Protocol and backend operations are implemented by Java. For full settings, see [the Clojure guide](../../../sdk/clojure/README.md); handles share the provider, while lock acquisitions remain independent.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
