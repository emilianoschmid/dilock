# Clojure with Redis

`quickstart/main.clj` uses the Clojure resource-scope macros over the Java Redis engine. It depends on the Java core, Redis adapter, and Clojure facade.

Install the Java reactor in a local Maven repository, then run `clojure -M:test` from `sdk/clojure`. Run this source from that project with `clojure -M -e '(load-file "../../docs/examples/clojure-redis/quickstart/main.clj")'`. Set `DILOCK_REDIS_URL`, `DILOCK_NAMESPACE` (default `default`), and `DILOCK_COORDINATION_EPOCH`. Redis namespace metadata must already match. Protocol and backend operations are implemented by Java. For full connection and acquisition options, see [the Clojure guide](../../../sdk/clojure/README.md). Resource handles share a provider but each acquisition is independent.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
