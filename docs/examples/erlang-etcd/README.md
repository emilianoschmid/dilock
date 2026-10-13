# Erlang with etcd

`quickstart/main.erl` calls the Erlang-facing facade over the shared Elixir/BEAM native etcd engine. It is not an independent Erlang lock implementation or package. First build the Elixir application and dependencies as described in [the Elixir SDK guide](../../../sdk/elixir/README.md), then compile with the BEAM paths available: `erlc -o <beam-output> docs/examples/erlang-etcd/quickstart/main.erl`.

Call `main:run(Endpoint, Epoch, Resource)` from an Erlang runtime with the facade and Elixir application's BEAM paths loaded. etcd metadata for `default` must already match `Epoch` and use profile `etcd-native-lock-v1`; the protected resource must atomically enforce the returned epoch/revision fence. For explicit provider budgets and supervised provider lifetimes, see [the Erlang facade guide](../../../sdk/erlang/README.md). Handles share a provider process, while acquisitions remain independent.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
