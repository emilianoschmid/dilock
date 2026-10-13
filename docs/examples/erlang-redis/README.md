# Erlang with Redis

`quickstart/main.erl` calls the Erlang-facing facade over the shared Elixir/BEAM Redis engine. It is not an independent Erlang lock implementation or package. First build the Elixir application and dependencies as described in [the Elixir SDK guide](../../../sdk/elixir/README.md), then compile with the BEAM paths available: `erlc -o <beam-output> docs/examples/erlang-redis/quickstart/main.erl`.

Call `main:run(Endpoint, Epoch, Resource)` from an Erlang runtime with the facade and Elixir application's BEAM paths loaded. Redis metadata for `default` must already match `Epoch`. For explicit provider budgets and supervised provider lifetimes, see [the Erlang facade guide](../../../sdk/erlang/README.md). Resource handles share one provider process; separate acquisitions remain distinct.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
