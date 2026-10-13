# Elixir + Redis quickstart

This consumer uses Dilock's Elixir Redis provider backed by Redix. It acquires two distinct resources sequentially through one shared provider: first with defaults, then with an explicit policy. The handles reuse the provider's Redis client. It is a native Elixir implementation; the isolated style examples are not used. The SDK is experimental and Redis's single-authority profile has no strong fencing guarantee.

Prerequisites: Elixir 1.18+, Mix dependencies, a reachable Redis server, and pre-provisioned metadata for namespace `default` and the epoch supplied in `DILOCK_COORDINATION_EPOCH`. Redis metadata is described in [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The provider validates but never provisions it.

From the repository root, set `REDIS_URL` and `DILOCK_COORDINATION_EPOCH`, then run:

```sh
cd sdk/elixir
mix deps.get
mix run ../../docs/examples/elixir-redis/quickstart/main.exs
```

The script reads `DILOCK_NAMESPACE` (default `default`). The example requires the corresponding metadata to exist before it starts.
