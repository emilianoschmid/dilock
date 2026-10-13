# Elixir + etcd quickstart

This consumer uses Dilock's native Elixir etcd provider, generated from the focused etcd v3 protocol definitions and calling native Lock/Lease RPCs. It acquires two resources sequentially on one provider/channel, first with defaults and then with explicit policy; both callbacks print the epoch/revision fence. The SDK is experimental; downstream writes must atomically enforce that fence.

Prerequisites: Elixir 1.18+, Mix dependencies, a reachable etcd v3 endpoint, and metadata already provisioned at `/dilock/namespaces/<namespace>/meta` for the configured epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Dilock never creates the metadata.

From the repository root, set `DILOCK_COORDINATION_EPOCH` and optionally `ETCD_URL` / `DILOCK_NAMESPACE`, then run:

```sh
cd sdk/elixir
mix deps.get
mix run ../../docs/examples/elixir-etcd/quickstart/main.exs
```

The namespace defaults to `default`, endpoint to `http://127.0.0.1:2379`, and the script requires a matching pre-existing metadata record.
