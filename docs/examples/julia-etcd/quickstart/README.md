# Julia + etcd quickstart

This consumer uses Dilock's experimental Julia adapter with native etcd v3 Lock and Lease RPCs over gRPCClient. It sequentially locks two resources on one provider, first with defaults and then with explicit 3-second wait / 20-second lease settings plus automatic keepalive. It prints each etcd epoch/revision fence. The protected resource must atomically enforce that fence.

Prerequisites: Julia 1.10+, dependencies from `sdk/julia/Project.toml`, a reachable etcd v3 endpoint, and pre-provisioned metadata at `/dilock/namespaces/billing/meta` matching `DILOCK_COORDINATION_EPOCH`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Dilock only reads and validates this record.

From the repository root:

```sh
julia --project=sdk/julia -e 'using Pkg; Pkg.instantiate()'
julia --project=sdk/julia docs/examples/julia-etcd/quickstart/main.jl
```

Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_COORDINATION_EPOCH`, and optionally `DILOCK_ETCD_TOKEN` for the prepared environment. The sample's namespace is `billing`.
