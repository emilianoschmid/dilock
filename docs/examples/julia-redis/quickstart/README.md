# Julia + Redis quickstart

This consumer uses the Dilock Julia Redis adapter with Redis.jl. It borrows one caller-owned Redis connection and sequentially locks two resources, using defaults first and then an explicit 3-second wait / 20-second lease. The Julia SDK is an experimental preview; Redis's single-authority profile does not provide strong fencing.

Prerequisites: Julia 1.10+, dependencies from `sdk/julia/Project.toml`, a reachable Redis server, and namespace metadata already provisioned for the configured namespace and epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The adapter validates metadata and does not create it.

From the repository root:

```sh
julia --project=sdk/julia -e 'using Pkg; Pkg.instantiate()'
julia --project=sdk/julia docs/examples/julia-redis/quickstart/main.jl
```

The source defaults to local Redis and a local epoch; edit it to match the pre-provisioned metadata before running. It disconnects the borrowed Redis connection after release.
