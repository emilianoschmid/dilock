# R + Redis quickstart

This consumer uses Dilock's experimental R package with a caller-owned `redux` hiredis connection. The package uses Redis `SET NX PX` and its packaged owner-check Lua scripts; it does not install a separate Redis service. This Redis profile has no strong fencing guarantee.

Prerequisites: R, the native build prerequisites documented in `sdk/r/README.md`, package imports (`jsonlite`, `openssl`, `RProtoBuf`, `rgrpc`) and the suggested `redux` package, a reachable Redis server, and pre-provisioned metadata matching the selected namespace and epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Runtime setup does not create metadata.

Install the local package and run from the repository root (POSIX shell):

```sh
R CMD INSTALL sdk/r
DILOCK_RUN_EXAMPLES=1 DILOCK_COORDINATION_EPOCH=local-redis-epoch Rscript docs/examples/r-redis/quickstart/main.R
```

The source defaults to namespace `default`; set the epoch to the value already provisioned there. The environment guard keeps the example inert unless explicitly enabled.
