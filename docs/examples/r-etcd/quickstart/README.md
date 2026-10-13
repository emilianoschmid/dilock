# R + etcd quickstart

This consumer uses the experimental native etcd gRPC adapter in Dilock's R package (`rgrpc` plus the bundled etcd v3.7.2 protobuf definitions). The code demonstrates the epoch/create-revision fence. This adapter is explicitly not production-supported; the protected resource must atomically enforce the fence.

Prerequisites: R plus the native gRPC/protobuf build requirements in `sdk/r/README.md`, package imports `jsonlite`, `openssl`, `RProtoBuf` and `rgrpc`, a reachable etcd v3 endpoint, and metadata pre-provisioned at `/dilock/namespaces/default/meta` for `DILOCK_COORDINATION_EPOCH`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Dilock does not create or repair metadata.

Install the local package and run from the repository root (POSIX shell):

```sh
R CMD INSTALL sdk/r
DILOCK_RUN_EXAMPLES=1 DILOCK_COORDINATION_EPOCH=local-etcd-epoch Rscript docs/examples/r-etcd/quickstart/main.R
```

The example defaults to target `127.0.0.1:2379` and namespace `default`. Configure endpoint and epoch to match the cluster before running.
