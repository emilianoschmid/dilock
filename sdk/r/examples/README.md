# R quickstarts

These scripts exercise Dilock's actual Redis and etcd adapters. They use the
SDK defaults first, then show explicit wait and lease durations. The Redis
provider borrows a `redux` connection owned by the caller; the etcd provider
owns a gRPC channel and is closed when the script exits.

The namespace metadata must already exist and its coordination epoch must be
provided by the deployment. These examples do not create or repair metadata.
Install Dilock and its dependencies first (from the repository root, run
`R CMD INSTALL sdk/r`). Set `DILOCK_RUN_EXAMPLES=1`,
`DILOCK_COORDINATION_EPOCH`, and the endpoint variables listed below, then run
the scripts from `sdk/r`.

Redis variables: `DILOCK_REDIS_HOST` (default `127.0.0.1`),
`DILOCK_REDIS_PORT` (default `6379`), and `DILOCK_REDIS_NAMESPACE`
(default `default`). Run `Rscript examples/redis-quickstart.R`.

etcd variables: `DILOCK_ETCD_TARGET` (default `127.0.0.1:2379`) and
`DILOCK_ETCD_NAMESPACE` (default `default`). Run
`Rscript examples/etcd-quickstart.R`.

The examples assume disposable or explicitly designated namespaces and use
distinct resource names. Lease expiry does not stop protected R code; use
application-level fencing or idempotency where stale side effects matter.
