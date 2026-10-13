# Dilock — Dart

This directory contains the Dart shared contract, Redis adapter and etcd
adapter. The local syntax sample in `../../examples/style/dart/` is only a
design reference and does not implement distributed locking.

The Redis package implements canonical draft 1 identity, validates
pre-provisioned namespace metadata, bundles owner-checked Lua commands, and
distinguishes acknowledged contention from ambiguous acquisition. It uses a
bounded provider-owned connection pool; creating a resource handle does not
open a new connection. Redis failover does not provide strong fencing.

The etcd adapter implements etcd's native Lock, LeaseGrant/Revoke, bidirectional
LeaseKeepAlive, exact-key Range verification, and epoch-scoped create-revision
fencing. Generated etcd v3.7.2 gRPC stubs are included. Run `dart analyze`,
`dart format`, and `dart test` before using a build. A substitute transaction
protocol would not interoperate with `etcd-native-lock-v1`. See
[`etcd/README.md`](etcd/README.md) for details.

Run `dart pub get --offline`, `dart analyze`, `dart test`, and `dart doc` in
each implemented package. An exact etcd installed-package consumer and external
registry resolution remain open. No package has been published; broader
conformance, distributed-fault and release artifact evidence remain required.

