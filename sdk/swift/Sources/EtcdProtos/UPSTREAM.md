# etcd v3.7.2 gRPC definitions

These focused proto files preserve the wire names and field numbers used by
Dilock from the official etcd v3.7.2 sources:

- `go.etcd.io/etcd/api/v3@v3.7.2/etcdserverpb/rpc.proto`
- `go.etcd.io/etcd/api/v3@v3.7.2/mvccpb/kv.proto`
- `go.etcd.io/etcd/server/v3@v3.7.2/etcdserver/api/v3lock/v3lockpb/v3lock.proto`

Only the Range, LeaseGrant, LeaseRevoke, LeaseKeepAlive, Lock, and Unlock
services and the fields Dilock reads are included. The omitted fields have no
effect on protobuf wire compatibility. The source module licenses are retained
in `LICENSE-etcd-api` and `LICENSE-etcd-server`.

The corresponding SwiftProtobuf and gRPC Swift sources are checked in under
`Generated/` so a package consumer can build without running SwiftPM generator
plugins. They were generated with SwiftProtobuf 1.38.1 and grpc-swift-protobuf
2.1.1 from the schemas above. When changing a schema or generator version,
regenerate all six files and verify the package with a clean consumer build.
The `.proto` and generator configuration files remain here as the editable
source of truth.
