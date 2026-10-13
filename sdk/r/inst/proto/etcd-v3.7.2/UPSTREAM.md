# etcd v3.7.2 gRPC definitions

These focused proto files preserve the wire names and field numbers used by
Dilock from the official etcd v3.7.2 sources:

- `go.etcd.io/etcd/api/v3@v3.7.2/etcdserverpb/rpc.proto`
- `go.etcd.io/etcd/api/v3@v3.7.2/mvccpb/kv.proto`
- `go.etcd.io/etcd/server/v3@v3.7.2/etcdserver/api/v3lock/v3lockpb/v3lock.proto`

Only the Range, LeaseGrant, LeaseRevoke, LeaseKeepAlive, Lock, and Unlock
services and the fields Dilock reads are included. The test fixture also uses
the official KV `Put` RPC solely to provision its local namespace metadata when
it is absent; the SDK never provisions or repairs metadata. The omitted fields
have no effect on protobuf wire compatibility. The source module licenses are
retained in `LICENSE-etcd-api` and `LICENSE-etcd-server`.
