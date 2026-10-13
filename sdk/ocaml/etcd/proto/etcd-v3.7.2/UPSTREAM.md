# etcd v3.7.2 protobuf sources

These focused schemas are derived from the official etcd v3.7.2 sources:

- `rpc.proto`: <https://github.com/etcd-io/etcd/blob/v3.7.2/api/etcdserverpb/rpc.proto>
- `kv.proto`: <https://github.com/etcd-io/etcd/blob/v3.7.2/api/mvccpb/kv.proto>
- `v3lock.proto`: <https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3lock/v3lockpb/v3lock.proto>

The checked-in proto files retain only the fields and RPC definitions Dilock
uses: `KV/Range`, `Lease/LeaseGrant`, `Lease/LeaseRevoke`, bidirectional
`Lease/LeaseKeepAlive`, and `Lock/Lock` plus `Lock/Unlock`. Field numbers and
wire types are preserved. The source modules are licensed under Apache-2.0;
the applicable upstream notices are included as `LICENSE-etcd-api` and
`LICENSE-etcd-server` and installed with the schemas.

Generated OCaml files in `sdk/ocaml/lib/etcd_generated` are derived from these
schemas with `ocaml-protoc-plugin` 4.4.0. Their first-line attribution points
back to this notice and the adjacent license texts. Regenerate from this
directory with `protoc` 3.21.12 or compatible and the plugin on `PATH`:

```sh
protoc -I . \
  --ocaml_out='int64_as_int=false;int32_as_int=false:../../../lib/etcd_generated' \
  kv.proto rpc.proto v3lock.proto
```

The options preserve protobuf `int64` values as OCaml `int64`; do not switch to
native OCaml `int` conversion because etcd revisions and lease identifiers are
64-bit protocol values.
