# etcd v3.7.2 gRPC definitions

The focused protocol definitions in this directory are taken from the matching
Dilock Swift SDK copy. Their upstream paths, field compatibility scope and
retained source licenses are documented in `../../../swift/Sources/EtcdProtos/UPSTREAM.md`.
They cover only etcd v3.7.2 Range, LeaseGrant, LeaseRevoke, LeaseKeepAlive,
Lock and Unlock. These source files alone are not an etcd adapter; generated
Elixir bindings and a runtime integration must pass before that backend is
advertised.
