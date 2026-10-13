# Go etcd public API consumer

This consumer uses the caller-owned etcd v3 client and the native `Lock` gRPC adapter. Two Dilock providers contend on a unique resource; the example expects the bounded waiter to report `ErrWaitTimeout`, releases the owner, and checks that the successor fence advances within the same configured epoch. Go's etcd adapter borrows the client and lock stub, so the providers must close before the client.

Provision namespace metadata separately before running: `/dilock/namespaces/<namespace>/meta` must contain `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. This executable only reads metadata. Resource-side fencing must atomically check/register both epoch and `create_revision`; expiry does not stop stale application work.

The source imports the repository's current Go package names (`dilock` and
`dilock/etcd`). The standalone sample uses a local `replace` and runs against
this checkout; it is not an installed-module example. Replace the local module
reference with the verified owner-qualified path before documenting external
installation.
