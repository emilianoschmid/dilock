# Dilock for OCaml

The OCaml SDK is an experimental Lwt client for Redis and etcd. A provider is
task-confined, each etcd unary RPC owns a dedicated socket, and a held etcd
lease keeps a separate bidirectional KeepAlive stream until release. Lease
expiry does not stop work already running. Never treat a distributed lock as a
local mutex or as exactly-once execution.

## Redis

The Redis provider borrows a caller-owned Redis-Lwt connection. Keep it private
to one task as required by the client. The default acquisition waits 5 seconds
and requests a 30-second lease. If a Redis command completes after the wait
deadline, the SDK will not return its lease: it attempts owner-checked release,
then reports `Wait_timeout` only when cleanup is acknowledged and
`Acquisition_unknown` otherwise. The wait budget does not interrupt a stalled
Redis-Lwt command, so transport-level timeouts still determine when that call
settles.

Resource names must be strict UTF-8, contain no NUL, and encode to 1–1024
bytes. Invalid, overlong, surrogate, truncated, and out-of-range encodings are
rejected before any Redis command so Redis and etcd derive the same protocol
identity for every accepted name.

An explicit Redis renewal whose response fails or is cancelled may have changed
the server TTL without returning its acknowledgement. That lease is marked
locally `lost` before the original error propagates, and a later renewal is not
sent. Call `release` to attempt owner-checked cleanup; the lease state never
returns to `held`.

Renewal and release share a per-lease mutation gate. A release waits for an
in-flight renewal to settle, and a renewal queued behind a terminal release
does not send another Redis command. Keep one task responsible for each Redis
connection as required by Redis-Lwt.

`with_lease` accepts an optional `~on_release` callback that receives the
terminal `Released`, `Already_not_owned`, or `Unknown` outcome. This makes
best-effort scope cleanup observable without changing the work result. An
observer exception is suppressed; if the work itself raises, that original
exception remains primary even when cleanup fails. A cleanup exception after
successful work still propagates. Cancelling the scoped work does not cancel an
already-started release: the cleanup promise is shielded until its backend
cleanup operation settles, and its outcome is still sent to `~on_release`.

```ocaml
open Lwt.Infix

let run connection =
  Dilock.provider connection "reports" "prod-east-1" >>= fun provider ->
  Dilock.lock provider "daily" >>= fun handle ->
  Dilock.with_lease handle Dilock.defaults (fun _lease -> Lwt.return_unit)
```

## etcd

The etcd provider verifies pre-provisioned namespace metadata before use. The
application must provision the metadata key using the shared metadata contract;
the SDK never creates or changes namespace metadata. The epoch must match the
other clients that share the namespace.

```ocaml
open Lwt.Infix

let run () =
  let options = {
    Etcd.defaults with
    endpoint = "127.0.0.1:2379";
    namespace = "reports";
    coordination_epoch = "prod-east-1";
  } in
  Etcd.provider options >>= fun provider ->
  Etcd.lock provider "daily" >>= fun resource ->
  Etcd.with_lease resource (fun lease ->
    let fence = Etcd.fence lease in
    (* Use the epoch and create_revision in downstream fencing checks. *)
    ignore fence;
    Lwt.return_unit)
```

`Etcd.defaults` targets `127.0.0.1:2379`, waits up to 5 seconds, requests
a 30-second lease and uses 2-second RPC/cleanup timeouts. Always set the
namespace and nonempty coordination epoch for the deployment. For a complete
policy, pass `{ wait_ms; lease_ms }` to `acquire` or `with_lease`; both values
are milliseconds. `acquire`'s wait budget includes lease grant, queueing and
exact-key verification. Each RPC timeout and the shared release-cleanup budget
include DNS resolution, TCP connection establishment and the gRPC exchange.
Native etcd `Lock` has no safe nonblocking mode, so
`try_acquire` returns `Unsupported_capability`.

The `lease` handle stays valid only while its conservative monotonic deadline
has not elapsed and its KeepAlive stream remains healthy. `release` stops that
stream, linearly ranges the exact opaque lock key and verifies its lease ID,
then sends native `Unlock` only for confirmed ownership and attempts
`LeaseRevoke` over a fresh connection. `Released` requires all three positive
observations: exact ownership, Unlock acknowledgement and LeaseRevoke
acknowledgement. Confirmed key absence or lease mismatch, Revoke `NOT_FOUND`,
or a positive Revoke without the full acknowledgement set returns
`Already_not_owned`; unresolved cleanup returns `Unknown`. The observation and
both cleanup RPCs share one bounded cleanup budget. If a
caller cancels during release, the cleanup task continues under its serialized
release gate; a subsequent release observes its terminal result. Cancellation
while waiting for a lock closes that RPC's dedicated socket before attempting
the independent revoke. If revoke is not confirmed, acquisition is classified
as `Acquisition_unknown`.

The etcd `with_lease` scope has the same optional `~on_release` callback and
cleanup-observation behavior as the Redis scope.

## Consul (optional package)

Consul ships as a separate `dilock-consul` package under `consul/`. It uses
the Consul HTTP API with `cohttp-lwt-unix`, and does not add dependencies to
the Redis/etcd package. From `sdk/ocaml`, install the existing `dilock`
package first, then install `consul/` as the optional package:

```sh
opam install .
cd consul
opam install .
```

Provisioning is explicit and regular connect only accepts a linearizable read
of compatible metadata. Keep the same namespace and coordination epoch across
all participants. Consul TTLs are rounded up to whole seconds; supported
leases range from 10 seconds to 24 hours. Fences contain the epoch and the
per-key `LockIndex`. Retain `dilock/locks/*` KV entries: deleting a lock key
can reset its fencing counter. A fence must be atomically enforced by the
protected resource; the lock alone cannot prevent stale external writes.

```ocaml
open Lwt.Infix

let run () =
  let options = {
    Consul.(defaults "prod-east-1") with
    endpoint = "http://127.0.0.1:8500";
    namespace = "reports";
  } in
  Consul.provision_namespace options >>= fun () ->
  Consul.connect options >>= fun provider ->
  Consul.lock provider "daily" >>= fun resource ->
  Consul.with_lease resource options.default_acquisition (fun lease ->
    let fence = Consul.fence lease in
    ignore fence;
    Lwt.return_unit) >>= fun () ->
  Consul.close provider
```

The direct Consul package currently has source-level contract and opt-in live
tests. Set `DILOCK_TEST_CONSUL_ENDPOINT` to enable the single-node lifecycle
runner; optionally set `DILOCK_TEST_CONSUL_NAMESPACE`,
`DILOCK_TEST_CONSUL_EPOCH`, and `DILOCK_TEST_CONSUL_TOKEN`. See
`consul/test/consul_contract.ml` and `consul/test/live_consul.ml`.

The current etcd transport is plaintext HTTP/2 only; configure an appropriately
isolated/trusted endpoint. This preview does not enable TLS, authentication, or
automatic coordinator fallback. Its HTTP/2 headers include the configured
`:authority` because the selected grpc-lwt request hook does not add it
automatically. `create_revision` is the etcd fence component; pair it with the
configured coordination epoch when comparing tokens. The SDK does not claim
that this token by itself fences downstream writes.

Run `dune build` and `dune runtest` from this directory. The protobuf
subset, generation command and upstream license notices are in
[UPSTREAM.md](etcd/proto/etcd-v3.7.2/UPSTREAM.md).
