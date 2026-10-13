# OCaml + Redis quickstart

This example is an OCaml/Lwt consumer of Dilock's actual Redis adapter, using a caller-owned Redis-Lwt connection. It acquires two handles sequentially through one provider, with defaults and then an explicit 3-second wait / 20-second lease. The OCaml SDK is experimental. Redis does not provide strong fencing in this profile.

Prerequisites: OCaml 4.14+, opam and Dune 3.8+, the dependencies declared by `sdk/ocaml/dilock.opam`, a reachable Redis server, and pre-provisioned Redis namespace metadata for `default` and `local-redis-epoch`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The client will not provision metadata.

Install the SDK in the active opam switch, then build and run this standalone Dune consumer from the repository root:

```sh
opam install ./sdk/ocaml
dune exec --root docs/examples/ocaml-redis/quickstart ./main.exe
```

Set `DILOCK_REDIS_HOST`, `DILOCK_REDIS_PORT`, `DILOCK_NAMESPACE`, and `DILOCK_COORDINATION_EPOCH` to match the existing namespace record. The sample disconnects its borrowed connection after the lease scope finishes.
