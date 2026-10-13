# OCaml + etcd quickstart

This is an OCaml/Lwt consumer of Dilock's experimental adapter using native etcd v3 Lock, Lease and Range RPCs. It acquires two sequential resource handles on one provider, first with defaults and then with an explicit 3-second wait / 20-second lease. The sample reports the epoch-scoped creation-revision fence. The protected resource must compare the fence atomically; lease expiry does not stop OCaml work.

Prerequisites: OCaml 4.14+, opam and Dune 3.8+, the dependencies declared by `sdk/ocaml/dilock.opam`, a reachable etcd v3 endpoint, and metadata at `/dilock/namespaces/default/meta` for `local-etcd-epoch`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The adapter only validates the record.

Install the SDK in the active opam switch, then build and run this standalone Dune consumer from the repository root:

```sh
opam install ./sdk/ocaml
dune exec --root docs/examples/ocaml-etcd/quickstart ./main.exe
```

Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_COORDINATION_EPOCH` to match your namespace. Native etcd Lock queues waiters; the SDK does not offer nonblocking try-acquire for this backend.
