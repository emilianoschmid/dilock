# OCaml PostgreSQL adapter

This optional package uses a small C ABI shim and `ctypes.foreign` to expose
the experimental PostgreSQL advisory-session profile through Lwt. Every held
lease owns one private PostgreSQL session; metadata provisioning is explicit,
and the profile has no TTL or generic fencing token.

Build it with the Dilock C ABI and PostgreSQL backend installed. The checked-in
contract test is deterministic, and `test/postgres_live.ml` is an opt-in
two-provider consumer that has passed Dune/ctypes compilation and real
PostgreSQL provision, contention, liveness, release and handoff locally.
Expose the native installation through `LIBRARY_PATH` while linking and
`LD_LIBRARY_PATH` while running. Hosted CI evidence is still pending; this
profile remains experimental.
