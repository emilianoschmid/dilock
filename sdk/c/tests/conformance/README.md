# C ABI conformance worker

This worker is test-only and is not installed or shipped with the C ABI. Its
backend operations call only the public functions in `dilock/dilock.h`; C++ is
used for JSONL parsing and concurrent request dispatch because the C ABI does
not define a JSON dependency.

Build it against a fresh installed `Dilock::c` target with this directory's
CMake project. The local `workers.json` entry uses `launch_worker.py` and
requires `DILOCK_C_WORKER_CONTAINER` to identify a task-owned disposable Docker
container with the worker binary and repository mounted read-only at
`/workspace`. Set `DILOCK_C_WORKER_BINARY` if the binary is not at its default
container path, and `DILOCK_C_WORKER_LD_LIBRARY_PATH` if shared libraries are
outside the default loader path. Its default includes the test install prefix
and pinned builder dependency roots. The launcher accepts only repository-local config files and
removes any temporary endpoint-adjusted config after the worker exits.

`test_worker_protocol.py` checks strict JSONL envelopes, malformed and unknown
input handling, the 64 KiB bound, and shutdown. `test_worker_live.py` exercises
the public C ABI worker against explicitly provisioned Redis and etcd test
namespaces. Redis's effective TTL is its exact `PX` duration; etcd's effective
whole-second TTL comes from `dilock_etcd_lease_effective_ttl_ms` and is returned
as `effective_lease_ms`.

The worker does not provision or repair namespace metadata. Its etcd endpoint
must be an `http://` target for this local no-TLS test adapter; this is not a
production TLS example or a claim that the C facade is an independent backend
implementation.
