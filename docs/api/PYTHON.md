# Python API

The Python SDK is implemented for Python 3.11 and newer. It uses native
`asyncio`, structural protocols, immutable options, typed outcomes and
exceptions. Redis and etcd are separate optional dependencies; a provider
selects one configured authority and never fails over to another backend.

## Redis consumer

Install the Redis extra, then use a provider and handle as follows:

```python
import asyncio

from dilock import Options
from dilock.redis import RedisProvider


async def rebuild() -> None:
    async with RedisProvider(
        "redis://127.0.0.1:6379", "catalog", "deployment-2026-01"
    ) as provider:
        lock = provider.lock("catalog/rebuild")
        async with await lock.acquire(wait=5.0, options=Options(lease=30.0)) as lease:
            lease.ensure_held()
            await rebuild_catalog()


async def rebuild_catalog() -> None:
    ...


asyncio.run(rebuild())
```

The equivalent etcd provider is `EtcdProvider(endpoint, namespace, epoch)` from
`dilock.etcd`. Both providers own their network client and are bound to the
event loop on which they are used. Close them with `async with` or
`await provider.aclose()`. A handle can be reused for sequential attempts; it
is not a local mutex and does not make resource work reentrant.

`Options` is frozen. Its durations are seconds: `lease` is the backend TTL,
`rpc` bounds one SDK operation, and `cleanup` bounds ownership cleanup.
`auto_renew` defaults to false. Redis renews only when explicitly enabled;
etcd keeps its temporary lease alive while queued and, when enabled, while
held. The overall `wait` argument to `acquire` is a contention budget, not a
per-attempt delay. Only acknowledged contention is retried. `try_acquire`
returns `None` only for acknowledged contention; etcd cannot provide a native
nonblocking try operation, so this request raises `UnsupportedCapability`.
The wait deadline can expire during namespace preflight or read-only
classification of an acknowledged Redis conflict; those paths raise
`WaitTimeout` once the caller's budget is exhausted. A backend or transport
error observed before that deadline remains `BackendUnavailable`, and an
uncertain dispatched acquisition remains `AcquisitionUnknown`.

Acquisitions return independent leases. Explicitly `await lease.release()` to
inspect `ReleaseOutcome`; leases also implement an async context manager.
`lease.state`, `lease.lost`, and `lease.ensure_held()` are local permission
observations. Loss notification does not stop application work, and the
consumer must check loss and protect external writes with its own atomic
fencing or idempotency mechanism. The etcd profile exposes `(epoch,
create_revision)` as `lease.fence`; Redis does not claim strong fencing.

## Errors and cancellation

- `WaitTimeout` means the contention wait budget elapsed; it is not a backend
  outage.
- `AcquisitionUnknown` means a dispatched acquisition may exist remotely. Do
  not enter protected work or blindly retry on another authority.
- `BackendUnavailable` reports a transport or backend failure.
- `ProtocolMismatch` reports missing or incompatible provisioned namespace
  metadata.
- `UnsupportedCapability` reports a requested backend guarantee that is not
  available.
- `LeaseLost` means local permission was withdrawn.

Cancellation before dispatch remains `asyncio.CancelledError`. If cancellation
arrives after a request may have reached the backend, the SDK performs bounded
cleanup and reports `AcquisitionUnknown` when ownership cannot be established.
Release outcomes distinguish acknowledged release, confirmed loss of
ownership, and unknown cleanup. Cancellation and cleanup never turn an
uncertain result into ordinary contention.

Providers, handles and leases are event-loop bound and are not thread-safe or
transferable between loops. Provider shutdown releases tracked leases with
bounded cleanup and closes its client. Process death bypasses cleanup and
relies on the backend TTL. A lease is not a guarantee of exactly-once effects,
and expiry does not stop a stale process.

Every namespace must be explicitly provisioned with the selected backend
profile, identity scheme and approved coordination epoch before use. SDK
providers refuse missing or incompatible metadata and never create or repair
it. See the [Redis profile](../../spec/backends/REDIS.md) and
[etcd profile](../../spec/backends/ETCD.md) for their distinct guarantees.

For a complete executable Redis consumer, see
[`docs/examples/python-redis/`](../examples/python-redis/README.md). It requires
a real, already provisioned Redis namespace; the small files in
`examples/style/` are syntax references only.
