# etcd native Lock profile — draft 1

## Transport and lifecycle

Use the official v3 gRPC Lock/Unlock and Lease APIs. Prefer maintained native clients; generating a thin transport binding from official version-pinned protobuf definitions is acceptable when a suitable maintained client is absent. Do not invent an HTTP approximation or silently implement another recipe. Preserve upstream licenses for vendored proto definitions and their transitive imports.

Create a **distinct server lease per logical acquisition**, including concurrent attempts within one process. The native API treats two Lock calls with the same lease as one acquisition; sharing that lease across independent attempts would silently introduce reentrancy. See the [official etcd concurrency API](https://etcd.io/docs/v3.6/dev-guide/api_concurrency_reference_v3/).

Grant lease with requested TTL; retain the effective server TTL. Start managed keepalive while awaiting `Lock(name, lease_id)`. Pass a gRPC deadline/cancellation for the wait budget. Cancellation or failure attempts revoke of that lease with an independent bounded cleanup budget; keep no ghost waiter alive through indefinite keepalive.

## Ownership evidence

On successful Lock RPC, retain its returned **opaque key**. Perform a linearizable Range on that exact key, confirming that it exists and is attached to the expected lease. Capture `create_revision` as the acquisition fencing value. Do not substitute the Lock response header revision, a local counter or `mod_revision` without a reviewed proof. Never modify the native Lock key directly: the etcd API reserves its lifecycle to the lock mechanism.

If verification cannot finish within the current conservative lease budget, do not return an active lease. Abandon/revoke with bounded cleanup and report the accurate failure category. Native waiters need continuous keepalive until acquired or cancelled; a finite wait is not the same as lease lifetime.

## Renewal and release

Keepalive updates the confirmed conservative validity budget, based on request timing and server TTL. Missing/late replies can invalidate local reliance even when the server might still retain the key. Reconnection cannot resurrect a lease declared lost.

Release with native Unlock on the exact returned key and then stop/revoke the acquisition's own lease. Before Unlock, linearly range the exact returned key and verify its lease ID; an Unlock response alone is insufficient because the native RPC may acknowledge when the key is already absent, including expiry between a prior observation and Unlock. Return `Released` only when exact ownership was observed and both Unlock and the positive LeaseRevoke response are acknowledged. If the unique lease is confirmed absent/revoked, or the exact key is confirmed absent/mismatched, return `AlreadyNotOwned`; if ownership or cleanup remains uncertain, return `Unknown`. Use one bounded cleanup budget for the observation and both RPCs. No broad prefix deletes. Revoke cleans up cancelled waiters and orphan attempts; it cannot affect another attempt because leases are never shared. Test Unlock/revoke acknowledgement combinations and the inter-RPC expiry race explicitly, following actual server behavior rather than guessed error codes.

## Fencing and recovery

Expose `create_revision` together with a deployment epoch tied to an approved coordinator incarnation. Document quorum and linearizability assumptions; external effects are not automatically transactional with etcd.

The resource must compare/register the fence atomically. A snapshot restore, cluster replacement or unsafe revision reset needs a coordinated maintenance procedure. Revisions are not an eternal global sequence independent of restore policy. Do not generate a random new epoch at every SDK startup, and do not automatically allow an unknown epoch at the protected resource.

## Required tests

Native queue progress; unique leases per attempt; cancellation removes waiters; waiting longer than one TTL while keepalive operates; same-process competing attempts; return-key validation; create-revision ordering; no-quorum acquisition failure; leader failover; disconnection beyond TTL; stale resource write rejected after a successor registers its fence; restore/migration guard. Use one member only for fast functional tests; use a real three-member cluster for quorum/failover claims.
