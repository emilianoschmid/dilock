# ADR 0001: Initial runtime profile and bounded cleanup

Date: 2026-10-03. Status: accepted for implementation, not release approval.

Keep draft 1 identity, metadata, Lua and ownership bytes unchanged. Redis uses
one configured authority, SET NX PX and the canonical compare-and-mutate scripts.
No client-side acquisition retry or offline queue is allowed. Every acquisition
has a new cryptographic owner. Namespace metadata is read before acquisition;
only an explicit administration tool creates it using create-if-absent.

The first slice supports explicit renewal and optional managed auto-renewal.
Validity starts before sending the request and subtracts a margin of
max(10 ms, 1 percent of the effective TTL). TTL is limited to one day, and the
RPC budget must fit inside the conservative TTL. A local deadline only withdraws
permission; it does not stop a process. Late responses cannot revive leases.

Cancellation after dispatch is conservative: acquisition is unknown until
bounded, owner-specific cleanup is attempted. Cleanup uses a fresh budget and
is tracked by the provider. An orphan can survive until TTL expiry. Redis
fencing requests fail explicitly. No fallback backend exists.

For etcd use official native gRPC Lock/Unlock and distinct Lease IDs. Generated
bindings must come from pinned, licensed upstream protobuf sources. Do not ship
a placeholder etcd adapter or substitute Redis/HTTP for this protocol.

Package/runtime versions and support evidence are independent. Local packages
may use unverified publisher identifiers only as explicitly marked staging
identifiers; there is no authorized publication in this task.
