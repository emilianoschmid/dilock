# PostgreSQL advisory-session profile — draft 1

## Scope

This profile uses PostgreSQL session-level advisory locks. It is intended for
one logical acquisition per private database connection and does not use
transaction-level advisory locks. A connection pool must not be used as the
owner of a held lease unless it can guarantee that the same physical session
remains reserved for the entire lease.

The database is the coordination domain. Every participant must use the same
PostgreSQL database, namespace metadata row, profile and deployment policy.
PostgreSQL does not provide a TTL for advisory locks: a live session retains
the lock until `pg_advisory_unlock` or session termination. A process crash or
network failure therefore relies on PostgreSQL detecting and closing the
session. This is cleanup behavior, not fencing and not a guarantee that stale
application work stops.

## Identity and metadata

The lock key is a signed 64-bit value derived from the first 16 hexadecimal
characters of the draft-1 resource identity, interpreted as big-endian two's
complement. All SDKs must implement this conversion identically. The profile
therefore has the residual collision assumption of a 64-bit digest prefix;
applications requiring a stronger collision boundary must use a different
profile or enforce a separate resource-side identity registry.

An operator provisions this table and its row before clients start:

```sql
CREATE TABLE dilock_namespace_meta (
    namespace text PRIMARY KEY,
    protocol_major integer NOT NULL,
    identity_scheme text NOT NULL,
    profile text NOT NULL,
    coordination_epoch text NOT NULL,
    fixed_lease_ms bigint NULL
);
```

The row must use profile `postgres-advisory-session-v1`, identity scheme
`sha256-framed-utf8-v1`, protocol major `1`, the approved nonempty epoch and a
null `fixed_lease_ms`. Runtime clients read and validate the row; they never
create, repair, overwrite or delete it.

## Acquisition and lifecycle

For each logical attempt, open one private session and execute:

```sql
SELECT pg_try_advisory_lock($1::bigint);
```

`true` is a grant and `false` is ordinary contention. Waiters use a bounded
monotonic polling budget. A cancelled or timed-out attempt closes its private
session. If a mutation response is lost, the outcome is unknown even though a
best-effort close is attempted; the client must not enter protected work.

Release executes `pg_advisory_unlock` on the same session and then closes it.
An acknowledged `true` is `Released`; `false` is `AlreadyNotOwned`. Any
unacknowledged unlock or close is `Unknown`. There is no unconditional unlock
from another session and no force-unlock API.

`renew` is a liveness check only. It may issue `SELECT 1` on the owning
session, but it cannot extend a lease or provide a fence. A failed check marks
the local lease lost. `require_fencing=true` is rejected before backend I/O.

## Required evidence

Required tests include metadata missing/mismatch, same-resource contention,
different-resource progress, same-process non-reentrancy, bounded timeout and
cancellation, lost acquisition/unlock acknowledgements, session cleanup after
process failure, pool/session misuse, auth failure, old-session release
rejection, and cross-SDK contention using the same PostgreSQL database.

A single PostgreSQL instance proves functional advisory-lock behavior only. It
does not prove failover, fencing, exactly-once effects, or production recovery.
Report the PostgreSQL server and client versions used for every support claim.
