# Redis single-authority lease profile — draft 1

## Scope

Use a supported Redis server and a maintained client per language. This profile is **not Redlock** and does not run a multi-master majority algorithm. Redis documents the primitive and its replication/failover caveat in its [distributed locks guidance](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

The initial supported claim is limited to a single authoritative keyspace without lock-key eviction, manual deletion or state-losing failover. Sentinel/Cluster routing support is not equivalent to strict mutual exclusion across failover. Default documentation and capability checks must not advertise that stronger guarantee.

Asynchronous-replication promotion can grant the same resource from a stale
replica while the old primary's lease remains live. Do not treat this profile
as safe for strict mutual exclusion across state-losing failover; Redis
documents this limitation in its [distributed locks guidance](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

Production configurations must discuss persistence, restart behavior, `maxmemory` and eviction. Use a dedicated/protected keyspace and a non-evicting policy. No lock implementation can preserve its state if an operator flushes it. `WAIT` alone is not a substitute for a consensus/fencing proof.

## Acquisition

Use the identity/value in `../IDENTITY.md`. Send one atomic `SET key value NX PX ttl_ms`. Confirm success and remaining conservative local validity before returning a lease. A null reply means `SET NX` observed an existing key. Before reporting ordinary contention, issue one bounded read-only `TYPE key`: `string` or `none` keeps the result as busy (the key may have expired after the atomic observation); every other key type is a protocol mismatch and the SDK must leave that key untouched. If `TYPE` fails or times out, surface backend unavailability rather than contention. Never retry `SET` as part of this check. Validate type/protocol/profile failures distinctly; do not erase an unexpected key.

If the response is lost after send, acquisition is unknown. Do not convert a subsequent null response into ordinary contention or assume the key is free. For v0.1 prefer stopping that acquisition, bounded owner-checked cleanup and lease expiry over inventing a complex recovery protocol. Account for a request that executes after cancellation; never hand its stale response to a consumer.

Contended waiting uses bounded jittered polling until the wait deadline. Pub/sub is not required and cannot be the authority for acquisition.

## Renew and release

Canonical Lua files are adjacent. Renewal compares the entire owner value and runs `PEXPIRE` in the same atomic script. Release compares and deletes in the same script. Reject invalid TTLs before calls; the renewal reference script also validates TTL.

Bundle these scripts into each adapter at build time and test their source hash against the canonical file. Handle script-cache misses with a bounded safe reload/re-execution path. Never split GET and DELETE/PEXPIRE into two network operations. Never use unconditional DEL as a cleanup shortcut.

Only one mutating lifecycle operation is in flight per lease. Stop scheduling renewals before release; await/cancel in-flight operations according to bounded cleanup semantics. A delayed renewal or release must never act on a different owner. Terminal local states cannot be revived by a late success.

## No strong fence in this profile

Do not expose an `INCR` counter as a strong cross-failover fence. Ownership tokens are unguessable identifiers, not ordering values. A request requiring monotonic fencing must be rejected for this adapter. Users needing correctness of external writes should use a suitable coordinator/resource-side protocol rather than a reassuring property name.

## Required tests

Atomic acquisition contention; old-owner release/renew rejection; expiry; cancellation before/during/after send; lost response; late command; wrong type; auth failure; busy vs unavailable; script reload; no hidden retry/offline-queue ownership resurrection; eviction/misconfiguration detection where feasible. Demonstrate/document the unsupported failover counterexample rather than marking it “impossible” or filtering it out.
