# ADR 0009: Classify Redis lock keys with unexpected types

## Status

Accepted

## Context

The Redis profile acquires with one atomic `SET key value NX PX ttl_ms`. Redis
returns a null response whenever a key of any type already exists, so a list,
hash, or module-specific value at a Dilock lock key was reported as ordinary
contention. The backend contract also requires protocol/configuration errors to
remain distinct from contention, and unexpected keys must never be removed.

## Decision

After an acknowledged null `SET NX`, each adapter performs one bounded read-only
`TYPE` query. `string` and `none` preserve the acknowledged-busy result; `none`
is possible when the conflicting string expires before the type query. Any
other type returns a protocol mismatch. A failed type query returns a backend
error, never busy. The SDK does not retry acquisition or mutate the existing
key during classification.

The identity, Redis key format, ownership value, acquisition command, lease
lifecycle and wire vectors do not change. This is diagnostic classification
after a confirmed contention response, not a lock grant or fencing mechanism.

## Consequences

- Busy clients pay one extra Redis round trip for each contended attempt.
- Mis-typed lock keys become visible configuration errors and remain intact.
- A string that expires between `SET NX` and `TYPE` still yields busy for that
  attempt; the next caller may acquire on its next explicit attempt.
- Existing golden identity/ownership vectors remain unchanged. All SDKs must
  pass the same real-Redis wrong-type conformance case.
