# Consul HTTP/KV session profile — draft 1

Status: design target; support is per SDK and requires real-server evidence.

## Transport and scope

Use Consul's HTTP API through a configured agent endpoint. SDKs use their native
HTTP stack; no language-specific Consul SDK is required. The agent must forward
requests to the intended datacenter. Never use stale or cached reads to confirm
ownership. Requests do not automatically follow redirects or retry mutations.
HTTPS uses system trust roots; ACL tokens are sent in `X-Consul-Token`, never in
URLs, exceptions, logs, or worker output.

See the official [KV HTTP API](https://developer.hashicorp.com/consul/api-docs/kv)
and [session HTTP API](https://developer.hashicorp.com/consul/api-docs/session)
for version-specific request details.

This profile uses KV session locks, not Consul's CLI `consul lock` command. The
namespace metadata key is `dilock/namespaces/<namespace>/meta`; the lock key is
`dilock/locks/<resource_id>` as defined in `../IDENTITY.md`. Provision metadata
explicitly before use. Its exact shape is:

```json
{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"consul-kv-session-v1","coordination_epoch":"<approved epoch>","fixed_lease_ms":null}
```

Ordinary acquisition never creates or repairs metadata. Provision with KV CAS
`cas=0`; on an existing entry, verify exact semantic equality and leave it
untouched. Read it with a linearizable KV read and compare every field.

## Session and acquisition lifecycle

Create one new Consul session for each logical acquisition attempt with
`Behavior=release`, `LockDelay=0s`, an explicit TTL, and no health checks. TTL
must be between 10 seconds and 24 hours; practical deployments should stay at
or below one hour. Round the requested lease upward to whole seconds and return
that effective duration. Requests below 10 seconds or above 24 hours fail
before network I/O. TTL sessions require explicit renewal. While waiting for a
lock, renew the same attempt's session so a long wait does not orphan its
waiter. Never share a session across attempts.

Acquire with `PUT /v1/kv/<lock-key>?acquire=<session-id>` and a fixed empty
payload. Only an acknowledged JSON `true` is a grant; acknowledged `false` is
contention. After a grant, perform a linearizable KV read and require the
returned `Session` to equal the attempt session and `LockIndex` to be positive.
If the read is missing, malformed, late, or names another session, do not hand
out a lease. A request whose mutation outcome is not acknowledged is unknown;
do not call it busy or retry as a new acquisition. Attempt bounded cleanup by
releasing the same session and destroying it.

Use a monotonic conservative deadline from the session-create or session-renew
request-send time and effective TTL, minus the shared safety margin. A current
ownership read or KV acquire alone does not refresh that deadline. Session renewals run only while waiting
or held and are serialized against release. Renewal first renews the session,
then linearly confirms the lock is still held by the same session with the same
`LockIndex`; it cannot reacquire or revive a terminal lease. Release sends
`PUT /v1/kv/<lock-key>?release=<session-id>`. An acknowledged `true` is
`Released`; acknowledged `false` is followed by a linearizable read to
distinguish `AlreadyNotOwned` from uncertainty. Destroy the attempt session
after release or abandonment using an independent bounded cleanup budget.

The HTTP API is advisory. The lock does not stop a client that writes to some
other resource without checking a fence. ACLs must restrict session creation,
renewal, destruction, lock-key read, acquire, and release to the intended
namespace and prefix. Never send a management token to a user-controlled
endpoint. Use one Consul agent/datacenter and one namespace/ACL view for all
participants.

## Fencing and key retention

Expose `(approved coordination epoch, LockIndex)` as the fence. Consul
increments `LockIndex` on successful acquisition and preserves it on release.
The adapter verifies and returns the index from the read after acquisition; a
resource must atomically register/enforce the fence before relying on external
side effects. `LockIndex` is per KV key, not a global sequence.

Consul permits arbitrary KV writes and deletes outside the lock primitive. The
profile therefore requires operators to retain lock entries: deleting and
recreating a lock key can reset its `LockIndex`. Do not provide an SDK delete
operation or automatic cleanup of lock entries. Fence safety is conditional on
that operational invariant and the approved epoch. Snapshot restore, datacenter
replacement, manual key deletion, or epoch changes require coordinated
maintenance and resource-side fence migration. The advisory lock API is not an
authorization boundary against clients with broader ACLs.

## Guarantees and required evidence

No FIFO fairness or immediate expiry-to-successor timing is promised. Consul
may take up to twice a session TTL to reap forcibly expired sessions; a local
lease expires conservatively at its own deadline, while a successor may wait
longer. A zero lock delay avoids adding another intentional delay but does not
make Consul enforce a lease at the protected resource. Quorum loss, server
restoration, and operator mutation are backend-profile boundaries.

Required tests include: metadata missing/mismatch and CAS conflict; session TTL
bounds and rounding; busy versus unavailable; same-session validation;
LockIndex monotonicity and retained-key behavior; timeout/cancellation and lost
acquire/release acknowledgements; session renewal while waiting and held; stale
owner renewal/release; session expiry and cleanup; auth failure; response
filtering/ACL behavior; and cross-SDK contention. A single-node Consul instance
validates only functionality, not quorum or failover. Pin and report the real
Consul and client versions used for each support claim.
