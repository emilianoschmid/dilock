# Dilock semantic contract — draft 1

Status: normative target for the initial implementation, subject to reviewed corrections before the first release. MUST/MUST NOT/SHOULD express project requirements, not a claim that a runtime already implements them.

## Scope and assumptions

Exclusive, non-reentrant named-resource leases. No reader/writer locks, semaphores, multi-key atomic acquisition, distributed transactions, leader service, exactly-once execution or force-unlock API in v0.1. Participants are cooperative but may crash, pause or lose connectivity. Coordinator storage deletion, evictions, failover semantics and snapshot restoration are backend-specific assumptions, not hidden details.

A lease is a permission observed under a backend profile, not a capability to stop stale processes. External effects require resource-side serialization, transactions, idempotency and/or fencing as appropriate.

## Identity

Use `IDENTITY.md`. The same logical lock is the tuple `(coordination domain, namespace, resource bytes)`. SDK language and package version are NOT part of that identity. Protocol migration MUST NOT quietly place old and new clients in independent key spaces.

## Acquisition

`acquire` waits up to an explicit wait budget. Success returns a new lease only after positive ownership evidence and a remaining conservative validity budget. `try_acquire` performs one logical attempt without waiting for contention; it may still take a bounded network round-trip.

Ordinary contention is the only null/None/no-value result of try-acquire. Wait timeout, cancellation, backend unavailable, unsupported capability, configuration error, protocol mismatch and unknown acquisition outcome are distinguishable. SDKs map them to native errors/exceptions. Local validation must precede network calls where possible.

If a request may have committed but its response is lost, do not enter user work, do not return “busy” and do not blindly retry as a new owner. Return/raise acquisition-outcome-unknown (or native cancellation with an explicit outcome-unknown annotation), cancel pending work and attempt bounded owner-specific cleanup. An orphan may remain until its lease expires. A timeout of a local future does not cancel an already queued remote command.

Disable or account for client offline queues and hidden retries. A delayed old acquisition must never be surfaced as a new live lease. Use new unpredictable ownership identity for every new logical acquisition generation, with retries only under the backend's explicitly specified idempotence rules.

## Durations and clocks

Use native duration types publicly; serialize integer milliseconds for conformance. Default targets: wait 5 s, lease 30 s, RPC 2 s, cleanup 2 s. These are starting defaults, not universal tuning advice. The SDK must validate finite positive durations, overflow, server granularity and an RPC budget compatible with the lease. Infinite TTL is not supported.

Conservative local expiry is derived from the monotonic **request-send time**, effective server lease duration and a safety margin, never from the response-receive time plus TTL. A response received after the local deadline cannot authorize work. Renewal uses the same principle. Account for timer resolution, scheduling delay and clock drift assumptions; no fixed margin solves arbitrary suspension or clock behavior. Wall-clock timestamps are diagnostics only.

Native etcd waiting may continue longer than the originally granted TTL: keepalive must run while waiting, and the current confirmed lease budget must be validated before returning ownership. The requested and effective TTL may differ; report the latter.

## Lease state

```text
Held -> Releasing -> Released
Held -> Lost
Releasing -> ReleasedUnknown
Releasing -> Lost
```

Terminal states never return to Held. `Lost` means no longer safe to rely on local ownership, even if the server might still hold an orphan. Begin release by stopping new user reliance and serialized renewal; state moves before sending cleanup. Unknown release acknowledgement is not success. Repeated cleanup is safe and returns the cached terminal observation or a documented more precise acknowledgement without reactivating the lease.

Loss notification fires once with a reason: confirmed not-owner, conservative deadline elapsed, provider shutdown or unrecoverable profile violation. Temporary transport failure can be retried only within the current confirmed budget. Notification must not claim to interrupt an OS thread or forcibly cancel arbitrary external I/O.

## Renewal

Auto-renewal may be the default for supported profiles, but it has a bounded lifetime controlled by the lease/provider. No renewal after terminal state, no reacquisition disguised as renewal, no extending a lease now owned by someone else. Loss of certainty ends the lease locally. All mutating renewal/release operations are serialized per acquisition. Test late responses and request reordering explicitly.

## Release

Release only with the acquisition's ownership evidence, never `delete(resource)` unconditionally. Explicit outcomes: `Released` (acknowledged removal), `AlreadyNotOwned` (nothing belonging to this acquisition remains), `Unknown` (no acknowledgement). Public convenience disposal makes a bounded best effort and emits diagnostics; it does not claim remote deletion and must not mask an exception from the user's work.

Release must use its own bounded cleanup budget, not an already-cancelled acquisition token. Cleanup tasks must remain tracked. A process crash bypasses all destructors/finally blocks; backend expiry is the fallback, under the profile's assumptions.

## Fencing

Owner tokens prevent deleting another acquisition. They are NOT fencing tokens. A fence must order acquisitions and be checked atomically by the protected resource. An etcd creation revision is usable only within its managed epoch; Redis v0.1 declares no strong monotonic fence.

The resource stores an approved epoch and a high-watermark. A lower fence is rejected; equal fences may support repeated operations of the same acquisition, with separate operation IDs for idempotency. A newly acquired fence must be registered/enforced by the resource before relying on exclusion there. Fencing does not magically reject all old writes the instant a lease expires; rejection begins when the resource observes a later authorized fence (or applies its own validity rules).

Changing/restoring a coordinator requires an explicit epoch/migration procedure. Do not let arbitrary new epoch strings bypass the resource's high-watermark. Acquisition revision is not the completion order of arbitrary side effects.

## Fairness and availability

No global FIFO guarantee in the common contract. Notifications/watches are wake-up hints unless the backend specifies stronger semantics. Backoff uses bounded jitter to reduce contention, not a promise of starvation freedom. Under partitions/quorum loss, preserving a documented safety profile may require failing to acquire; the library does not silently trade that profile for availability.
