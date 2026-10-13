# Go Redis public API consumer

This console consumer uses two independent `dilock/redis` providers against one pre-provisioned Redis authority. It checks acknowledged contention, a bounded contention wait, TTL expiry, successor acquisition, and owner-checked stale cleanup. It sends real Redis commands; it does not emulate a distributed lock or provision a Redis service.

## Prerequisites and namespace provisioning

- Go 1.26 or newer and a Redis authority reachable from this machine.
- An operator-provisioned namespace with profile `redis-single-authority-v1`, identity scheme `sha256-framed-utf8-v1`, the chosen `coordination_epoch`, and `fixed_lease_ms: null`.
- A Redis endpoint supplied through `DILOCK_REDIS_URL`, plus matching `DILOCK_NAMESPACE` and `DILOCK_EPOCH` values.

The required metadata key is `dilock:namespace:<namespace>:meta`; its JSON fields are `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "redis-single-authority-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The SDK never creates or repairs this record. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md) and [`spec/backends/REDIS.md`](../../../spec/backends/REDIS.md) for the protocol and deployment assumptions. If metadata must be provisioned, do it separately with an operator-approved provisioning tool. A Redis `SET ... NX` operation can prevent overwriting an existing record; inspect an existing key rather than replacing it.

Keep credentials out of this source tree and command history. The Redis URL can contain credentials; the SDK avoids logging the endpoint, but shell history and process inspection are outside its control. Use environment injection or a secret manager. Run this short-TTL demonstration only against a non-production namespace.

## Build and run

From the repository root, the example's local `replace` directive uses this checkout's Go SDK and does not access a package registry:

```powershell
$env:GOTOOLCHAIN = "local"
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
go -C docs/examples/go-redis build -o ..\..\..\.tools\go-redis-example.exe .
```

Set the endpoint and already provisioned identity, then run:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
.tools/go-redis-example.exe
```

Expected output:

```text
contender: acknowledged busy
contender: bounded wait ended with ErrWaitTimeout
contender: acquired after Redis TTL
original owner: stale release left successor untouched
contender: successor release acknowledged
```

This directory is a source consumer using a local module replacement. It is not evidence that a versioned Go module artifact has been published or independently installed.

## Lifetime, timeout, cancellation, and errors

`OpenProvider` owns its Redis connection; `Provider.Close` is deferred for each provider. The two providers have separate connections. A reusable handle creates independent acquisition generations. Every successful lease is explicitly released with a fresh bounded cleanup context; a process exit can bypass cleanup, so the server TTL is the fallback. The sample disables auto-renew and uses a three-second server TTL.

`TryAcquire` returns `(nil, nil)` only for an acknowledged busy key. The example's `Acquire` wait retries only that result and returns `dilock.ErrWaitTimeout` when its contention budget expires. Other failures remain errors: callers can use `errors.Is` for categories such as `dilock.ErrProtocolMismatch`, `dilock.ErrBackendUnavailable`, `dilock.ErrUnsupported`, `dilock.ErrProviderClosed`, `dilock.ErrAcquisitionUnknown`, and `dilock.ErrWaitTimeout`. Cancellation before dispatch is returned as a context error. If a dispatched `SET NX PX` has no reliable acknowledgement, acquisition is `ErrAcquisitionUnknown`, not busy; the SDK schedules bounded owner-checked cleanup. Do not begin protected work without a returned lease.

Release requires its own cleanup context and withdraws local reliance before attempting owner-checked deletion. A release outcome can be `ReleasedOwner`, `AlreadyNotOwned`, or `ReleaseUnknown`; an error or unknown outcome is not proof that the key was removed. Lease state and `Lost()` are local observations only. Expiry and `Lost()` cannot stop stale goroutines or external side effects, and this Redis single-authority profile does not provide strong fencing or failover guarantees. Use application-level fencing, transactions, or idempotency where the protected resource requires them. This example is not a production-readiness claim.
