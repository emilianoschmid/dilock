# Dilock for Go

The Go SDK implements the draft 1 Redis single-authority and etcd native Lock profiles. It uses `context.Context`, small structs and interfaces, `errors.Is`, and explicit release. Core, Redis and etcd are separate modules. Their `example.invalid/dilock/...` module paths and `v0.1.0` dependency versions are local-only candidates; they are intentionally not resolvable public identities and are not published. Keep `go.work` enabled for repository development. A verified owner-qualified path must replace these candidates before external installation is supported.

## Example

For the common path, configure a provider once and use `AcquireDefault` or `WithLease` on each reusable handle. An omitted namespace defaults to `default`; the epoch remains explicit. `WithLeaseOptions` accepts a complete immutable per-acquisition policy. A successful callback returns nil only when release was acknowledged as `ReleasedOwner`; `AlreadyNotOwned` returns an error matching `ErrLeaseLost`, and unknown cleanup returns an error matching `ErrReleaseUnknown`. Non-released cleanup is also reported through the provider observer (or standard logger). If the callback fails, its error remains primary while the cleanup result is reported. Direct `Acquire` and `TryAcquire` options must explicitly set their lease, RPC and cleanup budgets; only the provider policy normalizes omitted defaults.

For etcd, `CleanupTimeout` is one monotonic budget shared across keepalive shutdown, waiting for an in-flight renewal, native Unlock and lease Revoke during release or abandoned-acquisition cleanup. If an explicit renewal does not finish within that budget, cleanup attempts Revoke without waiting further. The `Release` call's context only bounds how long that caller waits; cleanup continues independently after caller cancellation.

Runnable minimal and configured examples for both adapters are in [Redis quickstart](../../docs/examples/go-redis/quickstart/README.md) and [etcd quickstart](../../docs/examples/go-etcd/quickstart/README.md). They demonstrate two different resource handles sharing one provider. This does not imply atomic multi-resource acquisition; each callback is an independent lease.

```go
provider, err := redis.OpenProvider(ctx, "redis://127.0.0.1:6379/0", redis.ProviderOptions{
    Namespace: "orders",
    Epoch:     "redis-primary-2026-10",
})
if err != nil {
    return err
}
defer provider.Close()

lock, err := provider.Lock("invoice/4821")
if err != nil {
    return err
}
err = lock.WithLease(ctx, func(lease *redis.Lease) error {
    if lease.State() != dilock.Held {
        return dilock.ErrLeaseLost
    }
    // Run the protected operation; use resource-side fencing or idempotency as needed.
    return nil
})
if err != nil {
    return err // includes acquisition errors; scope cleanup uncertainty is reported by the observer.
}
```

`WithLease` uses an independent bounded cleanup context. If you call `Release` directly, inspect both its outcome and error; do not discard either value.

Provision `dilock:namespace:<namespace>:meta` before use with protocol major `1`, identity scheme `sha256-framed-utf8-v1`, profile `redis-single-authority-v1`, the same stable `coordination_epoch`, and `fixed_lease_ms: null`. The SDK never creates or repairs metadata. A metadata mismatch is an error.

## Lifetime and guarantees

`OpenProvider` owns the Redis connection and closes it from `Provider.Close`. `NewProvider` borrows a `RedisClient`; the caller owns that connection and must disable command retries and offline queuing. Providers, handles and leases are safe for concurrent goroutine use. A lease must not be copied after first use. Every acquisition creates an independent owner token. `Close` rejects new work, releases tracked leases with bounded backend cleanup and closes only an owned client.

`TryAcquire` returns `(nil, nil)` only for an acknowledged busy key. `Acquire` retries only that result until its monotonic wait deadline. If Redis acknowledges acquisition just after the wait deadline, Dilock releases that owner using the independent cleanup budget and returns `ErrWaitTimeout` only when cleanup is acknowledged; an unknown cleanup result is `ErrAcquisitionUnknown`. Backend errors, metadata mismatch, unsupported fencing, cancellation and uncertain acquisition remain distinguishable with `errors.Is`. Cancellation after sending `SET NX PX` is reported as `ErrAcquisitionUnknown`; the SDK does not return a lease or convert it to contention, and tracks bounded owner-checked cleanup. A canceled context before dispatch is returned directly.

Use a fresh cleanup context for `Lease.Release`; the acquisition context does not own release. Release withdraws local reliance before cleanup and returns `ReleasedOwner`, `AlreadyNotOwned`, or `ReleaseUnknown`. Renewal is serialized with release; an unconfirmed renewal ends local reliance. `Lost()` closes once but does not stop user goroutines or external work. Process exit can bypass cleanup, so Redis expiry is the fallback. Lease durations are server TTLs; local validity is conservatively measured from request send time with a scheduling margin.

This profile uses one Redis authority, not Redlock. It does not provide a strong fencing token or guarantee exclusivity across asynchronous-replica promotion that loses lock state, key eviction, operator deletion or independent coordinators. External side effects need their own fencing, transactions or idempotency where required. Configure protected, non-evicting lock storage and follow the deployment assumptions in [`spec/backends/REDIS.md`](../../spec/backends/REDIS.md).

## Build and validation

The Redis adapter uses `github.com/redis/go-redis/v9` v9.21.0. The adapter pins a stable client version and disables go-redis command retries for its owned client. Run from this directory (the workspace discovers all four modules):

```powershell
go test ./...
go vet ./...
go -C redis test ./...
go -C redis vet ./...
go -C etcd test ./...
go -C etcd vet ./...
go -C cmd/dilock-go-worker build .
```

The conformance worker is a local tool module that depends on both adapters; it is not a fourth public SDK artifact. The workspace applies local versioned replacements so internal modules resolve without publishing. Isolated consumers use explicit local `replace` directives for these placeholder identities.

Real Redis interoperability requires pre-provisioned namespace metadata and an explicitly configured standalone test authority. No test provisions or modifies shared Redis metadata automatically. From the repository root, launch the conformance worker with `go -C sdk/go/cmd/dilock-go-worker run . <config.json>`; it uses JSON Lines on stdin/stdout.

## etcd native Lock profile

The `example.invalid/dilock/etcd` package uses etcd's official gRPC Lock/Unlock service and v3
Lease/Range APIs. It creates one distinct server lease for each logical
acquisition, keeps queued waiters alive, and verifies the exact opaque key
returned by Lock with a linearizable Range. The exposed fence is the verified
`create_revision` paired with the configured stable epoch. Fencing only works
when the protected resource atomically registers and rejects stale values; an
etcd snapshot restore or coordinator replacement requires a coordinated epoch
and resource-watermark procedure.

The provider borrows both clients. Build the v3 KV/Lease client and a Lock RPC
over a gRPC connection configured for the same cluster, then close the
provider before closing either client:

```go
client, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://127.0.0.1:2379"}})
if err != nil {
    return err
}
defer client.Close()
lockRPC, err := etcd.NewGRPCLockRPC(client.ActiveConnection())
if err != nil {
    return err
}
provider, err := etcd.NewProvider(client, lockRPC, etcd.ProviderOptions{
    Namespace: "orders",
    Epoch:     "prod-eu1-etcd-incarnation-1",
})
if err != nil {
    return err
}
defer provider.Close()
```

The namespace metadata at
`/dilock/namespaces/<namespace>/meta` must be provisioned explicitly with
profile `etcd-native-lock-v1`; normal acquisition never creates or repairs it.
Configure gRPC TLS and authentication on the supplied connection. A cancelled
waiter is revoked with an independent bounded cleanup context. `TryAcquire`
uses `RPCTimeout` as its maximum queue wait and returns busy only after revoking
its unique lease and confirming a competing native waiter still exists; an
uncertain timeout remains an error. `Acquire` uses its explicit wait duration.
Auto-renewal keeps the confirmed conservative local validity budget updated;
it cannot prevent a process pause or make external effects transactional with
etcd. A local lease is cooperative, not a local mutex or exactly-once guard.

The official native Lock client binding is exposed from etcd's
`server/v3/etcdserver/api/v3lock/v3lockpb` module. The adapter deliberately
does not use `client/v3/concurrency.Mutex`, which is a separate client-side
transaction/watch recipe. The selected etcd client and binding are pinned to
v3.7.2 for the current local etcd 3.7.2 functional target.
