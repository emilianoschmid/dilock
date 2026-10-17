# Dilock F# consumer

This executable reuses the maintained .NET Redis, etcd and Consul SDKs; it does not implement a second
lock engine. The easy path uses the SDK's `default` namespace and common wait/lease defaults.
Configure `DILOCK_REDIS_URL`, `DILOCK_ETCD_ENDPOINT` or `DILOCK_CONSUL_ENDPOINT`, plus the provisioned
`DILOCK_EPOCH`, to run a backend operation. If both endpoints are configured, set
`DILOCK_BACKEND=redis`, `DILOCK_BACKEND=etcd` or `DILOCK_BACKEND=consul`; the consumer fails closed rather than
silently choosing a coordinator. An explicit backend also requires its matching endpoint.
If a backend is selected without `DILOCK_EPOCH`, the consumer exits before connecting.
Etcd accepts an optional `DILOCK_NAMESPACE` (default `default`). When backend configuration
is absent, the executable performs only its local identity checks.

`Program.fs` checks default/custom policy composition and pre-cancelled connection behavior without
backend access. Its `withAsyncDispose` helper awaits `IAsyncDisposable` and preserves a body or
cancellation exception if cleanup also throws. When configured, it also runs a bounded semantic lifecycle against Redis, etcd or Consul:
two handles share one provider, a queued contender is cancelled, the owner remains held, repeated
release returns the same acknowledged result, and a successor acquires after release. F# task
expressions do not provide C#'s `await using` syntax, so `try/finally` explicitly disposes both
lease and provider. The configured
path uses immutable public options. Consul additionally reads the optional ACL token from
`CONSUL_HTTP_TOKEN` and only accepts TTLs from 10 seconds through 24 hours:

### Quick path: Redis defaults

The endpoint and deployment-approved epoch are required. Namespace `default`,
five-second acquisition wait, and thirty-second lease are the defaults. Metadata
must already be provisioned. F# scopes both resources with `try/finally`:

```fsharp
let runRedis endpoint epoch cancellationToken = task {
    let options = RedisProviderOptions(Endpoint = endpoint, ExpectedEpoch = epoch)
    let! provider = RedisProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> task {
        let! lease = provider.Lock("reports/daily").AcquireAsync(cancellationToken)
        do! withAsyncDispose lease (fun () -> task {
            if lease.State <> LeaseState.Held then invalidOp "Lease is no longer locally held."
            do! rebuildDailyReport()
            let! outcome = lease.ReleaseAsync()
            if outcome <> ReleaseOutcome.Released then
                invalidOp $"Redis release was not acknowledged: {outcome}"
        })
    })
}
```

### Explicit connection and acquisition policy

Set the namespace, connection timeout, total wait, TTL, per-command RPC and
cleanup budgets, and renewal behavior when the deployment needs non-defaults.
This example keeps Redis auto-renew disabled and passes cancellation to both
connection and acquisition:

```fsharp
let runConfiguredRedis endpoint epoch cancellationToken = task {
    let policy = AcquireOptions(
        WaitTimeout = TimeSpan.FromSeconds 12.0,
        Lease = LeaseOptions(
            LeaseDuration = TimeSpan.FromSeconds 45.0,
            RpcTimeout = TimeSpan.FromSeconds 2.0,
            CleanupTimeout = TimeSpan.FromSeconds 3.0,
            AutoRenew = false))
    let options = RedisProviderOptions(
        Endpoint = endpoint,
        Namespace = "billing",
        ExpectedEpoch = epoch,
        ConnectTimeout = TimeSpan.FromSeconds 4.0,
        DefaultAcquisition = policy)
    let! provider = RedisProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> task {
        let! lease = provider.Lock("invoice/42/reconcile").AcquireAsync(policy, cancellationToken)
        do! withAsyncDispose lease (fun () -> task {
            if lease.State <> LeaseState.Held then invalidOp "Lease is no longer locally held."
            do! rebuildInvoice()
            let! outcome = lease.ReleaseAsync()
            if outcome <> ReleaseOutcome.Released then
                invalidOp $"Redis release was not acknowledged: {outcome}"
        })
    })
}
```

The snippets assume `rebuildDailyReport` and `rebuildInvoice` are application
functions returning `Task`. `LeaseState.Held` is only the SDK's conservative
local observation, not a remote ownership check. Lease expiry cannot stop stale
work; use idempotency or resource-side fencing where required. Redis has no
strong fencing token.

The snippets call the consumer-local `withAsyncDispose` helper shown in
`Program.fs`; F#'s `task` expression has no built-in `await using` equivalent.
This keeps disposal asynchronous instead of synchronously blocking in `finally`.
Only set a custom policy when the deployment needs it. Pass the caller's
`CancellationToken` to connection and acquisition; cleanup remains independent
of that token. The example configures `CleanupObserver` so an unacknowledged
scope cleanup is reported as unknown. Unknown cleanup does not prove that the
lock remains or is absent; reconcile the protected resource before assuming it
was released.

Run locally with `dotnet run --project sdk/dotnet/reuse/fsharp/Dilock.FSharp.Consumer.fsproj -c Release`.
