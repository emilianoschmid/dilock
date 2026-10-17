# Dilock Visual Basic .NET consumer

This executable consumes the existing .NET Redis, etcd and Consul SDKs rather than maintaining a separate
lock implementation. Its easy path uses the provisioned `default` namespace and the common
five-second wait / thirty-second lease policy. For Consul, provision draft metadata first and set optional `CONSUL_HTTP_TOKEN` for ACLs.
Configure one of `DILOCK_REDIS_URL`,
`DILOCK_ETCD_ENDPOINT` or `DILOCK_CONSUL_ENDPOINT`, plus `DILOCK_EPOCH`, to run a live backend operation.
If multiple endpoints are configured, set `DILOCK_BACKEND=redis`, `DILOCK_BACKEND=etcd` or
`DILOCK_BACKEND=consul`; the consumer fails closed
rather than silently choosing a coordinator. An explicit backend also requires its matching
endpoint. If a backend is selected without `DILOCK_EPOCH`, the consumer exits before connecting.
Etcd accepts an optional `DILOCK_NAMESPACE` (default `default`). Without backend settings
it performs only its local identity checks.

The consumer checks default/custom policy composition and pre-cancelled connection behavior without
backend access. Its `WithAsyncDisposeAsync` helper awaits `IAsyncDisposable` and preserves a body or
cancellation exception if cleanup also throws. When configured, it also runs a bounded semantic lifecycle against Redis, etcd or Consul:
two handles share one provider, a queued contender is cancelled, the owner remains held, repeated
release returns the same acknowledged result, and a successor acquires after release. The samples
use the async-scope helper because VB cannot await from a `Finally` block and has no C# `await using`
syntax.

### Quick path: Redis defaults

Only the Redis endpoint and deployment-approved epoch are required. The
namespace defaults to `default`, with a five-second acquisition wait and
thirty-second lease. Namespace metadata must already be provisioned.

```vb
Dim options = New RedisProviderOptions With {
    .Endpoint = redisEndpoint,
    .ExpectedEpoch = approvedEpoch
}
Dim provider = Await RedisProvider.ConnectAsync(options, cancellationToken)
Await WithAsyncDisposeAsync(provider, Async Function()
    Dim lease = Await provider.Lock("reports/daily").AcquireAsync(cancellationToken)
    Await WithAsyncDisposeAsync(lease, Async Function()
        If lease.State <> LeaseState.Held Then Throw New InvalidOperationException("Lease is no longer locally held.")
        Await RebuildDailyReportAsync()
        Dim outcome = Await lease.ReleaseAsync()
        If outcome <> ReleaseOutcome.Released Then
            Throw New InvalidOperationException($"Redis release was not acknowledged: {outcome}")
        End If
    End Function)
End Function)
```

### Explicit connection and acquisition policy

Use immutable options to set the namespace, connection timeout, total wait,
lease TTL, RPC and cleanup budgets, and renewal behavior. Redis auto-renew is
disabled here. The caller's cancellation token applies to connection and
acquisition; cleanup runs in `Finally` independently of that token.

```vb
Dim policy = New AcquireOptions With {
    .WaitTimeout = TimeSpan.FromSeconds(12),
    .Lease = New LeaseOptions With {
        .LeaseDuration = TimeSpan.FromSeconds(45),
        .RpcTimeout = TimeSpan.FromSeconds(2),
        .CleanupTimeout = TimeSpan.FromSeconds(3),
        .AutoRenew = False
    }
}
Dim options = New RedisProviderOptions With {
    .Endpoint = redisEndpoint,
    .Namespace = "billing",
    .ExpectedEpoch = approvedEpoch,
    .ConnectTimeout = TimeSpan.FromSeconds(4),
    .DefaultAcquisition = policy
}
Dim provider = Await RedisProvider.ConnectAsync(options, cancellationToken)
Await WithAsyncDisposeAsync(provider, Async Function()
    Dim lease = Await provider.Lock("invoice/42/reconcile").AcquireAsync(policy, cancellationToken)
    Await WithAsyncDisposeAsync(lease, Async Function()
        If lease.State <> LeaseState.Held Then Throw New InvalidOperationException("Lease is no longer locally held.")
        Await RebuildInvoiceAsync()
        Dim outcome = Await lease.ReleaseAsync()
        If outcome <> ReleaseOutcome.Released Then
            Throw New InvalidOperationException($"Redis release was not acknowledged: {outcome}")
        End If
    End Function)
End Function)
```

The snippets assume `RebuildDailyReportAsync` and `RebuildInvoiceAsync` are
application functions returning `Task`, and use the consumer-local
`WithAsyncDisposeAsync` helper from `Program.vb`. The helper keeps the body
exception primary when cleanup also fails. `LeaseState.Held` is only the SDK's
conservative local observation, not a remote ownership check. Lease expiry
cannot stop stale work; use idempotency or resource-side fencing where required.
Redis has no strong fencing token. Always pass cancellation through to the SDK.

To override the wait or lease timing independently for one acquisition, pass
an `AcquireOptions` object to the handle. Provider options install a
`CleanupObserver`; if best-effort disposal cannot confirm the owner-checked
release, the example reports `Unknown` to standard error. Reconcile the
protected resource before treating that outcome as released or still held.

Run with `dotnet run --project sdk/dotnet/reuse/vb/Dilock.VisualBasic.Consumer.vbproj -c Release`.
