# Dilock — C# / .NET

Status: draft-1 core contract, Redis, native etcd and draft Consul HTTP/KV session adapters, JSONL worker backend selection, Release builds and single-member local backend integration tests implemented. Consul uses 10-second-minimum TTL sessions and exposes a per-key `LockIndex` fence whose safety depends on retaining each lock entry. The tested backends do not establish quorum, leader failover, resource-side atomic fencing, cross-language conformance or production readiness.

Design: Small interfaces; sealed implementations; Task; CancellationToken; IAsyncDisposable; immutable options.

The provider owns one Redis authority, its leases, renewal and cleanup. Explicitly `await using` the provider and leases; handles may be reused. Calls are thread-safe; lease mutations are serialized. `try_acquire` returns null only for acknowledged contention. `CancellationToken` before send cancels locally. After a dispatched mutation, a lost acknowledgement or unconfirmed owner cleanup is `AcquisitionUnknown`; when Redis acknowledged acquisition and cancellation cleanup is confirmed, cancellation is reported as `OperationCanceledException`. Cleanup has a bounded independent budget. `await using` performs best-effort disposal and reports non-`Released` cleanup through the configured observer; call `ReleaseAsync` and inspect its result when the acknowledgement matters. `Lost` withdraws local permission but cannot stop stale work. Auto-renewal is opt-in. Strong fencing is rejected for Redis. Process crashes use the server TTL fallback. Redis strict mutual exclusion assumes one writable authority that preserves lock state; asynchronous replica promotion can lose a held key and is unsupported for this guarantee. Use resource-side fencing enforced atomically when failover safety is required.

Build: `dotnet build sdk/dotnet/Worker/Worker.csproj -c Release`. Redis client `StackExchange.Redis` is pinned to 3.3.1. etcd details and local integration test instructions are in `Dilock.Etcd/README.md`; Consul configuration and limits are in `Dilock.Consul/README.md`. The artifacts are local-only; NuGet owner names and publication are not set up.

## Quick use

```csharp
using System;
using System.Threading.Tasks;
using Dilock.Redis;

var endpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_URL")
    ?? throw new InvalidOperationException("Set DILOCK_REDIS_URL.");
var epoch = Environment.GetEnvironmentVariable("DILOCK_EPOCH")
    ?? throw new InvalidOperationException("Set the provisioned DILOCK_EPOCH.");

await using var provider = await RedisProvider.ConnectAsync(endpoint, epoch);
var monthlyClose = provider.Lock("monthly-close");
await using (var lease = await monthlyClose.AcquireAsync())
{
    await RunShortOperationAsync();
    var outcome = await lease.ReleaseAsync();
    if (outcome != ReleaseOutcome.Released)
        throw new InvalidOperationException($"Redis release was not acknowledged as Released: {outcome}");
} // await using is a bounded best-effort fallback; the 30-second server TTL is unchanged.

static Task RunShortOperationAsync() => Task.CompletedTask;
```

The convenience factory selects the provisioned `default` namespace and common
five-second wait / thirty-second lease policy. Replace the endpoint and epoch
with deployment-approved values; connection validates pre-created namespace
metadata and does not create or repair it. For the full immutable transport and
acquisition configuration, observer behavior, and simultaneous resources
sharing one provider, see
[`docs/examples/dotnet-redis/quickstart`](../../docs/examples/dotnet-redis/quickstart/README.md)
and
[`docs/examples/dotnet-etcd/quickstart`](../../docs/examples/dotnet-etcd/quickstart/README.md).

`AcquireAsync` treats its wait as one total budget across metadata validation,
the Redis `SET NX` attempt, and contention classification. If a dispatched `SET`
times out, the result remains `AcquisitionUnknown` because Redis may have
accepted it.

Use `../../spec/` for semantics and `../../docs/api/LANGUAGE-DESIGN.md` for design. Do not publish before real backend, conformance and installed-package tests pass.
