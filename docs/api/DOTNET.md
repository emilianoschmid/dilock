# .NET API design

Target baseline: `net10.0`, current stable C# for that SDK after M0 verification; nullable enabled. Do not set `LangVersion=preview` or `latest` in reproducible builds. Add older target frameworks only with an explicit support reason and CI coverage.

## Proposed public shape

These are design snippets, not compiled SDK code in this blueprint. Validate/finalize them with consumer tests before expansion.

```csharp
public interface ILockProvider : IAsyncDisposable
{
    LockCapabilities Capabilities { get; }
    IDistributedLock GetLock(LockName name);
}

public interface IDistributedLock
{
    LockName Name { get; }
    Task<ILockLease> AcquireAsync(
        AcquireOptions? options = null,
        CancellationToken cancellationToken = default);
    Task<ILockLease?> TryAcquireAsync(
        LeaseOptions? options = null,
        CancellationToken cancellationToken = default);
}

public interface ILockLease : IAsyncDisposable
{
    LeaseState State { get; }
    CancellationToken LeaseLost { get; }
    FencingToken? FencingToken { get; }
    Task<ReleaseOutcome> ReleaseAsync(
        CancellationToken cancellationToken = default);
}
```

`TryAcquireAsync` returns null only for contention. Acquisition wait timeout uses a dedicated `LockTimeoutException`; uncertain acquisition uses `AcquisitionOutcomeUnknownException`; unavailable backend and unsupported capability have distinguishable categories. Native cancellation remains `OperationCanceledException`; annotate whether a remote acquisition may exist and guarantee bounded cleanup attempts.

`AcquireOptions` composes validated `LeaseOptions` plus wait timeout. Use immutable records for value options where appropriate, `TimeSpan`, `readonly record struct` or validated class for small value types, and sealed implementation classes. Keep mutable state and synchronization private. Do not expose ownership secrets or backend client types from Core.

## Consumer intent

```csharp
// Design example. Backend/provider construction is shown in the eventual quickstart.
var resource = provider.GetLock(new LockName("camera/123/reindex"));
await using var lease = await resource.AcquireAsync(cancellationToken: stoppingToken);
using var workCancellation = CancellationTokenSource.CreateLinkedTokenSource(
    stoppingToken, lease.LeaseLost);
await RebuildDerivedIndexAsync(workCancellation.Token);
```

This example is appropriate for cooperative derived work, not a proof of exactly-once mutation. A separate example must implement resource-side fencing for etcd and explicitly reject using that example with Redis.

## Implementation guidance

Use `Task` for ordinary I/O results. `DisposeAsync` returns `ValueTask` because `IAsyncDisposable` requires it. Do not choose `ValueTask` across the API without evidence. Avoid sync-over-async `.Result`/`.Wait()` wrappers. A future sync API needs its own bounded path, not thread-pool blocking wrapped in an async name.

Inject time (`TimeProvider`), randomness and backend behavior through internal testable seams. Share backend clients appropriately; do not create a connection per acquisition. StackExchange.Redis is a candidate transport client, not an instruction to wrap a different project's lock protocol. For etcd, evaluate a maintained binding or generate official gRPC messages.

Separate Microsoft DI integration from the basic SDK. Native `ILogger`/`ActivitySource`/`Meter` integration may be optional; do not force an OpenTelemetry exporter. API compatibility snapshots, XML doc output and package-consumer tests are required. Public comments document cancellation, throw conditions, lifetime and thread safety, not merely repeat method names.

## Review specifics

Test concurrent acquire on one reusable lock; disposing twice; explicit release racing auto-renew; provider disposal during acquisition; a borrowed multiplexer surviving provider disposal; lost lease cancellation not firing after ordinary successful release unless explicitly documented; user exception preserved during failed cleanup. Do not bind ownership to the calling thread or make reentrancy implicit through AsyncLocal.
