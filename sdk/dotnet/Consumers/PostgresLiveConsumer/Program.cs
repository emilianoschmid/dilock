using Dilock;
using Dilock.Postgres;

if (args.Length < 1)
    throw new ArgumentException("Usage: PostgresLiveConsumer <connection-string> [namespace] [epoch]");

var connectionString = args[0];
var nameSpace = args.Length > 1 ? args[1] : "ci-postgres-dotnet";
var epoch = args.Length > 2 ? args[2] : "ci-postgres-dotnet-epoch";
var providerOptions = new PostgresProviderOptions
{
    ConnectionString = connectionString,
    Namespace = nameSpace,
    ExpectedEpoch = epoch,
};
await PostgresProvider.ProvisionNamespaceAsync(providerOptions);
await using var first = await PostgresProvider.ConnectAsync(providerOptions);
await using var second = await PostgresProvider.ConnectAsync(providerOptions);

var leaseOptions = new LeaseOptions
{
    LeaseDuration = TimeSpan.FromSeconds(5),
    RpcTimeout = TimeSpan.FromMilliseconds(500),
    CleanupTimeout = TimeSpan.FromMilliseconds(500),
};
var firstHandle = first.Lock("dotnet-provider");
var secondHandle = second.Lock("dotnet-provider");
var lease = await firstHandle.TryAcquireAsync(leaseOptions)
    ?? throw new InvalidOperationException("first PostgreSQL acquisition was not acknowledged");
try
{
    if (!await lease.RenewAsync()) throw new InvalidOperationException("PostgreSQL liveness renewal was not acknowledged");
    if (await secondHandle.TryAcquireAsync(leaseOptions) is not null)
        throw new InvalidOperationException("contended PostgreSQL acquisition unexpectedly succeeded");

    var different = await second.Lock("dotnet-different-resource").TryAcquireAsync(leaseOptions)
        ?? throw new InvalidOperationException("different PostgreSQL resource did not progress");
    try
    {
        if (await different.ReleaseAsync() != ReleaseOutcome.Released)
            throw new InvalidOperationException("different-resource release was not acknowledged");
    }
    finally { await different.DisposeAsync(); }

    try
    {
        await secondHandle.AcquireAsync(TimeSpan.FromMilliseconds(150), leaseOptions);
        throw new InvalidOperationException("contended PostgreSQL acquisition did not time out");
    }
    catch (DilockException error) when (error.Kind == FailureKind.WaitTimeout) { }

    try
    {
        await secondHandle.TryAcquireAsync(leaseOptions with { RequireFencing = true });
        throw new InvalidOperationException("unsupported PostgreSQL fencing was accepted");
    }
    catch (DilockException error) when (error.Kind == FailureKind.UnsupportedCapability) { }

    using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
    try
    {
        await secondHandle.AcquireAsync(TimeSpan.FromSeconds(5), leaseOptions, cancellation.Token);
        throw new InvalidOperationException("cancelled PostgreSQL acquisition unexpectedly succeeded");
    }
    catch (OperationCanceledException) { }

    if (await lease.ReleaseAsync() != ReleaseOutcome.Released)
        throw new InvalidOperationException("PostgreSQL release was not acknowledged");
}
finally { await lease.DisposeAsync(); }

var handoff = await secondHandle.TryAcquireAsync(leaseOptions)
    ?? throw new InvalidOperationException("PostgreSQL same-session handoff did not progress");
try
{
    if (await handoff.ReleaseAsync() != ReleaseOutcome.Released)
        throw new InvalidOperationException("PostgreSQL handoff release was not acknowledged");
}
finally { await handoff.DisposeAsync(); }

Console.WriteLine("PostgreSQL .NET live consumer passed: contention, independent resource, timeout, cancellation, capability rejection and handoff.");
