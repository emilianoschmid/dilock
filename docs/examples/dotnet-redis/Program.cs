using Dilock;
using Dilock.Redis;

var endpoint = RequiredEnvironment("DILOCK_REDIS_URL");
var namespaceName = RequiredEnvironment("DILOCK_NAMESPACE");
var epoch = RequiredEnvironment("DILOCK_EPOCH");
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

var resource = $"docs-example/dotnet-redis/{Guid.NewGuid():N}";
var options = new LeaseOptions
{
    LeaseDuration = TimeSpan.FromSeconds(3),
    RpcTimeout = TimeSpan.FromSeconds(1),
    CleanupTimeout = TimeSpan.FromSeconds(1),
    AutoRenew = false
};

await using var originalProvider = await RedisProvider.ConnectAsync(
    endpoint, namespaceName, epoch, cancellationToken: cancellation.Token);
await using var contenderProvider = await RedisProvider.ConnectAsync(
    endpoint, namespaceName, epoch, cancellationToken: cancellation.Token);

var originalHandle = originalProvider.Lock(resource);
var contenderHandle = contenderProvider.Lock(resource);
ILease? originalLease = null;
ILease? successorLease = null;

try
{
    originalLease = await originalHandle.TryAcquireAsync(options, cancellation.Token)
        ?? throw new InvalidOperationException("The unique example resource was unexpectedly busy.");

    var busy = await contenderHandle.TryAcquireAsync(options, cancellation.Token);
    if (busy is not null)
    {
        await busy.ReleaseAsync();
        throw new InvalidOperationException("A contender acquired while the original lease was active.");
    }
    Console.WriteLine("contender: acknowledged busy");

    // Wait past Redis TTL. Expiry withdraws local reliance but cannot stop this process.
    await Task.Delay(options.LeaseDuration + TimeSpan.FromMilliseconds(500), cancellation.Token);

    successorLease = await contenderHandle.TryAcquireAsync(options, cancellation.Token)
        ?? throw new InvalidOperationException("The contender remained busy after the lease TTL.");
    Console.WriteLine("contender: acquired after Redis TTL");

    // Owner-checked cleanup must leave the successor's distinct token untouched.
    var staleRelease = await originalLease.ReleaseAsync();
    if (staleRelease != ReleaseOutcome.AlreadyNotOwned)
        throw new InvalidOperationException($"Expected stale release to be AlreadyNotOwned, got {staleRelease}.");
    Console.WriteLine("original owner: stale release did not remove successor");

    var finalRelease = await successorLease.ReleaseAsync();
    if (finalRelease != ReleaseOutcome.Released)
        throw new InvalidOperationException($"Successor release was not acknowledged: {finalRelease}.");
    Console.WriteLine("contender: successor release acknowledged");
}
finally
{
    if (successorLease is not null)
        await successorLease.ReleaseAsync();
    if (originalLease is not null)
        await originalLease.ReleaseAsync();
}

static string RequiredEnvironment(string name) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
        ? value
        : throw new InvalidOperationException($"Set {name} for the pre-provisioned Redis namespace.");
