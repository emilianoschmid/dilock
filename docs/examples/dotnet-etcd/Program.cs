using Dilock;
using Dilock.Etcd;

var endpoint = RequiredEnvironment("DILOCK_ETCD_ENDPOINT");
var namespaceName = RequiredEnvironment("DILOCK_NAMESPACE");
var epoch = RequiredEnvironment("DILOCK_EPOCH");

using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

var resource = $"docs-example/dotnet-etcd/{Guid.NewGuid():N}";
var options = new LeaseOptions
{
    LeaseDuration = TimeSpan.FromSeconds(10),
    RpcTimeout = TimeSpan.FromSeconds(2),
    CleanupTimeout = TimeSpan.FromSeconds(2),
    AutoRenew = false
};

await using var ownerProvider = await EtcdProvider.ConnectAsync(
    endpoint, namespaceName, epoch, cancellationToken: cancellation.Token);
await using var successorProvider = await EtcdProvider.ConnectAsync(
    endpoint, namespaceName, epoch, cancellationToken: cancellation.Token);

ILease? ownerLease = null;
ILease? successorLease = null;
try
{
    ownerLease = await ownerProvider.Lock(resource).AcquireAsync(
        TimeSpan.FromSeconds(5), options, cancellation.Token);
    if (ownerLease.State != LeaseState.Held)
        throw new InvalidOperationException("Owner lease is not locally held after acquisition.");

    var ownerEtcdLease = (IEtcdLease)ownerLease;
    Console.WriteLine($"owner: acquired fence {ownerEtcdLease.Epoch}/{ownerEtcdLease.CreateRevision}");

    var release = await ownerLease.ReleaseAsync();
    if (release != ReleaseOutcome.Released)
        throw new InvalidOperationException($"Owner release was not acknowledged: {release}.");
    ownerLease = null;
    Console.WriteLine("owner: release acknowledged");

    successorLease = await successorProvider.Lock(resource).AcquireAsync(
        TimeSpan.FromSeconds(5), options, cancellation.Token);
    if (successorLease.State != LeaseState.Held)
        throw new InvalidOperationException("Successor lease is not locally held after acquisition.");
    var successorEtcdLease = (IEtcdLease)successorLease;
    Console.WriteLine($"successor: acquired fence {successorEtcdLease.Epoch}/{successorEtcdLease.CreateRevision}");

    var successorRelease = await successorLease.ReleaseAsync();
    if (successorRelease != ReleaseOutcome.Released)
        throw new InvalidOperationException($"Successor release was not acknowledged: {successorRelease}.");
    successorLease = null;
    Console.WriteLine("successor: release acknowledged");
}
finally
{
    if (successorLease is not null)
        await successorLease.ReleaseAsync();
    if (ownerLease is not null)
        await ownerLease.ReleaseAsync();
}

static string RequiredEnvironment(string name) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
        ? value
        : throw new InvalidOperationException($"Set {name} for the operator-provisioned etcd namespace.");
