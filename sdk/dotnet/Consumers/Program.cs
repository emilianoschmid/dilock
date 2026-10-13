using Dilock;
using Dilock.Redis;

if (ResourceIdentity.Hash("default", "r").Length != 64)
    throw new InvalidOperationException("identity not usable");

if (args.Length == 1)
{
    await using var provider = await RedisProvider.ConnectAsync(args[0], "consumer", "local-development");
    var lease = await provider.Lock("resource").TryAcquireAsync(new LeaseOptions());
    if (lease is not null) await lease.DisposeAsync();
}
Console.WriteLine("Installed NuGet consumer passed.");
