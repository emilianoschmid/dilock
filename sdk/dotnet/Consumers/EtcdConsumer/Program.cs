using Dilock;
using Dilock.Etcd;

var digest = ResourceIdentity.Hash("consumer", "resource");
if (digest.Length != 64)
    throw new InvalidOperationException("Identity API is not available from the package.");

if (args.Length > 0)
{
    var endpoint = args[0];
    var ns = args.Length > 1 ? args[1] : "dotnet-package-consumer";
    var epoch = args.Length > 2 ? args[2] : "local-package-consumer-v1";
    await using var provider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
    var handle = provider.Lock("package-consumer/resource");
    try
    {
        await handle.TryAcquireAsync();
        throw new InvalidOperationException("etcd try-acquire must report UnsupportedCapability.");
    }
    catch (DilockException exception) when (exception.Kind == FailureKind.UnsupportedCapability)
    {
        Console.WriteLine("Installed etcd package consumer passed.");
    }
}
else
{
    Console.WriteLine("Installed etcd package API compiled.");
}
