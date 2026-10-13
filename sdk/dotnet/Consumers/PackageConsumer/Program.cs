using Dilock;
using Dilock.Redis;
using Dilock.Etcd;

// Exercise public APIs from installed NuGet archives without connecting to a backend.
// Real Redis/etcd behavior is covered by the separate backend integration/conformance runs.
var identity = ResourceIdentity.Hash("consumer", "invoice/42");
if (identity.Length != 64 || identity.Any(character => !Uri.IsHexDigit(character)))
    throw new InvalidOperationException("Installed core package returned an invalid resource identity.");

var options = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(5) };
options.Validate();
if (options.TtlMilliseconds != 5000)
    throw new InvalidOperationException("Installed core package did not preserve lease options.");

RequireMethod(typeof(RedisProvider), nameof(RedisProvider.ConnectAsync), typeof(string), typeof(string), typeof(string), typeof(CancellationToken));
RequireMethod(typeof(RedisProvider), nameof(RedisProvider.Lock), typeof(string));
RequireMethod(typeof(EtcdProvider), nameof(EtcdProvider.Lock), typeof(string));
RequireMethod(typeof(ILockHandle), nameof(ILockHandle.TryAcquireAsync), typeof(LeaseOptions), typeof(CancellationToken));
RequireMethod(typeof(ILockHandle), nameof(ILockHandle.AcquireAsync), typeof(TimeSpan), typeof(LeaseOptions), typeof(CancellationToken));
if (!typeof(IEtcdLease).GetProperty(nameof(IEtcdLease.CreateRevision))!.CanRead)
    throw new InvalidOperationException("Installed etcd package omitted its public create_revision fence.");

Console.WriteLine("Exact NuGet artifact consumer passed: core identity/options and Redis/etcd public APIs are available.");
return;

static void RequireMethod(Type type, string name, params Type[] parameters)
{
    if (type.GetMethod(name, parameters) is null)
        throw new InvalidOperationException($"Installed package omitted {type.FullName}.{name}.");
}
