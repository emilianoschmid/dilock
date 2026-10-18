using Dilock;
using Dilock.Redis;
using Dilock.Etcd;
using Dilock.Consul;
using Dilock.Postgres;

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
RequireMethod(typeof(ConsulProvider), nameof(ConsulProvider.Lock), typeof(string));
if (!typeof(PostgresProvider).GetMethods().Any(method => method.Name == nameof(PostgresProvider.ConnectAsync)))
    throw new InvalidOperationException("Installed PostgreSQL package omitted PostgresProvider.ConnectAsync.");
if (!typeof(PostgresProvider).GetMethods().Any(method => method.Name == nameof(PostgresProvider.ProvisionNamespaceAsync)))
    throw new InvalidOperationException("Installed PostgreSQL package omitted explicit namespace provisioning.");
RequireMethod(typeof(PostgresProvider), nameof(PostgresProvider.Lock), typeof(string));
RequireMethod(typeof(ILockHandle), nameof(ILockHandle.TryAcquireAsync), typeof(LeaseOptions), typeof(CancellationToken));
RequireMethod(typeof(ILockHandle), nameof(ILockHandle.AcquireAsync), typeof(TimeSpan), typeof(LeaseOptions), typeof(CancellationToken));
if (!typeof(IEtcdLease).GetProperty(nameof(IEtcdLease.CreateRevision))!.CanRead)
    throw new InvalidOperationException("Installed etcd package omitted its public create_revision fence.");
if (!typeof(IConsulLease).GetProperty(nameof(IConsulLease.LockIndex))!.CanRead)
    throw new InvalidOperationException("Installed Consul package omitted its lock-index fence.");

Console.WriteLine("Exact NuGet artifact consumer passed: core identity/options and Redis/etcd/Consul/PostgreSQL public APIs are available.");
return;

static void RequireMethod(Type type, string name, params Type[] parameters)
{
    if (type.GetMethod(name, parameters) is null)
        throw new InvalidOperationException($"Installed package omitted {type.FullName}.{name}.");
}
