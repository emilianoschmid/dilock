# Dilock.Consul

Draft implementation of `consul-kv-session-v1`. It uses the Consul HTTP KV
and Session APIs and needs no Consul client package. Provision matching
metadata with `tools/provision_consul.py` before connecting. Session TTLs are
rounded up to whole seconds and must be from 10 seconds through 24 hours.

```csharp
await using var provider = await ConsulProvider.ConnectAsync(
    new ConsulProviderOptions
    {
        Endpoint = "http://127.0.0.1:8500",
        Namespace = "app",
        ExpectedEpoch = "deployment-2026-01",
        Token = Environment.GetEnvironmentVariable("CONSUL_HTTP_TOKEN")
    });

await using var lease = await provider.Lock("reports/daily").AcquireAsync();
if (lease is IConsulLease consulLease)
    Console.WriteLine($"Fence: {consulLease.Epoch}:{consulLease.LockIndex}");
```

`LockIndex` is a per-key fence only while operators retain the KV lock entry
and its approved epoch. Consul sessions provide advisory locks; they do not
stop a stale process or enforce a fence at an external resource. A single
Consul agent proves functional behavior only, not quorum or failover.
