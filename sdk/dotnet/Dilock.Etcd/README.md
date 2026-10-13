# Dilock.Etcd

The .NET etcd provider uses etcd's native v3 gRPC `Lock`, `Unlock`, `Lease`, and linearizable `Range` APIs. It grants one lease for each acquisition attempt, including attempts made concurrently through one handle. It reads namespace metadata but never creates or changes it.

`CleanupTimeout` is one monotonic budget for stopping acquisition keepalive, draining an in-flight renewal, and revoking the attempt lease. Cleanup does not restart a full timeout for each phase.

```csharp
await using var provider = await EtcdProvider.ConnectAsync(
    "http://127.0.0.1:2379", "billing", "cluster-incarnation-2026-01");
var handle = provider.Lock("invoice/482");
await using var lease = await handle.AcquireAsync(
    TimeSpan.FromSeconds(5),
    new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(30), AutoRenew = true });

var etcdLease = (IEtcdLease)lease;
Console.WriteLine($"Fence: {etcdLease.Epoch}/{etcdLease.CreateRevision}");
```

Provision `/dilock/namespaces/<namespace>/meta` before connecting. Its value must be a UTF-8 JSON object with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. Metadata is checked with a linearizable read. The SDK refuses a missing or incompatible record.

`EtcdProvider` owns its gRPC channel and active leases; `await using` is recommended. Handles are reusable and concurrent calls are independent. The provider is thread-safe. Lease state changes are serialized; `Lost` withdraws local reliance but cannot stop a process that continues working. Cancellation before a request is sent is local. Cancellation or a missing reply after dispatch never becomes contention; the SDK revokes that attempt's unique lease with a separate bounded cleanup budget and reports uncertainty if the revoke cannot be confirmed. etcd has no native non-blocking try-lock RPC, so `TryAcquireAsync` fails with `UnsupportedCapability` without sending an RPC. `AcquireAsync` uses one native queue wait bounded by its total wait budget; an expired wait is reported only after the attempt's own lease revoke is acknowledged.

For custom TLS trust or mutual TLS, pass a handler factory to `ConnectAsync`; it must return a fresh `HttpMessageHandler` owned by this provider's channel. Dispose the provider to dispose that handler. The channel has no gRPC retry service configuration.

etcd rounds lease TTLs to integer seconds; `EffectiveLeaseMilliseconds` reports the server's effective TTL. The provider keeps a waiter lease alive while the native Lock RPC is pending. `AutoRenew` controls renewal after acquisition. A successful ownership check ranges the exact opaque key returned by `Lock`, confirms its lease attachment, and exposes that key's `create_revision` through `IEtcdLease`.

The fence is the pair `(Epoch, CreateRevision)`. The protected resource must atomically validate the approved epoch and register/reject revisions before relying on fencing. A restore or replacement coordinator requires a coordinated epoch migration. Fencing does not make the protected side effect transactional with etcd. Single-member functional tests do not prove quorum or leader-failover behavior.

This provider pins `dotnet-etcd` 9.1.0 for its generated etcd protobuf messages and native gRPC service stubs. It constructs the transport channel and stubs directly without the package's retry-configured convenience channel, so an ambiguous mutation is not replayed. The package and its transitive dependencies are pinned by `packages.lock.json`.

Run the real-server tests against a disposable local v3 server with `DILOCK_ETCD_ENDPOINT=http://127.0.0.1:2379`:

```powershell
dotnet test sdk/dotnet/Tests/Dilock.Tests.csproj -c Release --filter FullyQualifiedName~EtcdIntegration
```

The integration tests create unique namespace metadata and lock keys on that configured server. Do not point them at a shared or production cluster.
