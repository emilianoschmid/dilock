# .NET etcd public API consumer

This standalone console application uses `Dilock.Etcd` and one already-running etcd v3 authority. Two separately owned providers acquire the same unique resource in sequence. The sample displays each epoch-scoped etcd `create_revision` fence, explicitly releases the first lease, and then acquires and releases a successor. etcd's native `Lock` API has no non-blocking try-lock operation, so the sample does not infer “busy” from a timeout.

An operator must provision namespace metadata before the application connects. The required key is `/dilock/namespaces/<namespace>/meta`, containing UTF-8 JSON with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The SDK validates it with a linearizable read and never creates or changes it. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md). Use a disposable, non-production authority for this short lease demonstration.

## Build and run

From the repository root, restore the public-package consumer using only the staged local NuGet feeds:

```powershell
$repo = (Get-Location).Path
$env:NUGET_PACKAGES = Join-Path $repo '.tools/dotnet-example-packages'
dotnet restore docs/examples/dotnet-etcd/EtcdConsumer.csproj --source (Join-Path $repo '.tools/nuget-local-final') --source (Join-Path $repo '.tools/localnuget') --source (Join-Path $repo '.tools/localflat') -p:NuGetAudit=false -p:NoWarn=NU1900
dotnet build docs/examples/dotnet-etcd/EtcdConsumer.csproj -c Release --no-restore
```

The local-only procedure uses the packages already staged in this checkout and keeps NuGet's writable cache under `.tools`. `NuGetAudit=false` avoids an online audit lookup; `NoWarn=NU1900` suppresses only the unavailable audit-source warning. A normal consumer installs `Dilock.Etcd` from its configured NuGet source and restores normally.

Set an endpoint and the exact identity already provisioned by the operator:

```powershell
$env:DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:2379'
$env:DILOCK_NAMESPACE = 'example'
$env:DILOCK_EPOCH = 'operator-approved-cluster-incarnation'
dotnet run --project docs/examples/dotnet-etcd/EtcdConsumer.csproj -c Release --no-build
```

The endpoint must be one HTTP/2 etcd authority URL without credentials, path, query, or fragment. For TLS or client certificates, construct a fresh handler with the SDK's `ConnectAsync` handler-factory overload. Never place credentials in source or command history.

## Lifetime, cancellation, fencing, and limits

Each provider owns its gRPC channel and tracked leases and is disposed with `await using`. Handles are reusable and provider operations are thread-safe. Ctrl+C cancels the active API operation through a `CancellationToken`. Cancellation or a transport failure after a request is sent is not ordinary contention; acquisition may have an unknown remote outcome, and the SDK performs bounded attempt-lease cleanup. `AcquireAsync` uses etcd's native queued Lock RPC with a bounded wait. Its timeout is a typed failure, not a “busy” result. `TryAcquireAsync` reports `UnsupportedCapability` without an RPC.

The fence is the pair `(Epoch, CreateRevision)`. A protected resource must atomically validate the approved epoch and reject stale revisions before relying on fencing. Restoring or replacing the etcd authority requires a coordinated epoch migration. Lease expiration or the `Lost` signal cannot stop stale application work, and etcd fencing does not make a downstream effect transactional with etcd. This example does not validate quorum failure, failover, or fencing enforcement by an external resource, and is not production-readiness evidence.
