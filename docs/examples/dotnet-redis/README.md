# .NET Redis public API consumer

This console app exercises `Dilock.Redis` against one already-running Redis authority. Two independently owned providers contend on a unique resource. The example observes acknowledged contention, waits for the original server-side TTL, acquires as a successor, then checks that late cleanup from the original owner cannot remove the successor's lock.

The example never writes namespace metadata. An operator must provision the metadata before running it. The required record is `dilock:namespace:<namespace>:meta` with UTF-8 JSON containing `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "redis-single-authority-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md). Use a non-production Redis authority for this short-lived demonstration. The resource name is unique per run and its TTL is three seconds.

## Build and run

From the repository root, pack the local SDK packages and restore the consumer only from that local feed plus the already-cached NuGet packages:

```powershell
$repo = (Get-Location).Path
$env:NUGET_PACKAGES = Join-Path $repo '.tools/dotnet-example-packages'
New-Item -ItemType Directory -Force .tools/dotnet-example-feed | Out-Null
dotnet pack sdk/dotnet/Dilock.Core/Dilock.Core.csproj -c Release -o .tools/dotnet-example-feed
dotnet pack sdk/dotnet/Dilock.Redis/Dilock.Redis.csproj -c Release -o .tools/dotnet-example-feed
dotnet restore docs/examples/dotnet-redis/RedisConsumer.csproj --source (Join-Path $repo '.tools/dotnet-example-feed') --source (Join-Path $repo '.tools/dotnet-home/.nuget/packages') --ignore-failed-sources
dotnet build docs/examples/dotnet-redis/RedisConsumer.csproj -c Release --no-restore
```

The local-only restore command uses this checkout's populated package cache so the example can be compiled without contacting a NuGet registry. A normal consumer can install `Dilock.Redis` version `0.1.0-dev.1` from its configured NuGet source and then run `dotnet restore` and `dotnet build` normally.

Set values for a namespace that an operator has already provisioned, then run the public-package consumer:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
dotnet run --project docs/examples/dotnet-redis/RedisConsumer.csproj -c Release --no-build
```

`DILOCK_REDIS_URL` may contain Redis credentials. Keep it in the environment or a secret manager; do not put credentials in source, command history, or logs. The SDK does not log the endpoint. Use a Redis CLI or provisioning tool outside this example to create the metadata record, for example:

```powershell
$metadata = '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"deployment-2026-01","fixed_lease_ms":null}'
redis-cli -u $env:DILOCK_REDIS_URL SET 'dilock:namespace:example:meta' $metadata NX
```

Check the provisioning command result. `NX` prevents overwriting existing metadata; if the key already exists, inspect it and resolve any epoch/profile mismatch explicitly. Do not use the sample namespace or epoch for a real deployment without an operator's approval.

## Ownership, cancellation, and limits

Each `RedisProvider` owns its connection and must be disposed (`await using` here). A handle can be reused; every returned lease represents one acquisition and should be explicitly released. The two providers in this sample own separate client connections. `TryAcquireAsync` returns `null` only for acknowledged contention; connection, protocol, and uncertain-acquisition errors throw `DilockException` instead.

Ctrl+C cancels the current wait or API call through its `CancellationToken`. Cancellation before a command is dispatched is local. If cancellation or a missing response happens after acquisition was sent, the result is ambiguous and is reported as `AcquisitionUnknown`, never as busy; the SDK schedules owner-checked cleanup. Cleanup has its own bounded budget. `finally` also attempts owner-checked release, and provider disposal performs bounded best-effort cleanup.

With auto-renew disabled, the three-second TTL is a server-side expiry fallback. Expiry or the `Lost` signal cannot stop stale application work. The owner token prevents this SDK's late release from deleting a successor, but is not a fencing token. Redis single-authority does not provide strong fencing or failover guarantees. This sample validates one Redis authority only and is not evidence of production readiness.
