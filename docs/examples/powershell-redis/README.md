# PowerShell with Redis

`quickstart/main.ps1` is a PowerShell consumer over the .NET Redis engine and module. Build .NET 8 assemblies locally from the repository root:

```powershell
dotnet build sdk/dotnet/Dilock.Redis/Dilock.Redis.csproj -c Release
$env:DILOCK_ASSEMBLY_DIR = (Resolve-Path sdk/dotnet/Dilock.Redis/bin/Release/net8.0).Path
$env:DILOCK_REDIS_URL = 'redis://127.0.0.1:6379'
$env:DILOCK_COORDINATION_EPOCH = '<provisioned-epoch>'
pwsh -NoProfile -File docs/examples/powershell-redis/quickstart/main.ps1
```

`DILOCK_NAMESPACE` defaults to `default`; its Redis metadata must already match the epoch. The sample uses the five-second wait/thirty-second lease defaults and always disposes lease and provider. For explicit settings and packaged module usage, see [the PowerShell guide](../../../sdk/dotnet/reuse/powershell/README.md). Multiple handles from one provider reuse the connection but own separate acquisitions.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
