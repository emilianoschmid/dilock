# PowerShell with etcd

`quickstart/main.ps1` is a PowerShell consumer over the .NET native etcd engine and module. Build .NET 8 assemblies locally from the repository root:

```powershell
dotnet build sdk/dotnet/Dilock.Etcd/Dilock.Etcd.csproj -c Release
$env:DILOCK_ASSEMBLY_DIR = (Resolve-Path sdk/dotnet/Dilock.Etcd/bin/Release/net8.0).Path
$env:DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:2379'
$env:DILOCK_COORDINATION_EPOCH = '<provisioned-epoch>'
pwsh -NoProfile -File docs/examples/powershell-etcd/quickstart/main.ps1
```

`DILOCK_NAMESPACE` defaults to `default`; etcd metadata must already match and use profile `etcd-native-lock-v1`. Atomically enforce the epoch/revision fence at the protected resource. For explicit settings and module packaging, see [the PowerShell guide](../../../sdk/dotnet/reuse/powershell/README.md). Handles from one provider reuse a channel but acquire independently.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
