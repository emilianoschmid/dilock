# F# with etcd

`quickstart/main.fs` consumes the .NET native etcd adapter from F#; it reuses the .NET engine. The local project targets .NET 8 and references `Dilock.Etcd`.

From the repository root, compile with `dotnet build docs/examples/fsharp-etcd/quickstart/Dilock.Quickstart.fsproj -c Release`. Set `DILOCK_ETCD_ENDPOINT` and `DILOCK_COORDINATION_EPOCH`, then run with `dotnet run --project docs/examples/fsharp-etcd/quickstart/Dilock.Quickstart.fsproj -c Release`. etcd metadata for the default namespace must already match and use profile `etcd-native-lock-v1`. The protected resource must atomically enforce the epoch/revision fence.

For explicit timeout and lease configuration, see [the F# SDK guide](../../../sdk/dotnet/reuse/fsharp/README.md). Handles from one provider reuse its channel but acquire independent leases.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
