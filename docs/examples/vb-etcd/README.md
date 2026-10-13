# Visual Basic .NET with etcd

`quickstart/main.vb` consumes the .NET native etcd SDK from Visual Basic; it reuses the .NET engine. The local project targets .NET 8 and references `Dilock.Etcd`.

From the repository root, compile with `dotnet build docs/examples/vb-etcd/quickstart/Dilock.Quickstart.vbproj -c Release`. Set `DILOCK_ETCD_ENDPOINT` and `DILOCK_COORDINATION_EPOCH`, then run with `dotnet run --project docs/examples/vb-etcd/quickstart/Dilock.Quickstart.vbproj -c Release`. etcd metadata for the default namespace must already match and use profile `etcd-native-lock-v1`. Enforce its epoch/revision fence atomically at the protected resource.

For explicit timeout and lease configuration, see [the VB SDK guide](../../../sdk/dotnet/reuse/vb/README.md). Handles from one provider reuse the gRPC channel but each acquisition has independent ownership.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
