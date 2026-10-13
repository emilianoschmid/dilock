# Visual Basic .NET with Redis

`quickstart/main.vb` consumes the .NET Redis SDK from Visual Basic; it reuses the .NET engine. The local project targets .NET 8 and references `Dilock.Redis`.

From the repository root, compile with `dotnet build docs/examples/vb-redis/quickstart/Dilock.Quickstart.vbproj -c Release`. Set `DILOCK_REDIS_URL` and `DILOCK_COORDINATION_EPOCH`, then run with `dotnet run --project docs/examples/vb-redis/quickstart/Dilock.Quickstart.vbproj -c Release`. The default namespace and five-second wait/thirty-second lease policy apply. Metadata must already match the configured epoch.

For explicit timeout and lease configuration, see [the VB SDK guide](../../../sdk/dotnet/reuse/vb/README.md). Multiple handles from one provider share its connection; separate acquisitions do not form one atomic operation.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
