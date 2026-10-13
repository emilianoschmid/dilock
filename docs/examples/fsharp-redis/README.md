# F# with Redis

`quickstart/main.fs` is an F# consumer of the .NET Redis SDK; it reuses the .NET engine. The local project targets .NET 8 and references `Dilock.Redis`.

From the repository root, compile with `dotnet build docs/examples/fsharp-redis/quickstart/Dilock.Quickstart.fsproj -c Release`. Set `DILOCK_REDIS_URL` and `DILOCK_COORDINATION_EPOCH`, then run with `dotnet run --project docs/examples/fsharp-redis/quickstart/Dilock.Quickstart.fsproj -c Release`. The default namespace and five-second wait/thirty-second lease policy apply. Metadata must already match the configured epoch.

For explicit timeout and lease configuration, see [the F# SDK guide](../../../sdk/dotnet/reuse/fsharp/README.md). Multiple resource handles created from one provider share its backend connection; each acquisition still has independent ownership.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
