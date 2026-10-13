# .NET etcd quickstart

Provision namespace metadata once with an approved epoch before running this
example. Runtime connection never creates or repairs metadata. Set
`DILOCK_ETCD_ENDPOINT` and `DILOCK_EPOCH`; `DILOCK_NAMESPACE` selects the
namespace for the configured example. The zero-argument provider factory uses
namespace `default` and the shared 5 second wait / 30 second lease defaults:

```powershell
dotnet run --project docs/examples/dotnet-etcd/quickstart/Quickstart.csproj
```

Pass `--configured` to use the full immutable provider and acquisition policies:

```powershell
dotnet run --project docs/examples/dotnet-etcd/quickstart/Quickstart.csproj -- --configured
```

Only `--shared-provider` holds two different resources through the same gRPC
channel and stub set; etcd still creates a separate server lease per acquisition.
The example prints etcd creation revisions only after requiring acknowledged
`Released` outcomes for both leases. `await using` is bounded best-effort
fallback cleanup. These independent locks are not acquired atomically.
That scope does not extend TTL while work is paused, and etcd cannot stop stale
application work. Its strong fence must be checked by the protected resource.

Run the shared-provider case with two independent resource names through the same provider:

```powershell
dotnet run --project docs/examples/dotnet-etcd/quickstart/Quickstart.csproj -- --shared-provider
```
