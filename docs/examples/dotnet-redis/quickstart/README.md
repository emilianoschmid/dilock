# .NET Redis quickstart

Provision namespace metadata once with an approved epoch before running this
example. Runtime connection never creates or repairs metadata. Set
`DILOCK_REDIS_URL` and `DILOCK_EPOCH`; `DILOCK_NAMESPACE` selects the namespace
for the configured example. The zero-argument provider factory uses namespace
`default` and the shared 5 second wait / 30 second lease defaults:

```powershell
dotnet run --project docs/examples/dotnet-redis/quickstart/Quickstart.csproj
```

Pass `--configured` to use the full immutable provider and acquisition policies:

```powershell
dotnet run --project docs/examples/dotnet-redis/quickstart/Quickstart.csproj -- --configured
```

Only `--shared-provider` holds two different resources at the same time. They
share one provider and Redis client pool; the operation is not atomic across
resources. The program explicitly requires an acknowledged `Released` result
before reporting success; `await using` provides bounded best-effort fallback
cleanup. This short example leaves renewal disabled; the scope does not extend
the distributed TTL.

Run the shared-provider case with two independent resource names through the same provider:

```powershell
dotnet run --project docs/examples/dotnet-redis/quickstart/Quickstart.csproj -- --shared-provider
```
