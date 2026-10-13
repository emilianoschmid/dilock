# Exact NuGet artifact consumer

`package_consumer.py` builds the Core, Redis, and etcd NuGet packages from the current SDK sources, restores a fresh application using only the generated local feed and an isolated offline dependency cache, then runs this fixture outside the repository checkout.

The fixture verifies the installed identity/options APIs and the Redis/etcd public API surface without connecting to an authority. Backend behavior is validated separately by the real-server integration and cross-SDK conformance suites.

Run from the repository root with a new, empty scratch directory outside the checkout:

```powershell
python sdk/dotnet/Consumers/package_consumer.py `
  --output validation/implementation/m6-dotnet-package-consumer.json `
  --scratch $env:TEMP/dilock-dotnet-package-consumer
```

The report includes package SHA-256 hashes, commands, status, and the retained external consumer workspace. Existing reports and non-empty workspaces are never overwritten.

The default `dotnet` executable is resolved from `PATH`; override it with `--dotnet` or `DILOCK_DOTNET`. The dependency cache defaults to `REPO/.tools/dotnet-example-packages`; select another pre-populated cache with `--nuget-cache` or `DILOCK_DOTNET_NUGET_CACHE`. The consumer remains offline: restore uses only the generated exact-package feed and the copied cache.
