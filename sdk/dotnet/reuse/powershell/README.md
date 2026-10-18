# Dilock PowerShell integration

This directory contains a script consumer for development and a minimal local
PowerShell module candidate in [`module/Dilock`](module/Dilock/README.md). The
module wraps the existing public .NET Core, Redis, etcd, Consul and PostgreSQL APIs; it does not
implement a PowerShell lock engine. Its ZIP is locally buildable but has not
been published to the PowerShell Gallery.

Build the package offline from a populated local NuGet cache:

```powershell
pwsh -NoProfile -File sdk/dotnet/reuse/powershell/Build-DilockModule.ps1
```

The script uses the local package cache as its only restore source. Pass
`-NuGetCacheSource` to select another local NuGet package directory. For a
build already performed in an isolated Docker toolchain, pass its published
assembly directory with `-RuntimeAssemblyDirectory`; this packages those exact
DLLs without running a second host-side .NET build.

`Invoke-DilockExample.ps1` loads the locally built Dilock .NET Redis, etcd, Consul and PostgreSQL assemblies; it does
not create another lock engine or a PowerShell package. By default it builds both existing backend
projects without restore and checks the public identity API. Pass `-SkipBuild` only after those
Release assemblies have already been built. `-AssemblyDirectory` can point to one Release output
directory containing Core, Redis, Etcd, Consul and PostgreSQL assemblies when the artifacts were built by a consumer
project. Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_POSTGRES_CONNECTION_STRING` or the provisioned `DILOCK_EPOCH` to run the corresponding backend; otherwise set
`DILOCK_REDIS_URL` and the epoch to run Redis. If multiple endpoints are configured, autodetection fails
closed; choose one explicitly with `-Backend Redis`, `-Backend Etcd` or `-Backend Postgres`. An explicit backend whose
endpoint is missing is an error and never falls back to the other coordinator. With no endpoint or
selected backend without an epoch, the script stops before build or connection. With no backend
selected, the script does not run a live backend integration.

## Quick path: SDK defaults

Only the backend endpoint and approved epoch are required. The namespace
defaults to `default`, with a five-second wait and thirty-second lease. The
script's default path uses nested `try/finally` blocks and awaits `DisposeAsync`
synchronously at the PowerShell boundary, releasing the lease before disposing
its provider:

```powershell
$env:DILOCK_REDIS_URL = 'redis://127.0.0.1:6379'
$env:DILOCK_EPOCH = 'local-redis-epoch' # Must match already provisioned metadata.
pwsh -NoProfile -File sdk/dotnet/reuse/powershell/Invoke-DilockExample.ps1
```

## Explicit Redis configuration

Create immutable connection and acquisition options when the deployment needs
custom budgets or identity values. This complete API path sets the endpoint,
namespace, epoch, connect timeout, wait, lease, RPC and cleanup limits. Cleanup
is awaited in `finally`, independently of the acquisition token:

```powershell
$token = [Threading.CancellationToken]::None
$leaseOptions = [Dilock.LeaseOptions]::new()
$leaseOptions.LeaseDuration = [TimeSpan]::FromSeconds(45)
$leaseOptions.RpcTimeout = [TimeSpan]::FromSeconds(2)
$leaseOptions.CleanupTimeout = [TimeSpan]::FromSeconds(3)
$leaseOptions.AutoRenew = $false
$policy = [Dilock.AcquireOptions]::new()
$policy.WaitTimeout = [TimeSpan]::FromSeconds(12)
$policy.Lease = $leaseOptions

$providerOptions = [Dilock.Redis.RedisProviderOptions]::new()
$providerOptions.Endpoint = $env:DILOCK_REDIS_URL
$providerOptions.Namespace = 'billing'
$providerOptions.ExpectedEpoch = $env:DILOCK_EPOCH
$providerOptions.ConnectTimeout = [TimeSpan]::FromSeconds(4)
$providerOptions.DefaultAcquisition = $policy
$provider = [Dilock.Redis.RedisProvider]::ConnectAsync($providerOptions, $token).GetAwaiter().GetResult()
try {
    $lease = $provider.Lock('invoice/42/reconcile').AcquireAsync($policy, $token).GetAwaiter().GetResult()
    try {
        if ($lease.State -ne [Dilock.LeaseState]::Held) {
            throw "Lease is not held according to the SDK's local state: $($lease.State)"
        }
        # Held is a conservative local observation, not a remote ownership proof.
        # Perform bounded, idempotent application work here.
        $outcome = $lease.ReleaseAsync().GetAwaiter().GetResult()
        if ($outcome -ne [Dilock.ReleaseOutcome]::Released) {
            throw "Redis release was not acknowledged: $outcome"
        }
    }
    finally {
        $lease.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
    }
}
finally {
    $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
}
```

Use the equivalent `EtcdProviderOptions` and `EtcdProvider.ConnectAsync` for
etcd. Use `New-DilockConsulProvider` or `ConsulProviderOptions` for Consul; the
ACL token is read from `CONSUL_HTTP_TOKEN`, and Consul requires a 10-second-minimum TTL. Do not provision namespace metadata from ordinary application startup.
For PostgreSQL, use `New-DilockPostgresProvider -ConnectionString $env:DILOCK_POSTGRES_CONNECTION_STRING -Epoch $env:DILOCK_EPOCH`.
The connection string must target the provisioned namespace metadata; the PostgreSQL provider owns one physical session per acquisition and has no server TTL or fencing.
The module provider constructors and standalone example install the .NET
`CleanupObserver`; if best-effort scope disposal cannot confirm cleanup, they
write the backend and reason to standard error. Reconcile the protected resource
before assuming the remote lock is present or absent.
The one-acquisition form is also available by passing an `AcquireOptions`
object to the handle API:

```powershell
$leaseOptions = New-Object Dilock.LeaseOptions -Property @{
    LeaseDuration = [TimeSpan]::FromSeconds(45)
}
$policy = New-Object Dilock.AcquireOptions -Property @{
    WaitTimeout = [TimeSpan]::FromSeconds(12)
    Lease = $leaseOptions
}
$lease = $handle.AcquireAsync($policy, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
```

That final fragment assumes `$handle` is a public resource handle created from
an already connected provider. It only customizes acquisition policy; use the
complete example above when connection and namespace settings also need to be
explicit.

Do not hide backend errors or treat them as contention. Cancellation is accepted by the .NET SDK's
cancellation-token overloads.

Run from the repository root with `pwsh -NoProfile -File sdk/dotnet/reuse/powershell/Invoke-DilockExample.ps1`.
Use `-Backend Redis` or `-Backend Etcd` to select explicitly when both endpoint
variables exist. The selection regression can be run after building the F#
consumer assemblies with:

```powershell
pwsh -NoProfile -File sdk/dotnet/reuse/powershell/tests/Test-BackendSelection.ps1 `
  -AssemblyDirectory sdk/dotnet/reuse/fsharp/bin/Release/net8.0
```

For an existing build produced by an F#, VB.NET, or other .NET consumer project,
pass `-SkipBuild -AssemblyDirectory <directory-containing-Core-Redis-Etcd-Consul-dlls>`.
The endpoint and epoch variables must refer to an already provisioned backend and matching
namespace metadata; this consumer does not provision etcd metadata or publish a PowerShell module.
