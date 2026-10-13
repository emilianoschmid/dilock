# Dilock PowerShell module (local candidate)

This module is a PowerShell facade over the existing Dilock .NET Core, Redis,
and etcd assemblies. It adds no lock protocol or transport. It targets
PowerShell 7.4 or later and currently has no PowerShell Gallery identity or
publication status.

Build a local installable ZIP from the repository root:

```powershell
pwsh -NoProfile -File sdk/dotnet/reuse/powershell/Build-DilockModule.ps1
```

The script restores in locked mode only from the local NuGet package cache (using
`-NuGetCacheSource` to select that local directory), publishes a small
dependency-bundle project that references both current Redis and etcd .NET
projects, stages the single resolved transitive DLL closure in the module's
private `lib` folder, and writes
`.tmp/powershell-module/Dilock.PowerShell-0.1.0-local.zip`. Restore the pinned
.NET package dependencies into that cache separately when it is incomplete;
the package script never uses a remote NuGet source. The generated module directory or ZIP can be copied to a
PowerShell module path and imported with `Import-Module Dilock`.

Namespace metadata and its epoch must already be provisioned by deployment
infrastructure. Module startup never provisions metadata or falls back between
backends. Redis provides owner-token-checked cleanup but does not provide
strong fencing across asynchronous-replication failover. etcd exposes
`CreateRevision` on the lease; protected resources must validate its epoch and
revision atomically when fencing is required.

## Redis quick path

```powershell
Import-Module .\Dilock\Dilock.psd1
$provider = New-DilockRedisProvider -Endpoint 'redis://127.0.0.1:6379' `
    -Namespace 'default' -Epoch $env:DILOCK_EPOCH
try {
    $handle = Get-DilockLock -Provider $provider -Resource 'billing/invoice/42'
    Invoke-Dilock -Lock $handle -ScriptBlock {
        param($lease)
        if ($lease.State -ne [Dilock.LeaseState]::Held) { throw 'Lease is not held.' }
        # Keep protected work bounded and make external effects idempotent/fenced.
    }
}
finally {
    Close-DilockProvider -Provider $provider
}
```

`New-DilockRedisProvider` and `New-DilockEtcdProvider` accept a complete
`-Options` policy or `-WaitTimeout`, `-LeaseDuration`, `-RpcTimeout`,
`-CleanupTimeout`, `-AutoRenew`, and `-RequireFencing` as the provider's default
acquisition policy. Combining `-Options` with individual policy values fails
explicitly. `Enter-Dilock`
and `Invoke-Dilock` with no acquisition options preserve that configured policy.
Pass a complete `-Options` value or individual policy parameters to override it
for one acquisition; those two forms cannot be combined.

With an unconfigured provider, `Invoke-Dilock` waits up to five seconds by
default and requests a thirty-second lease. It invokes the script block with
the acquired .NET `ILease`, checks the
release result (including the explicit `Unknown` enum outcome), and disposes the
lease in `finally`. An `Unknown` result must be reconciled before retrying
protected work. Exceptions from connect,
acquisition, cancellation, backend operations, and the script block propagate;
only an acknowledged acquisition is passed to the body. Disposal is bounded by
the .NET cleanup budget and does not cancel already-running protected work. If
the body or explicit release throws and final disposal also throws, the original
error remains primary and the cleanup exception is attached at
`Exception.Data['Dilock.CleanupFailure']` for diagnosis.

Both provider constructors install the .NET `CleanupObserver`. If best-effort
lease or provider scope cleanup cannot confirm the owner-checked release, the
module writes the backend and cleanup reason to standard error. A warning does
not determine whether the remote lock still exists; reconcile the protected
resource before assuming either outcome. Direct .NET provider construction
should set `CleanupObserver` explicitly when callers rely on implicit disposal.

## Explicit etcd configuration and ownership

```powershell
$provider = New-DilockEtcdProvider -Endpoint 'http://127.0.0.1:2379' `
    -Namespace 'billing' -Epoch $env:DILOCK_EPOCH `
    -MetadataTimeout ([TimeSpan]::FromSeconds(3))
try {
    $handle = Get-DilockLock -Provider $provider -Resource 'invoice/42/reconcile'
    $lease = Enter-Dilock $handle -WaitTimeout ([TimeSpan]::FromSeconds(12)) `
        -LeaseDuration ([TimeSpan]::FromSeconds(45)) -RequireFencing
    try {
        if ($lease.State -ne [Dilock.LeaseState]::Held) { throw 'Lease is not held.' }
        $fence = [pscustomobject]@{ Epoch = $lease.Epoch; Revision = $lease.CreateRevision }
        # Pass both values to a protected resource that atomically rejects stale revisions.
        $outcome = Exit-Dilock $lease
        if ($outcome -eq [Dilock.ReleaseOutcome]::Unknown) {
            throw 'Release acknowledgement is unknown; reconcile before retrying work.'
        }
    }
    finally { Close-DilockLease $lease }
}
finally { Close-DilockProvider $provider }
```

Provider and lease have separate lifetimes. Dispose both with `Close-DilockProvider`
and `Close-DilockLease` in `finally`; repeated lease release returns the cached
outcome. `Invoke-Dilock` returns only after receiving the `Released` acknowledgement;
if the lease is already lost or otherwise not owned, the scope throws instead of
reporting successful completion. An `Unknown` release is not contention and must
not be retried blindly.
`-CancellationToken` accepts a .NET `CancellationToken`; cancellation is never
converted to a busy result. The options `-AutoRenew` and `-RequireFencing` are
explicit capabilities, not guarantees that a suspended process stops.
