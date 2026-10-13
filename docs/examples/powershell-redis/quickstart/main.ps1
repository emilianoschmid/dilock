$ErrorActionPreference = 'Stop'
$assemblyDirectory = $env:DILOCK_ASSEMBLY_DIR
if (-not $assemblyDirectory) { throw 'Set DILOCK_ASSEMBLY_DIR to the built Dilock .NET assembly directory.' }
foreach ($name in 'Dilock.Core.dll', 'Dilock.Redis.dll') {
    Add-Type -Path (Join-Path $assemblyDirectory $name)
}

$epoch = $env:DILOCK_COORDINATION_EPOCH
if (-not $epoch) { throw 'Set DILOCK_COORDINATION_EPOCH to the provisioned epoch.' }
$options = [Dilock.Redis.RedisProviderOptions]::new()
$options.Endpoint = if ($env:DILOCK_REDIS_URL) { $env:DILOCK_REDIS_URL } else { 'redis://127.0.0.1:6379' }
$options.Namespace = if ($env:DILOCK_NAMESPACE) { $env:DILOCK_NAMESPACE } else { 'default' }
$options.ExpectedEpoch = $epoch
$provider = [Dilock.Redis.RedisProvider]::ConnectAsync($options, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
try {
    $lease = $provider.Lock('reports/daily').AcquireAsync([Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($lease.State -ne [Dilock.LeaseState]::Held) { throw 'Lease is no longer locally held.' }
        # Replace with one bounded, idempotent unit of work.
        $outcome = $lease.ReleaseAsync().GetAwaiter().GetResult()
        if ($outcome -ne [Dilock.ReleaseOutcome]::Released) { throw "Redis release was not acknowledged: $outcome" }
    }
    finally { $lease.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }

    # Sequential second resource on the same provider/client with explicit timing.
    $leaseOptions = New-Object Dilock.LeaseOptions -Property @{ LeaseDuration = [TimeSpan]::FromSeconds(45) }
    $policy = New-Object Dilock.AcquireOptions -Property @{
        WaitTimeout = [TimeSpan]::FromSeconds(12)
        Lease = $leaseOptions
    }
    $monthly = $provider.Lock('reports/monthly').AcquireAsync($policy, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($monthly.State -ne [Dilock.LeaseState]::Held) { throw 'Monthly lease is no longer locally held.' }
        $outcome = $monthly.ReleaseAsync().GetAwaiter().GetResult()
        if ($outcome -ne [Dilock.ReleaseOutcome]::Released) { throw "Redis monthly release was not acknowledged: $outcome" }
    }
    finally { $monthly.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
}
finally { $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
