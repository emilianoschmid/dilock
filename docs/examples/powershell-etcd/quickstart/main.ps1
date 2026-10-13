$ErrorActionPreference = 'Stop'
$assemblyDirectory = $env:DILOCK_ASSEMBLY_DIR
if (-not $assemblyDirectory) { throw 'Set DILOCK_ASSEMBLY_DIR to the built Dilock .NET assembly directory.' }
foreach ($name in 'Dilock.Core.dll', 'Dilock.Etcd.dll') {
    Add-Type -Path (Join-Path $assemblyDirectory $name)
}

$epoch = $env:DILOCK_COORDINATION_EPOCH
if (-not $epoch) { throw 'Set DILOCK_COORDINATION_EPOCH to the provisioned epoch.' }
$options = [Dilock.Etcd.EtcdProviderOptions]::new()
$options.Endpoint = if ($env:DILOCK_ETCD_ENDPOINT) { $env:DILOCK_ETCD_ENDPOINT } else { 'http://127.0.0.1:2379' }
$options.Namespace = if ($env:DILOCK_NAMESPACE) { $env:DILOCK_NAMESPACE } else { 'default' }
$options.ExpectedEpoch = $epoch
$provider = [Dilock.Etcd.EtcdProvider]::ConnectAsync($options, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
try {
    $lease = $provider.Lock('reports/daily').AcquireAsync([Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($lease.State -ne [Dilock.LeaseState]::Held) { throw 'Lease is no longer locally held.' }
        $fence = $lease.Fence
        # Enforce epoch/revision atomically at the protected resource.
        Write-Output "Applying work under fence $($fence.Epoch)/$($fence.Value)"
        $outcome = $lease.ReleaseAsync().GetAwaiter().GetResult()
        if ($outcome -ne [Dilock.ReleaseOutcome]::Released) { throw "etcd release was not acknowledged: $outcome" }
    }
    finally { $lease.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }

    # Sequential second resource shares this provider channel and uses an override.
    $leaseOptions = New-Object Dilock.LeaseOptions -Property @{ LeaseDuration = [TimeSpan]::FromSeconds(45) }
    $policy = New-Object Dilock.AcquireOptions -Property @{
        WaitTimeout = [TimeSpan]::FromSeconds(12)
        Lease = $leaseOptions
    }
    $monthly = $provider.Lock('reports/monthly').AcquireAsync($policy, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($monthly.State -ne [Dilock.LeaseState]::Held) { throw 'Monthly lease is no longer locally held.' }
        $monthlyFence = $monthly.Fence
        Write-Output "Applying work under fence $($monthlyFence.Epoch)/$($monthlyFence.Value)"
        $outcome = $monthly.ReleaseAsync().GetAwaiter().GetResult()
        if ($outcome -ne [Dilock.ReleaseOutcome]::Released) { throw "etcd monthly release was not acknowledged: $outcome" }
    }
    finally { $monthly.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
}
finally { $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
