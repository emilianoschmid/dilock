[CmdletBinding()]
param(
    [string] $Endpoint = $env:DILOCK_REDIS_URL,
    [string] $EtcdEndpoint = $env:DILOCK_ETCD_ENDPOINT,
    [ValidateSet('Auto', 'Redis', 'Etcd')]
    [string] $Backend = 'Auto',
    [string] $Namespace = $env:DILOCK_NAMESPACE,
    [string] $Epoch = $env:DILOCK_EPOCH,
    [switch] $SkipBuild,
    [string] $AssemblyDirectory
)

$ErrorActionPreference = 'Stop'
$hasRedisEndpoint = -not [string]::IsNullOrWhiteSpace($Endpoint)
$hasEtcdEndpoint = -not [string]::IsNullOrWhiteSpace($EtcdEndpoint)
if ($Backend -eq 'Auto') {
    if ($hasRedisEndpoint -and $hasEtcdEndpoint) {
        throw 'Both Redis and etcd endpoints are configured; pass -Backend Redis or -Backend Etcd to select the authority explicitly.'
    }
    $selectedBackend = if ($hasEtcdEndpoint) { 'Etcd' } elseif ($hasRedisEndpoint) { 'Redis' } else { 'None' }
}
else {
    $selectedBackend = $Backend
    if ($selectedBackend -eq 'Redis' -and -not $hasRedisEndpoint) {
        throw '-Backend Redis requires -Endpoint or DILOCK_REDIS_URL; no other coordinator will be selected.'
    }
    if ($selectedBackend -eq 'Etcd' -and -not $hasEtcdEndpoint) {
        throw '-Backend Etcd requires -EtcdEndpoint or DILOCK_ETCD_ENDPOINT; no other coordinator will be selected.'
    }
}
if ($selectedBackend -ne 'None' -and [string]::IsNullOrWhiteSpace($Epoch)) {
    throw 'A selected Dilock backend requires -Epoch or DILOCK_EPOCH; no coordinator connection was attempted.'
}

$sdkRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
$redisProject = Join-Path $sdkRoot 'Dilock.Redis/Dilock.Redis.csproj'
$etcdProject = Join-Path $sdkRoot 'Dilock.Etcd/Dilock.Etcd.csproj'
if (-not $SkipBuild) {
    dotnet build $redisProject -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Dilock.Redis build failed: $LASTEXITCODE" }
    dotnet build $etcdProject -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Dilock.Etcd build failed: $LASTEXITCODE" }
}
else {
    Write-Output 'Using existing Release assemblies; build skipped by request.'
}

$redisOutput = Join-Path $sdkRoot 'Dilock.Redis/bin/Release/net8.0'
$etcdOutput = Join-Path $sdkRoot 'Dilock.Etcd/bin/Release/net8.0'
$assemblyPaths = if ([string]::IsNullOrWhiteSpace($AssemblyDirectory)) {
    @((Join-Path $redisOutput 'Dilock.Core.dll'),
      (Join-Path $redisOutput 'Dilock.Redis.dll'),
      (Join-Path $etcdOutput 'Dilock.Etcd.dll'))
}
else {
    @((Join-Path $AssemblyDirectory 'Dilock.Core.dll'),
      (Join-Path $AssemblyDirectory 'Dilock.Redis.dll'),
      (Join-Path $AssemblyDirectory 'Dilock.Etcd.dll'))
}
foreach ($path in $assemblyPaths) {
    if (-not (Test-Path $path)) { throw "Required local SDK assembly missing: $path" }
    Add-Type -Path $path
}

$effectiveNamespace = if ([string]::IsNullOrWhiteSpace($Namespace)) { 'default' } else { $Namespace }
$identity = [Dilock.ResourceIdentity]::Hash($effectiveNamespace, 'powershell/consumer')
if ($identity.Length -ne 64) { throw 'Dilock identity API is unavailable.' }
$policy = [Dilock.AcquireOptions]::new()
$policy.Validate()
if ($policy.WaitTimeout -ne [TimeSpan]::FromSeconds(5)) { throw 'Unexpected SDK wait default.' }
$configuredLease = New-Object Dilock.LeaseOptions -Property @{
    LeaseDuration = [TimeSpan]::FromSeconds(45)
}
$configuredPolicy = New-Object Dilock.AcquireOptions -Property @{
    WaitTimeout = [TimeSpan]::FromSeconds(12)
    Lease = $configuredLease
}
$configuredPolicy.Validate()
if ($configuredPolicy.Lease.TtlMilliseconds -ne 45000) { throw 'Configured lease option was not applied.' }

$cleanupObserver = [System.Action[Dilock.CleanupObservation]] {
    param([Dilock.CleanupObservation] $Observation)
    if ($Observation.Outcome -eq [Dilock.ReleaseOutcome]::Unknown) {
        [Console]::Error.WriteLine(
            'Dilock cleanup is unknown ({0}, {1}); reconcile the protected resource before assuming cleanup.',
            $Observation.Backend,
            $Observation.Reason)
    }
}

function Invoke-DilockLifecycle($Provider, [string] $Backend) {
    $resource = 'powershell/consumer/' + [Guid]::NewGuid().ToString('N')
    # Two handles from one provider share its connection/channel; they still
    # represent independent acquisitions of the same distributed resource.
    $ownerHandle = $Provider.Lock($resource)
    $waiterHandle = $Provider.Lock($resource)
    $owner = $ownerHandle.AcquireAsync([Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($owner.State -ne [Dilock.LeaseState]::Held) { throw "$Backend owner did not enter Held state." }

        $waitPolicy = New-Object Dilock.AcquireOptions -Property @{
            WaitTimeout = [TimeSpan]::FromSeconds(8)
            Lease = [Dilock.LeaseOptions]::new()
        }
        $cancellation = [Threading.CancellationTokenSource]::new()
        try {
            $waiting = $waiterHandle.AcquireAsync($waitPolicy, $cancellation.Token)
            Start-Sleep -Milliseconds 400
            $cancellation.Cancel()
            $wasCancelled = $false
            try {
                $unexpected = $waiting.GetAwaiter().GetResult()
                if ($null -ne $unexpected) {
                    $unexpected.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
                }
            }
            catch [OperationCanceledException] {
                $wasCancelled = $true
            }
            if (-not $wasCancelled) { throw "$Backend queued acquisition did not report cancellation." }
            if ($owner.State -ne [Dilock.LeaseState]::Held) { throw "$Backend waiter cancellation changed owner state." }
        }
        finally {
            $cancellation.Dispose()
        }

        $release = $owner.ReleaseAsync().GetAwaiter().GetResult()
        if ($release -ne [Dilock.ReleaseOutcome]::Released) { throw "$Backend explicit owner release was not acknowledged: $release" }
        $repeated = $owner.ReleaseAsync().GetAwaiter().GetResult()
        if ($repeated -ne $release) { throw "$Backend repeated release changed its outcome." }
    }
    finally {
        $owner.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
    }

    $successor = $waiterHandle.AcquireAsync([Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        if ($successor.State -ne [Dilock.LeaseState]::Held) { throw "$Backend successor did not acquire after handoff." }
        $successorRelease = $successor.ReleaseAsync().GetAwaiter().GetResult()
        if ($successorRelease -ne [Dilock.ReleaseOutcome]::Released) { throw "$Backend successor release was not acknowledged." }
    }
    finally {
        $successor.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
    }
}

if ([string]::IsNullOrWhiteSpace($Epoch)) {
    Write-Output 'PowerShell consumer API passed; live Redis/etcd NOT_RUN (set DILOCK_EPOCH and a backend endpoint).'
    return
}

if ($selectedBackend -eq 'Etcd') {
    # Etcd requires the provisioned namespace metadata to already match this epoch.
    $providerOptions = [Dilock.Etcd.EtcdProviderOptions]::new()
    $providerOptions.Endpoint = $EtcdEndpoint
    $providerOptions.Namespace = $effectiveNamespace
    $providerOptions.ExpectedEpoch = $Epoch
    $providerOptions.CleanupObserver = $cleanupObserver
    $provider = [Dilock.Etcd.EtcdProvider]::ConnectAsync(
        $providerOptions, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    try {
        Invoke-DilockLifecycle $provider 'etcd'
    }
    finally {
        $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
    }
    Write-Output 'PowerShell consumer and etcd operation passed.'
    return
}

if ($selectedBackend -ne 'Redis') {
    Write-Output 'PowerShell consumer API passed; live Redis/etcd NOT_RUN (set DILOCK_REDIS_URL or DILOCK_ETCD_ENDPOINT).'
    return
}

# The nested finally blocks are the PowerShell equivalent of an async using scope and ensure
# both SDK-owned resources are disposed after the Redis operation.
$providerOptions = [Dilock.Redis.RedisProviderOptions]::new()
$providerOptions.Endpoint = $Endpoint
$providerOptions.Namespace = $effectiveNamespace
$providerOptions.ExpectedEpoch = $Epoch
$providerOptions.CleanupObserver = $cleanupObserver
$provider = [Dilock.Redis.RedisProvider]::ConnectAsync(
    $providerOptions, [Threading.CancellationToken]::None
).GetAwaiter().GetResult()
try {
    Invoke-DilockLifecycle $provider 'Redis'
}
finally {
    $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
}
Write-Output 'PowerShell consumer and Redis operation passed.'
