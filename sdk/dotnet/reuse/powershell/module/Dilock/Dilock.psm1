Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:LibraryDirectory = Join-Path $PSScriptRoot 'lib'
if (-not (Test-Path -LiteralPath $script:LibraryDirectory -PathType Container)) {
    throw "Dilock module runtime dependencies are missing: $script:LibraryDirectory. Build the local module package first."
}

# PowerShell does not probe a script module's lib directory for transitive DLLs.
# The managed resolver is safe to call from .NET worker threads; do not enter a
# PowerShell scriptblock from AssemblyResolve, which may run outside the module runspace.
$script:ResolverPath = Join-Path $script:LibraryDirectory 'Dilock.Runtime.Bundle.dll'
if (-not (Test-Path -LiteralPath $script:ResolverPath -PathType Leaf)) {
    throw "Dilock module runtime resolver is missing: $script:ResolverPath"
}
[System.Reflection.Assembly]::LoadFrom($script:ResolverPath) | Out-Null
[Dilock.PowerShell.ModuleAssemblyResolver]::Register($script:LibraryDirectory)
$ExecutionContext.SessionState.Module.OnRemove = {
    [Dilock.PowerShell.ModuleAssemblyResolver]::Unregister()
}.GetNewClosure()

foreach ($assemblyName in @('Dilock.Core.dll', 'Dilock.Redis.dll', 'Dilock.Etcd.dll', 'Dilock.Consul.dll', 'Dilock.Postgres.dll')) {
    $assemblyPath = Join-Path $script:LibraryDirectory $assemblyName
    if (-not (Test-Path -LiteralPath $assemblyPath -PathType Leaf)) {
        throw "Dilock module assembly is missing: $assemblyPath"
    }
    [System.Reflection.Assembly]::LoadFrom($assemblyPath) | Out-Null
}

function New-DilockAcquireOptions {
    param(
        [Parameter(Mandatory)][TimeSpan] $WaitTimeout,
        [Parameter(Mandatory)][TimeSpan] $LeaseDuration,
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing
    )

    $lease = [Dilock.LeaseOptions]::new()
    $lease.LeaseDuration = $LeaseDuration
    $lease.RpcTimeout = $RpcTimeout
    $lease.CleanupTimeout = $CleanupTimeout
    $lease.AutoRenew = $AutoRenew.IsPresent
    $lease.RequireFencing = $RequireFencing.IsPresent

    $options = [Dilock.AcquireOptions]::new()
    $options.WaitTimeout = $WaitTimeout
    $options.Lease = $lease
    $options.Validate()
    return $options
}

function New-DilockCleanupObserver {
    return [System.Action[Dilock.CleanupObservation]] {
        param([Dilock.CleanupObservation] $Observation)
        if ($Observation.Outcome -ne [Dilock.ReleaseOutcome]::Unknown) { return }
        try {
            [Console]::Error.WriteLine(
                'Dilock cleanup is unknown ({0}, {1}); reconcile the protected resource before assuming cleanup.',
                $Observation.Backend,
                $Observation.Reason)
        }
        catch {
            # Diagnostics must never replace the result of bounded cleanup.
        }
    }
}

function New-DilockRedisProvider {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Endpoint,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
        [ValidatePattern('^[a-z0-9][a-z0-9_.-]{0,63}$')][string] $Namespace = 'default',
        [TimeSpan] $ConnectTimeout = ([TimeSpan]::FromSeconds(2)),
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout = ([TimeSpan]::FromSeconds(5)),
        [TimeSpan] $LeaseDuration = ([TimeSpan]::FromSeconds(30)),
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )

    $policyParameters = @('WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')
    $hasIndividualPolicy = $false
    foreach ($name in $policyParameters) {
        if ($PSBoundParameters.ContainsKey($name)) { $hasIndividualPolicy = $true; break }
    }
    if ($PSBoundParameters.ContainsKey('Options')) {
        if ($hasIndividualPolicy) { throw 'Pass either -Options or individual acquisition options, not both.' }
        if ($null -eq $Options) { throw '-Options cannot be null.' }
        $Options.Validate()
        $acquisition = $Options
    }
    else {
        $acquisition = New-DilockAcquireOptions -WaitTimeout $WaitTimeout -LeaseDuration $LeaseDuration `
            -RpcTimeout $RpcTimeout -CleanupTimeout $CleanupTimeout -AutoRenew:$AutoRenew -RequireFencing:$RequireFencing
    }
    $configuration = [Dilock.Redis.RedisProviderOptions]::new()
    $configuration.Endpoint = $Endpoint
    $configuration.Namespace = $Namespace
    $configuration.ExpectedEpoch = $Epoch
    $configuration.ConnectTimeout = $ConnectTimeout
    $configuration.DefaultAcquisition = $acquisition
    $configuration.CleanupObserver = New-DilockCleanupObserver
    return [Dilock.Redis.RedisProvider]::ConnectAsync($configuration, $CancellationToken).GetAwaiter().GetResult()
}

function New-DilockEtcdProvider {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Endpoint,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
        [ValidatePattern('^[a-z0-9][a-z0-9_.-]{0,63}$')][string] $Namespace = 'default',
        [TimeSpan] $MetadataTimeout = ([TimeSpan]::FromSeconds(2)),
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout = ([TimeSpan]::FromSeconds(5)),
        [TimeSpan] $LeaseDuration = ([TimeSpan]::FromSeconds(30)),
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )

    $policyParameters = @('WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')
    $hasIndividualPolicy = $false
    foreach ($name in $policyParameters) {
        if ($PSBoundParameters.ContainsKey($name)) { $hasIndividualPolicy = $true; break }
    }
    if ($PSBoundParameters.ContainsKey('Options')) {
        if ($hasIndividualPolicy) { throw 'Pass either -Options or individual acquisition options, not both.' }
        if ($null -eq $Options) { throw '-Options cannot be null.' }
        $Options.Validate()
        $acquisition = $Options
    }
    else {
        $acquisition = New-DilockAcquireOptions -WaitTimeout $WaitTimeout -LeaseDuration $LeaseDuration `
            -RpcTimeout $RpcTimeout -CleanupTimeout $CleanupTimeout -AutoRenew:$AutoRenew -RequireFencing:$RequireFencing
    }
    $configuration = [Dilock.Etcd.EtcdProviderOptions]::new()
    $configuration.Endpoint = $Endpoint
    $configuration.Namespace = $Namespace
    $configuration.ExpectedEpoch = $Epoch
    $configuration.MetadataTimeout = $MetadataTimeout
    $configuration.DefaultAcquisition = $acquisition
    $configuration.CleanupObserver = New-DilockCleanupObserver
    return [Dilock.Etcd.EtcdProvider]::ConnectAsync($configuration, $CancellationToken).GetAwaiter().GetResult()
}

function New-DilockConsulProvider {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Endpoint,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
        [ValidatePattern('^[a-z0-9][a-z0-9_.-]{0,63}$')][string] $Namespace = 'default',
        [TimeSpan] $MetadataTimeout = ([TimeSpan]::FromSeconds(2)),
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout = ([TimeSpan]::FromSeconds(5)),
        [TimeSpan] $LeaseDuration = ([TimeSpan]::FromSeconds(30)),
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )
    $policyParameters = @('WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')
    $hasIndividualPolicy = $false
    foreach ($name in $policyParameters) { if ($PSBoundParameters.ContainsKey($name)) { $hasIndividualPolicy = $true; break } }
    if ($PSBoundParameters.ContainsKey('Options')) {
        if ($hasIndividualPolicy) { throw 'Pass either -Options or individual acquisition options, not both.' }
        if ($null -eq $Options) { throw '-Options cannot be null.' }
        $Options.Validate()
        $acquisition = $Options
    }
    else {
        $acquisition = New-DilockAcquireOptions -WaitTimeout $WaitTimeout -LeaseDuration $LeaseDuration `
            -RpcTimeout $RpcTimeout -CleanupTimeout $CleanupTimeout -AutoRenew:$AutoRenew -RequireFencing:$RequireFencing
    }
    $configuration = [Dilock.Consul.ConsulProviderOptions]::new()
    $configuration.Endpoint = $Endpoint
    $configuration.Namespace = $Namespace
    $configuration.ExpectedEpoch = $Epoch
    $consulToken = [Environment]::GetEnvironmentVariable('CONSUL_HTTP_TOKEN')
    if (-not [string]::IsNullOrWhiteSpace($consulToken)) { $configuration.Token = $consulToken }
    $configuration.MetadataTimeout = $MetadataTimeout
    $configuration.DefaultAcquisition = $acquisition
    $configuration.CleanupObserver = New-DilockCleanupObserver
    return [Dilock.Consul.ConsulProvider]::ConnectAsync($configuration, $CancellationToken).GetAwaiter().GetResult()
}

function New-DilockPostgresProvider {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $ConnectionString,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
        [ValidatePattern('^[a-z0-9][a-z0-9_.-]{0,63}$')][string] $Namespace = 'default',
        [TimeSpan] $ConnectTimeout = ([TimeSpan]::FromSeconds(2)),
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout = ([TimeSpan]::FromSeconds(5)),
        [TimeSpan] $LeaseDuration = ([TimeSpan]::FromSeconds(30)),
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )
    $policyParameters = @('WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')
    $hasIndividualPolicy = $false
    foreach ($name in $policyParameters) { if ($PSBoundParameters.ContainsKey($name)) { $hasIndividualPolicy = $true; break } }
    if ($PSBoundParameters.ContainsKey('Options')) {
        if ($hasIndividualPolicy) { throw 'Pass either -Options or individual acquisition options, not both.' }
        if ($null -eq $Options) { throw '-Options cannot be null.' }
        $Options.Validate()
        $acquisition = $Options
    }
    else {
        $acquisition = New-DilockAcquireOptions -WaitTimeout $WaitTimeout -LeaseDuration $LeaseDuration `
            -RpcTimeout $RpcTimeout -CleanupTimeout $CleanupTimeout -AutoRenew:$AutoRenew -RequireFencing:$RequireFencing
    }
    $configuration = [Dilock.Postgres.PostgresProviderOptions]::new()
    $configuration.ConnectionString = $ConnectionString
    $configuration.Namespace = $Namespace
    $configuration.ExpectedEpoch = $Epoch
    $configuration.ConnectTimeout = $ConnectTimeout
    $configuration.DefaultAcquisition = $acquisition
    $configuration.CleanupObserver = New-DilockCleanupObserver
    return [Dilock.Postgres.PostgresProvider]::ConnectAsync($configuration, $CancellationToken).GetAwaiter().GetResult()
}

function Get-DilockLock {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, ValueFromPipeline)][Dilock.ILockProvider] $Provider,
        [Parameter(Mandatory, Position = 0)][ValidateNotNullOrEmpty()][string] $Resource
    )
    process { return $Provider.Lock($Resource) }
}

function Enter-Dilock {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, ValueFromPipeline)][Dilock.ILockHandle] $Lock,
        [TimeSpan] $WaitTimeout = ([TimeSpan]::FromSeconds(5)),
        [TimeSpan] $LeaseDuration = ([TimeSpan]::FromSeconds(30)),
        [TimeSpan] $RpcTimeout = ([TimeSpan]::FromSeconds(2)),
        [TimeSpan] $CleanupTimeout = ([TimeSpan]::FromSeconds(2)),
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )
    process {
        $acquisition = @{}
        foreach ($name in @('Options', 'WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')) {
            if ($PSBoundParameters.ContainsKey($name)) { $acquisition[$name] = $PSBoundParameters[$name] }
        }
        $acquisition.Lock = $Lock
        $acquisition.CancellationToken = $CancellationToken
        return Invoke-DilockAcquire @acquisition
    }
}

function Exit-Dilock {
    [CmdletBinding()]
    param([Parameter(Mandatory, ValueFromPipeline)][Dilock.ILease] $Lease)
    process { return $Lease.ReleaseAsync().GetAwaiter().GetResult() }
}

function Close-DilockLease {
    [CmdletBinding()]
    param([Parameter(Mandatory, ValueFromPipeline)][Dilock.ILease] $Lease)
    process { $Lease.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
}

function Close-DilockProvider {
    [CmdletBinding()]
    param([Parameter(Mandatory, ValueFromPipeline)][Dilock.ILockProvider] $Provider)
    process { $Provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null }
}

function Invoke-DilockAcquire {
    param(
        [Parameter(Mandatory)][object] $Lock,
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout,
        [TimeSpan] $LeaseDuration,
        [TimeSpan] $RpcTimeout,
        [TimeSpan] $CleanupTimeout,
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )

    $individualOptions = @('WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')
    $hasIndividualOptions = $false
    foreach ($name in $individualOptions) {
        if ($PSBoundParameters.ContainsKey($name)) { $hasIndividualOptions = $true; break }
    }
    if ($PSBoundParameters.ContainsKey('Options') -and $hasIndividualOptions) {
        throw 'Pass either -Options or individual acquisition options, not both.'
    }
    if ($PSBoundParameters.ContainsKey('Options')) {
        return $Lock.AcquireAsync($Options.WaitTimeout, $Options.Lease, $CancellationToken).GetAwaiter().GetResult()
    }
    if (-not $hasIndividualOptions) {
        # Preserve the provider's configured DefaultAcquisition policy.
        $default = New-DilockAcquireOptions -WaitTimeout ([TimeSpan]::FromSeconds(5)) -LeaseDuration ([TimeSpan]::FromSeconds(30))
        return $Lock.AcquireAsync($default.WaitTimeout, $default.Lease, $CancellationToken).GetAwaiter().GetResult()
    }

    $lease = [Dilock.LeaseOptions]::new()
    if ($PSBoundParameters.ContainsKey('LeaseDuration')) { $lease.LeaseDuration = $LeaseDuration }
    if ($PSBoundParameters.ContainsKey('RpcTimeout')) { $lease.RpcTimeout = $RpcTimeout }
    if ($PSBoundParameters.ContainsKey('CleanupTimeout')) { $lease.CleanupTimeout = $CleanupTimeout }
    if ($PSBoundParameters.ContainsKey('AutoRenew')) { $lease.AutoRenew = $AutoRenew.IsPresent }
    if ($PSBoundParameters.ContainsKey('RequireFencing')) { $lease.RequireFencing = $RequireFencing.IsPresent }
    $options = [Dilock.AcquireOptions]::new()
    if ($PSBoundParameters.ContainsKey('WaitTimeout')) { $options.WaitTimeout = $WaitTimeout }
    $options.Lease = $lease
    $options.Validate()
    return $Lock.AcquireAsync($options, $CancellationToken).GetAwaiter().GetResult()
}

function Invoke-DilockLeaseScope {
    param(
        [Parameter(Mandatory)] $Lease,
        [Parameter(Mandatory)][scriptblock] $ScriptBlock
    )

    $bodyFailure = $null
    try {
        & $ScriptBlock $Lease
        $releaseOutcome = Exit-Dilock -Lease $Lease
        if ($releaseOutcome.ToString() -ine 'Released') {
            throw [InvalidOperationException]::new(
                "Lease scope did not receive a Released acknowledgement: $releaseOutcome")
        }
        return $releaseOutcome
    }
    catch {
        $bodyFailure = $_
        throw
    }
    finally {
        try {
            Close-DilockLease -Lease $Lease
        }
        catch {
            if ($null -eq $bodyFailure) { throw }

            # Keep the application or explicit-release error primary, while
            # retaining the cleanup failure for callers investigating an
            # unknown release acknowledgement.
            try {
                $bodyFailure.Exception.Data['Dilock.CleanupFailure'] = $_.Exception
            }
            catch {
                try { [Console]::Error.WriteLine("Dilock lease cleanup also failed: $($_.Exception.Message)") }
                catch { }
            }
        }
    }
}

function Invoke-Dilock {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Dilock.ILockHandle] $Lock,
        [Parameter(Mandatory)][scriptblock] $ScriptBlock,
        [Dilock.AcquireOptions] $Options,
        [TimeSpan] $WaitTimeout,
        [TimeSpan] $LeaseDuration,
        [TimeSpan] $RpcTimeout,
        [TimeSpan] $CleanupTimeout,
        [switch] $AutoRenew,
        [switch] $RequireFencing,
        [System.Threading.CancellationToken] $CancellationToken = [System.Threading.CancellationToken]::None
    )

    $acquisition = @{}
    foreach ($name in @('Options', 'WaitTimeout', 'LeaseDuration', 'RpcTimeout', 'CleanupTimeout', 'AutoRenew', 'RequireFencing')) {
        if ($PSBoundParameters.ContainsKey($name)) { $acquisition[$name] = $PSBoundParameters[$name] }
    }
    $acquisition.Lock = $Lock
    $acquisition.CancellationToken = $CancellationToken
    $lease = Enter-Dilock @acquisition
    return Invoke-DilockLeaseScope -Lease $lease -ScriptBlock $ScriptBlock
}

Export-ModuleMember -Function @(
    'New-DilockRedisProvider', 'New-DilockEtcdProvider', 'New-DilockConsulProvider', 'New-DilockPostgresProvider', 'Get-DilockLock',
    'Enter-Dilock', 'Exit-Dilock', 'Close-DilockLease', 'Close-DilockProvider', 'Invoke-Dilock'
)
