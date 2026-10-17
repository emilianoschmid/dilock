[CmdletBinding()]
param([Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $ModuleManifest)

$ErrorActionPreference = 'Stop'
foreach ($name in @('DILOCK_CONSUL_ENDPOINT', 'DILOCK_CONSUL_NAMESPACE', 'DILOCK_EPOCH')) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) {
        throw "Set $name to an explicitly provisioned disposable Consul namespace."
    }
}

Import-Module -Name (Resolve-Path -LiteralPath $ModuleManifest).Path
$provider = New-DilockConsulProvider -Endpoint $env:DILOCK_CONSUL_ENDPOINT `
    -Namespace $env:DILOCK_CONSUL_NAMESPACE -Epoch $env:DILOCK_EPOCH
try {
    $handle = $provider.Lock('powershell/live/' + [Guid]::NewGuid().ToString('N'))
    $lease = $handle.TryAcquireAsync().GetAwaiter().GetResult()
    if ($null -eq $lease -or -not ($lease -is [Dilock.Consul.IConsulLease])) {
        throw 'Consul provider did not return its public lease API.'
    }
    if ($lease.LockIndex -le 0 -or $lease.Epoch -ne $env:DILOCK_EPOCH) {
        throw 'Consul fence did not match the provisioned epoch and positive LockIndex.'
    }
    if ($lease.ReleaseAsync().GetAwaiter().GetResult() -ne [Dilock.ReleaseOutcome]::Released) {
        throw 'Consul release was not acknowledged.'
    }
    Write-Output "PASS PowerShell Consul lifecycle; LockIndex=$($lease.LockIndex)."
}
finally {
    $provider.DisposeAsync().AsTask().GetAwaiter().GetResult() | Out-Null
}
