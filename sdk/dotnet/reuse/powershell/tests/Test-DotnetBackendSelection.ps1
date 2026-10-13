[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $FSharpAssembly,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $VisualBasicAssembly
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Invoke-Consumer {
    param(
        [Parameter(Mandatory)][string] $Assembly,
        [Parameter(Mandatory)][hashtable] $Environment
    )

    $names = @('DILOCK_BACKEND', 'DILOCK_REDIS_URL', 'DILOCK_ETCD_ENDPOINT', 'DILOCK_EPOCH')
    $old = @{}
    foreach ($name in $names) { $old[$name] = [Environment]::GetEnvironmentVariable($name) }
    try {
        foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $null) }
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value)
        }
        $output = @(& dotnet $Assembly 2>&1 | ForEach-Object { $_.ToString() }) -join "`n"
        $exitCode = $LASTEXITCODE
        return [pscustomobject]@{ ExitCode = $exitCode; Output = $output }
    }
    finally {
        foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $old[$name]) }
    }
}

function Assert-Failure {
    param([string] $Assembly, [hashtable] $Environment, [string] $Expected)
    $result = Invoke-Consumer $Assembly $Environment
    if ($result.ExitCode -eq 0 -or $result.Output -notmatch [regex]::Escape($Expected)) {
        throw "Expected '$Expected' failure from $Assembly (exit $($result.ExitCode)): $($result.Output)"
    }
}

function Assert-ApiOnly {
    param([string] $Assembly, [hashtable] $Environment)
    $result = Invoke-Consumer $Assembly $Environment
    if ($result.ExitCode -ne 0 -or $result.Output -notmatch 'live Redis/etcd NOT_RUN') {
        throw "Expected successful API-only path from $Assembly (exit $($result.ExitCode)): $($result.Output)"
    }
}

$both = @{ DILOCK_REDIS_URL = 'redis://127.0.0.1:1'; DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:1' }
$checks = 0
foreach ($assembly in @($FSharpAssembly, $VisualBasicAssembly)) {
    Assert-Failure $assembly $both 'Both Redis and etcd endpoints are configured'; $checks++
    Assert-Failure $assembly (@{ DILOCK_BACKEND = 'redis'; DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:1' }) 'requires DILOCK_REDIS_URL'; $checks++
    Assert-Failure $assembly (@{ DILOCK_BACKEND = 'etcd'; DILOCK_REDIS_URL = 'redis://127.0.0.1:1' }) 'requires DILOCK_ETCD_ENDPOINT'; $checks++
    Assert-Failure $assembly (@{ DILOCK_BACKEND = 'unknown'; DILOCK_REDIS_URL = 'redis://127.0.0.1:1' }) 'Unsupported DILOCK_BACKEND'; $checks++
    Assert-ApiOnly $assembly @{}; $checks++
    Assert-Failure $assembly ($both + @{ DILOCK_BACKEND = 'redis' }) 'requires DILOCK_EPOCH'; $checks++
    Assert-Failure $assembly ($both + @{ DILOCK_BACKEND = 'etcd' }) 'requires DILOCK_EPOCH'; $checks++
}

Write-Output "PASS .NET facade backend selection fail-closed tests: $checks cases (F# and VB.NET); no backend connections attempted."
