[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $AssemblyDirectory
)

$ErrorActionPreference = 'Stop'
$example = Join-Path $PSScriptRoot '..' 'Invoke-DilockExample.ps1'
$example = (Resolve-Path $example).Path

function Assert-ThrowsContaining {
    param(
        [Parameter(Mandatory)][string] $Expected,
        [Parameter(Mandatory)][hashtable] $Arguments
    )
    try {
        & $example @Arguments | Out-Null
    }
    catch {
        if ($_.Exception.Message.Contains($Expected, [StringComparison]::OrdinalIgnoreCase)) {
            return
        }
        throw "Expected error containing '$Expected', got: $($_.Exception.Message)"
    }
    throw "Expected invocation to fail with '$Expected'."
}

function Assert-NotRun {
    param([Parameter(Mandatory)][hashtable] $Arguments)
    $output = @(& $example @Arguments)
    if (-not ($output -join "`n" | Select-String -SimpleMatch 'live backend NOT_RUN')) {
        throw "Expected API-only path to report live backend NOT_RUN; got: $($output -join ' | ')"
    }
}

$base = @{
    SkipBuild = $true
    AssemblyDirectory = $AssemblyDirectory
    Epoch = 'selection-test-epoch'
}

Assert-ThrowsContaining -Expected 'Multiple coordinator endpoints are configured' -Arguments ($base + @{
    Backend = 'Auto'
    Endpoint = 'redis://127.0.0.1:1'
    EtcdEndpoint = '127.0.0.1:1'
})

Assert-ThrowsContaining -Expected 'Backend Redis requires' -Arguments ($base + @{
    Backend = 'Redis'
    Endpoint = ''
    EtcdEndpoint = '127.0.0.1:1'
})

Assert-ThrowsContaining -Expected 'Backend Etcd requires' -Arguments ($base + @{
    Backend = 'Etcd'
    Endpoint = 'redis://127.0.0.1:1'
    EtcdEndpoint = ''
})

Assert-ThrowsContaining -Expected 'Backend Postgres requires' -Arguments ($base + @{
    Backend = 'Postgres'
    PostgresConnectionString = ''
})

$missingEpoch = $base.Clone()
$missingEpoch.Epoch = ''
Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($missingEpoch + @{
    Backend = 'Redis'
    Endpoint = 'redis://127.0.0.1:1'
})
Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($missingEpoch + @{
    Backend = 'Etcd'
    EtcdEndpoint = 'http://127.0.0.1:1'
})
Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($missingEpoch + @{
    Backend = 'Postgres'
    PostgresConnectionString = 'Host=127.0.0.1;Port=1;Database=dilock'
})

# Explicit choice is resolved before connection, but a configured backend still
# requires an epoch. These cases stay offline and prove the fail-closed guard.
$explicitBase = $base.Clone()
$explicitBase.Epoch = ''
Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($explicitBase + @{
    Backend = 'Etcd'
    Endpoint = 'redis://127.0.0.1:1'
    EtcdEndpoint = '127.0.0.1:1'
})

Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($explicitBase + @{
    Backend = 'Auto'
    Endpoint = 'redis://127.0.0.1:1'
    EtcdEndpoint = ''
})
Assert-ThrowsContaining -Expected 'requires -Epoch or DILOCK_EPOCH' -Arguments ($explicitBase + @{
    Backend = 'Auto'
    Endpoint = ''
    EtcdEndpoint = '127.0.0.1:1'
})

Assert-ThrowsContaining -Expected 'Multiple coordinator endpoints are configured' -Arguments ($base + @{
    Backend = 'Auto'
    Endpoint = 'redis://127.0.0.1:1'
    PostgresConnectionString = 'Host=127.0.0.1;Port=1;Database=dilock'
})

Write-Output 'PASS backend selection fail-closed tests: 11 cases.'
