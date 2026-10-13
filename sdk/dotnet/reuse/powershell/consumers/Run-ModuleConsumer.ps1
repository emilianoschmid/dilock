[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('Redis', 'Etcd')][string] $Backend,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Endpoint,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $ModuleArchive
)

$ErrorActionPreference = 'Stop'
$archivePath = (Resolve-Path -LiteralPath $ModuleArchive).Path
$consumerRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("dilock-ps-module-consumer-" + [Guid]::NewGuid().ToString('N'))
$moduleDirectory = Join-Path $consumerRoot 'Dilock'
$lifecycleScript = Join-Path $PSScriptRoot 'Invoke-ModuleLifecycle.ps1'
New-Item -ItemType Directory -Path $moduleDirectory -Force | Out-Null

try {
    Expand-Archive -LiteralPath $archivePath -DestinationPath $moduleDirectory
    if (-not (Test-Path (Join-Path $moduleDirectory 'Dilock.psd1') -PathType Leaf)) {
        throw 'Local archive has an invalid module root.'
    }

    Push-Location $consumerRoot
    try {
        & pwsh -NoProfile -File $lifecycleScript -Backend $Backend -Endpoint $Endpoint `
            -Epoch $Epoch -ModuleDirectory $moduleDirectory
        $consumerExitCode = $LASTEXITCODE
    }
    finally { Pop-Location }

    if ($consumerExitCode -ne 0) { throw "Source-hidden $Backend module consumer exited $consumerExitCode." }
}
finally {
    # The child PowerShell process must exit before Windows releases its loaded DLL handles.
    Remove-Item -LiteralPath $consumerRoot -Recurse -Force
}

Write-Output "PASS ${Backend}: extracted ZIP consumer ran from $consumerRoot"
Write-Output 'PASS consumer module directory removed after the PowerShell process exited'
