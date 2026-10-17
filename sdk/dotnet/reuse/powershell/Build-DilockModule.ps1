[CmdletBinding()]
param(
    [string] $OutputDirectory = (Join-Path (Get-Location) '.tmp/powershell-module'),
    [string] $NuGetCacheSource = $(if ($env:USERPROFILE) { Join-Path $env:USERPROFILE '.nuget/packages' } else { Join-Path $HOME '.nuget/packages' }),
    [string] $RuntimeAssemblyDirectory
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../../..')).Path
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
$temporaryPath = [System.IO.Path]::GetFullPath((Join-Path $outputPath 'build'))
$moduleSource = Join-Path $PSScriptRoot 'module/Dilock'
$bundleProject = Join-Path $PSScriptRoot 'module/Dilock.Runtime.Bundle.csproj'

if (Test-Path -LiteralPath $temporaryPath) {
    throw "Refusing to overwrite existing module build directory: $temporaryPath. Use a fresh -OutputDirectory."
}
New-Item -ItemType Directory -Path $temporaryPath -Force | Out-Null
$moduleStage = Join-Path $temporaryPath 'Dilock'
$libraryStage = Join-Path $moduleStage 'lib'
New-Item -ItemType Directory -Path $libraryStage -Force | Out-Null
Copy-Item -Path (Join-Path $moduleSource '*') -Destination $moduleStage -Recurse -Force
Copy-Item -LiteralPath (Join-Path $repositoryRoot 'LICENSE') -Destination $moduleStage

try {
    if ([string]::IsNullOrWhiteSpace($RuntimeAssemblyDirectory)) {
        if (-not (Test-Path -LiteralPath $NuGetCacheSource -PathType Container)) {
            throw "The specified local NuGet cache source does not exist: $NuGetCacheSource"
        }
        & dotnet restore $bundleProject --locked-mode --source $NuGetCacheSource --ignore-failed-sources --verbosity minimal
        if ($LASTEXITCODE -ne 0) { throw "Offline dotnet restore failed from local cache source '$NuGetCacheSource': $LASTEXITCODE" }

        $assemblySourceDirectory = Join-Path $temporaryPath 'publish'
        & dotnet publish $bundleProject --configuration Release --no-restore `
            --output $assemblySourceDirectory -p:UseAppHost=false --verbosity minimal
        if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed for the combined backend dependency bundle: $LASTEXITCODE" }
    }
    else {
        $assemblySourceDirectory = (Resolve-Path -LiteralPath $RuntimeAssemblyDirectory -ErrorAction Stop).Path
    }

    foreach ($file in Get-ChildItem -LiteralPath $assemblySourceDirectory -Filter '*.dll' -File) {
        Copy-Item -LiteralPath $file.FullName -Destination (Join-Path $libraryStage $file.Name)
    }

    foreach ($required in @('Dilock.Runtime.Bundle.dll', 'Dilock.Core.dll', 'Dilock.Redis.dll', 'Dilock.Etcd.dll', 'Dilock.Consul.dll')) {
        if (-not (Test-Path -LiteralPath (Join-Path $libraryStage $required) -PathType Leaf)) {
            throw "Published backend closure did not produce required assembly $required"
        }
    }

    $manifestErrors = @()
    Test-ModuleManifest -Path (Join-Path $moduleStage 'Dilock.psd1') -ErrorAction SilentlyContinue -ErrorVariable manifestErrors | Out-Null
    if ($manifestErrors.Count -gt 0) { throw "Module manifest validation failed: $($manifestErrors -join '; ')" }

    $zipPath = Join-Path $outputPath 'Dilock.PowerShell-0.1.0-local.zip'
    if (Test-Path -LiteralPath $zipPath) { throw "Refusing to overwrite existing module archive: $zipPath" }
    Compress-Archive -Path (Join-Path $moduleStage '*') -DestinationPath $zipPath -CompressionLevel Optimal
    Write-Output "Module directory: $moduleStage"
    Write-Output "Local module archive: $zipPath"
}
catch {
    # Keep the failed stage and logs for diagnosis; the caller owns this unique output directory.
    throw
}
