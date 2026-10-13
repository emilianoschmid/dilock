param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("redis", "etcd")]
    [string]$Backend,
    [string]$Endpoint,
    [string]$OutputDirectory
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
Push-Location $repoRoot
try {
    if (-not $Endpoint) {
        $Endpoint = if ($Backend -eq "redis") { "redis://127.0.0.1:16379" } else { "http://127.0.0.1:22379" }
    }
    if (-not $OutputDirectory) {
        $OutputDirectory = "validation/implementation/kotlin-m8-$Backend"
    }
    $outputPath = Join-Path $repoRoot $OutputDirectory
    New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
    $outputRelative = [IO.Path]::GetRelativePath($repoRoot, $outputPath)
    $partners = @("dotnet", "python", "cpp", "go", "rust", "java", "typescript")
    $venvPython = Join-Path $repoRoot ".tools/venv/Scripts/python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        $python = $venvPython
        $env:PATH = "$(Join-Path $repoRoot '.tools/venv/Scripts');$(Join-Path $repoRoot '.tools/go1.27.1/go/bin');$env:PATH"
        $goCache = Join-Path $repoRoot ".tools/go-build-cache-kotlin-m8"
        New-Item -ItemType Directory -Path $goCache -Force | Out-Null
        $env:GOCACHE = $goCache
    } else {
        $python = (Get-Command python -ErrorAction Stop).Source
    }
    if (Test-Path -LiteralPath (Join-Path $repoRoot ".tools/dilock-cpp-etcd-docker-build/dilock_cpp_worker")) {
        $env:DILOCK_CPP_DOCKER_WORKER = "1"
    }
    $failures = [Collections.Generic.List[string]]::new()

    foreach ($partner in $partners) {
        $report = "$outputRelative/kotlin-$Backend-$partner.json"
        Write-Host "Running Kotlin <-> $partner directed pairs against $Backend"
        & $python "conformance/run.py" `
            --workers "sdk/kotlin/worker/m8-workers.json" `
            --sdks kotlin $partner `
            --backend $Backend `
            --endpoint $Endpoint `
            --output $report
        if ($LASTEXITCODE -ne 0) {
            $failures.Add("$partner (exit $LASTEXITCODE)")
        }
    }

    if ($failures.Count -gt 0) {
        Write-Error "Kotlin $Backend pair failures: $($failures -join ', ')"
        exit 1
    }
    Write-Host "All 14 directed Kotlin/$Backend pair cells passed. Reports: $outputRelative"
}
finally {
    Pop-Location
}
