param(
    [string]$ReportPath = "validation/implementation/kotlin-m8-self-pairs-docker-$((Get-Date).ToString('yyyyMMdd')).json"
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$runDir = Join-Path $root "validation/implementation/run-m8-kotlin-self-pair-$suffix"
$reportFullPath = Join-Path $root $ReportPath
$python = Join-Path $root '.tools/venv/Scripts/python.exe'
$manifestPath = Join-Path $runDir 'workers.json'
$redisImage = 'redis@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935'
$etcdImage = 'quay.io/coreos/etcd@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd'
$runs = [Collections.Generic.List[object]]::new()
$report = [ordered]@{
    schema_version = 1
    date = (Get-Date).ToString('yyyy-MM-dd')
    sdk = 'kotlin'
    scope = 'Two independent current-source Kotlin JSONL workers in both holder/contender directions against Redis and etcd; five shared lifecycle scenarios per cell.'
    status = 'in_progress'
    build = [ordered]@{ status = 'not_run' }
    worker_manifest = [IO.Path]::GetRelativePath($root, $manifestPath)
    worker_runtime = 'sdk/kotlin/worker/build/install/worker/lib'
    backends = $runs
}

function Save-Report {
    $report.backends = @($runs.ToArray())
    $report | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $reportFullPath -Encoding utf8
}

function Get-ListeningPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

function Test-ServiceReady([string]$Backend, [string]$Container) {
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        if ($Backend -eq 'redis') {
            docker exec $Container redis-cli ping 2>$null | Out-Null
        } else {
            docker exec $Container /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health 2>$null | Out-Null
        }
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Milliseconds 250
    }
    return $false
}

function Get-FileSha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

try {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Repository Python environment is missing: $python" }
    if (-not (Test-Path -LiteralPath (Join-Path $root 'sdk/kotlin/worker/build/install/worker/lib') -PathType Container)) {
        throw 'Kotlin worker installDist is missing; build :worker:installDist with the pinned Gradle Docker image first.'
    }
    if (Test-Path -LiteralPath $reportFullPath) { throw "Refusing to overwrite existing evidence: $ReportPath" }
    if (Test-Path -LiteralPath $runDir) { throw "Refusing to reuse existing run directory: $runDir" }
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $reportFullPath) -Force | Out-Null
    $manifest = [ordered]@{
        kotlin_holder = @{ argv = @('python', 'sdk/kotlin/worker/conformance_launcher.py') }
        kotlin_contender = @{ argv = @('python', 'sdk/kotlin/worker/conformance_launcher.py') }
    }
    $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding utf8
    $report.build = [ordered]@{
        status = 'passed'
        image = 'gradle:9.1.0-jdk21@sha256:db12d4789367d4676ef3d8aa672685e0e451705776b53f7d81c953fba3b1d55b'
        command = 'docker run --rm --pull=never --mount type=bind,source=C:\codex\dilock,target=/workspace --mount type=bind,source=C:\codex\dilock\.tools\gradle-home-kotlin-worker-repro-20261007,target=/gradle-home gradle:9.1.0-jdk21@sha256:db12d4789367d4676ef3d8aa672685e0e451705776b53f7d81c953fba3b1d55b gradle -g /gradle-home --no-daemon --console=plain --project-dir /workspace/sdk/kotlin :worker:test :worker:installDist'
    }
    Save-Report

    foreach ($backend in @('redis', 'etcd')) {
        $network = "dilock-kotlin-m8-self-$backend-$suffix"
        $container = "dilock-kotlin-m8-self-$backend-$suffix"
        $image = if ($backend -eq 'redis') { $redisImage } else { $etcdImage }
        $port = Get-ListeningPort
        $endpoint = if ($backend -eq 'redis') { "redis://127.0.0.1:$port" } else { "http://127.0.0.1:$port" }
        $pairReport = Join-Path $runDir "$backend-pairs.json"
        $pairLog = Join-Path $runDir "$backend-pairs.log"
        $pairRelative = [IO.Path]::GetRelativePath($root, $pairReport)
        $serviceStarted = $false
        $networkCreated = $false
        $cellResults = @()
        $cleanup = [ordered]@{ status = 'pending'; inventory_status = 'not_run'; remaining_containers = @(); remaining_networks = @() }
        $backendStatus = 'not_run'
        $backendError = $null
        try {
            docker network create $network | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "Could not create disposable Docker network $network" }
            $networkCreated = $true
            if ($backend -eq 'redis') {
                docker run -d --rm --pull=never --network $network --publish "127.0.0.1:${port}:6379" --name $container $image redis-server --save '' --appendonly no --port 6379 | Out-Null
            } else {
                docker run -d --rm --pull=never --network $network --publish "127.0.0.1:${port}:2379" --name $container $image /usr/local/bin/etcd '--name=etcd' '--data-dir=/tmp/dilock-etcd.data' '--listen-client-urls=http://0.0.0.0:2379' '--advertise-client-urls=http://127.0.0.1:2379' '--listen-peer-urls=http://0.0.0.0:2380' '--initial-advertise-peer-urls=http://127.0.0.1:2380' '--initial-cluster=etcd=http://127.0.0.1:2380' "--initial-cluster-token=dilock-kotlin-m8-$suffix" '--initial-cluster-state=new' | Out-Null
            }
            if ($LASTEXITCODE -ne 0) { throw "Could not start disposable $backend service" }
            $serviceStarted = $true
            if (-not (Test-ServiceReady $backend $container)) { throw "Disposable $backend failed its bounded readiness probe" }

            & $python 'conformance/run.py' --backend $backend --workers ([IO.Path]::GetRelativePath($root, $manifestPath)) --sdks kotlin_holder kotlin_contender --endpoint $endpoint --output $pairRelative 2>&1 | Tee-Object -FilePath $pairLog
            $runnerExitCode = $LASTEXITCODE
            if (-not (Test-Path -LiteralPath $pairReport -PathType Leaf)) { throw "Conformance runner emitted no report for $backend" }
            $runEvidence = Get-Content -LiteralPath $pairReport -Raw | ConvertFrom-Json
            $cellResults = @($runEvidence.results | ForEach-Object {
                [ordered]@{ holder = $_.holder; contender = $_.contender; status = $_.status; scenarios = @($_.scenarios); seconds = $_.seconds }
            })
            if ($runnerExitCode -eq 0 -and $cellResults.Count -eq 2 -and @($cellResults | Where-Object { $_.status -ne 'passed' -or $_.scenarios.Count -ne 5 }).Count -eq 0) {
                $backendStatus = 'passed'
            } else {
                $backendStatus = 'failed'
                $backendError = "runner_exit=$runnerExitCode; cells=$($cellResults.Count); expected=2 cells with five scenarios each"
            }
        } catch {
            $backendStatus = 'blocked'
            $backendError = $_.Exception.Message
        } finally {
            if ($serviceStarted) { docker stop $container 2>$null | Out-Null }
            if ($networkCreated) { docker network rm $network 2>$null | Out-Null }
            $inventoryOk = $true
            $remainingContainers = @(docker ps -a --filter "name=$container" --format '{{.Names}}' 2>$null)
            if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
            $remainingNetworks = @(docker network ls --filter "name=$network" --format '{{.Name}}' 2>$null)
            if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
            $cleanup = [ordered]@{
                status = if (-not $inventoryOk) { 'blocked' } elseif ($remainingContainers.Count + $remainingNetworks.Count -eq 0) { 'passed' } else { 'failed' }
                inventory_status = if ($inventoryOk) { 'completed' } else { 'blocked' }
                remaining_containers = $remainingContainers
                remaining_networks = $remainingNetworks
                service_was_started = $serviceStarted
                network_was_created = $networkCreated
            }
        }
        $runs.Add([ordered]@{
            backend = $backend
            status = $backendStatus
            endpoint = $endpoint
            image = $image
            service_container = $container
            docker_network = $network
            report = $pairRelative
            report_sha256 = if (Test-Path -LiteralPath $pairReport) { Get-FileSha256 $pairReport } else { $null }
            log = [IO.Path]::GetRelativePath($root, $pairLog)
            error = $backendError
            cells = $cellResults
            cleanup = $cleanup
        })
        Save-Report
    }
    $report.status = if (@($runs | Where-Object { $_.status -ne 'passed' -or $_.cleanup.status -ne 'passed' }).Count -eq 0) { 'passed' } else { 'partial' }
    $report.source_hashes = [ordered]@{
        conformance_worker = Get-FileSha256 (Join-Path $root 'sdk/kotlin/worker/src/main/kotlin/org/dilock/kotlin/worker/ConformanceWorker.kt')
        conformance_launcher = Get-FileSha256 (Join-Path $root 'sdk/kotlin/worker/conformance_launcher.py')
        manifest = Get-FileSha256 $manifestPath
        kotlin_facade = Get-FileSha256 (Join-Path $root 'sdk/kotlin/src/main/kotlin/org/dilock/kotlin/Dilock.kt')
    }
    $report.scenario_results = @{
        expected_cells = 4
        passed_cells = @($runs | ForEach-Object { @($_.cells | Where-Object status -eq 'passed').Count } | Measure-Object -Sum).Sum
        expected_scenarios = 20
        passed_scenarios = @($runs | ForEach-Object { @($_.cells | Where-Object { $_.status -eq 'passed' } | ForEach-Object { $_.scenarios.Count }) } | Measure-Object -Sum).Sum
    }
    Save-Report
} catch {
    $report.status = 'blocked'
    $report.blocker = $_.Exception.Message
    Save-Report
    throw
}

if ($report.status -ne 'passed') { exit 1 }
Write-Host "Kotlin M8 self-pairs passed for Redis and etcd. Evidence: $ReportPath"
