param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('redis', 'etcd')]
    [string]$Backend,
    [string]$ReportPath = "validation/implementation/kotlin-m8-extended-directed-pairs-$((Get-Date).ToString('yyyyMMdd-HHmmss')).json"
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$workerNetwork = "dilock-kotlin-m8-workers-$suffix"
$serviceNetwork = "dilock-kotlin-m8-service-$suffix"
$serviceName = "dilock-kotlin-m8-$Backend-$suffix"
$serviceAlias = if ($Backend -eq 'redis') { 'redis' } else { 'etcd' }
$redisImage = 'redis@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935'
$etcdImage = 'quay.io/coreos/etcd@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd'
$image = if ($Backend -eq 'redis') { $redisImage } else { $etcdImage }
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$hostPort = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
$listener.Stop()
$endpoint = if ($Backend -eq 'redis') { "redis://127.0.0.1:$hostPort" } else { "http://127.0.0.1:$hostPort" }
$workerEndpoint = if ($Backend -eq 'redis') { 'redis://redis:6379' } else { 'http://etcd:2379' }
$partners = @('php', 'ruby', 'dart', 'swift')
$python = Join-Path $root '.tools/venv/Scripts/python.exe'
$runDir = Join-Path $root "validation/implementation/run-m8-kotlin-extended-$suffix"
$reportFullPath = Join-Path $root $ReportPath
$runs = [Collections.Generic.List[object]]::new()
$startedResources = $false
$report = [ordered]@{
    schema_version = 1
    date = (Get-Date).ToString('yyyy-MM-dd')
    sdk = 'kotlin'
    backend = $Backend
    scope = 'Kotlin against PHP, Ruby, Dart and Swift in both directions'
    status = 'in_progress'
    endpoint = $endpoint
    worker_endpoint = $workerEndpoint
    image = $image
    service_container = $serviceName
    worker_network = $workerNetwork
    service_network = $serviceNetwork
    run_directory = [IO.Path]::GetRelativePath($root, $runDir)
    runs = $runs
    cleanup = [ordered]@{ status = 'pending' }
}

function Save-Report {
    $report.runs = @($runs.ToArray())
    $report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $reportFullPath -Encoding utf8
}

function Test-ServiceReady {
    param([int]$Attempts = 40)
    for ($i = 0; $i -lt $Attempts; $i++) {
        if ($Backend -eq 'redis') {
            docker exec $serviceName redis-cli ping 2>$null | Out-Null
        } else {
            docker exec $serviceName etcdctl --endpoints=http://127.0.0.1:2379 endpoint health 2>$null | Out-Null
        }
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Milliseconds 250
    }
    return $false
}

try {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Repository Python environment is missing: $python" }
    if (Test-Path -LiteralPath $reportFullPath) { throw "Refusing to overwrite existing evidence: $ReportPath" }
    if ($partners -contains 'dart') {
        $dartWorker = Join-Path $root 'validation/implementation/m8-worker-build-20261007/dilock-dart-worker'
        if (-not (Test-Path -LiteralPath $dartWorker -PathType Leaf)) { throw "Dart AOT worker missing: $dartWorker" }
    }
    $swiftWorker = Join-Path $root 'sdk/swift/.build/x86_64-unknown-linux-gnu/debug/dilock-swift-worker'
    if (-not (Test-Path -LiteralPath $swiftWorker -PathType Leaf)) { throw "Swift worker missing: $swiftWorker" }

    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $reportFullPath) -Force | Out-Null
    docker network create --internal $workerNetwork | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create isolated worker network' }
    docker network create $serviceNetwork | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create backend bridge network' }

    if ($Backend -eq 'redis') {
        docker run -d --rm --pull=never --network $serviceNetwork --publish "127.0.0.1:${hostPort}:6379" --name $serviceName $image redis-server --save '' --appendonly no --port 6379 | Out-Null
    } else {
        docker run -d --rm --pull=never --network $serviceNetwork --publish "127.0.0.1:${hostPort}:2379" --name $serviceName $image /usr/local/bin/etcd "--name=$serviceAlias" '--data-dir=/tmp/dilock-etcd.data' '--listen-client-urls=http://0.0.0.0:2379' "--advertise-client-urls=http://${serviceAlias}:2379" '--listen-peer-urls=http://0.0.0.0:2380' "--initial-advertise-peer-urls=http://${serviceAlias}:2380" "--initial-cluster=${serviceAlias}=http://${serviceAlias}:2380" "--initial-cluster-token=dilock-m8-$suffix" '--initial-cluster-state=new' | Out-Null
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not start disposable $Backend container" }
    $startedResources = $true
    docker network connect --alias $serviceAlias $workerNetwork $serviceName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not connect backend to isolated worker network' }
    if (-not (Test-ServiceReady)) { throw "Disposable $Backend did not become ready within 10 seconds" }
    $env:DILOCK_M8_DOCKER_NETWORK = $workerNetwork
    $env:DILOCK_M8_WORKER_ENDPOINT = $workerEndpoint
    $env:DILOCK_M8_DART_WORKER = Join-Path $root 'validation/implementation/m8-worker-build-20261007/dilock-dart-worker'
    $env:DILOCK_WORKER_DIAGNOSTICS = '1'
    foreach ($partner in $partners) {
        $name = "kotlin-$Backend-$partner"
        $outputRelative = [IO.Path]::GetRelativePath($root, (Join-Path $runDir "$name.json"))
        $logPath = Join-Path $runDir "$name.log"
        $command = "$python conformance/run.py --backend $Backend --workers sdk/kotlin/worker/m8-extended-workers.json --sdks kotlin $partner --endpoint $endpoint --output $outputRelative"
        Write-Host "Running $name ($workerEndpoint)"
        & $python 'conformance/run.py' --backend $Backend --workers 'sdk/kotlin/worker/m8-extended-workers.json' --sdks kotlin $partner --endpoint $endpoint --output $outputRelative 2>&1 | Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
        $cellStatuses = @()
        if (Test-Path -LiteralPath (Join-Path $root $outputRelative)) {
            try {
                $pairReport = Get-Content -LiteralPath (Join-Path $root $outputRelative) -Raw | ConvertFrom-Json
                $cellStatuses = @($pairReport.results | ForEach-Object { [ordered]@{ holder = $_.holder; contender = $_.contender; status = $_.status; seconds = $_.seconds; scenarios = $_.scenarios } })
            } catch { $cellStatuses = @([ordered]@{ status = 'report_parse_failed'; detail = $_.Exception.Message }) }
        } else {
            $cellStatuses = @([ordered]@{ status = 'not_run'; detail = 'Pair runner produced no report.' })
        }
        $status = if ($exitCode -eq 0 -and $cellStatuses.Count -eq 2 -and @($cellStatuses | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } elseif ($exitCode -eq 124) { 'blocked' } else { 'failed' }
        $runs.Add([ordered]@{ partner = $partner; status = $status; command = $command; exit_code = $exitCode; report = $outputRelative; log = [IO.Path]::GetRelativePath($root, $logPath); cells = $cellStatuses })
        Save-Report
    }
    $report.status = if (@($runs | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } else { 'partial' }
}
catch {
    $report.status = 'blocked'
    $report.blocker = $_.Exception.Message
    Save-Report
    throw
}
finally {
    Remove-Item Env:\DILOCK_M8_DOCKER_NETWORK -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_M8_WORKER_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_M8_DART_WORKER -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_WORKER_DIAGNOSTICS -ErrorAction SilentlyContinue
    docker stop $serviceName 2>$null | Out-Null
    docker network rm $workerNetwork 2>$null | Out-Null
    docker network rm $serviceNetwork 2>$null | Out-Null
    $inventoryOk = $true
    $remainingContainers = @(docker ps -a --filter "name=$serviceName" --format '{{.Names}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingWorkerNetwork = @(docker network ls --filter "name=$workerNetwork" --format '{{.Name}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingServiceNetwork = @(docker network ls --filter "name=$serviceNetwork" --format '{{.Name}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $report.cleanup = [ordered]@{
        status = if (-not $inventoryOk) { 'blocked' } elseif ($remainingContainers.Count -eq 0 -and $remainingWorkerNetwork.Count -eq 0 -and $remainingServiceNetwork.Count -eq 0) { 'passed' } else { 'failed' }
        inventory_status = if ($inventoryOk) { 'completed' } else { 'blocked' }
        remaining_containers = $remainingContainers
        remaining_networks = @($remainingWorkerNetwork + $remainingServiceNetwork)
        disposable_service_was_started = $startedResources
    }
    if ($report.status -eq 'in_progress') { $report.status = 'not_run' }
    Save-Report
}

if ($report.status -ne 'passed' -or $report.cleanup.status -ne 'passed') { exit 1 }
Write-Host "All eight directed Kotlin/$Backend pairs passed. Evidence: $ReportPath"
