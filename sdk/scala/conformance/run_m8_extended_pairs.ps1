param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('redis', 'etcd')]
    [string]$Backend,
    [string]$ReportPath = "validation/implementation/m8-scala-elixir-extended-$((Get-Date).ToString('yyyyMMdd-HHmmss'))-$Backend.json",
    [ValidateSet('full', 'php-dart', 'scala-elixir')]
    [string]$PairSet = 'full'
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$workerNetwork = "dilock-m8-scala-elixir-workers-$suffix"
$serviceNetwork = "dilock-m8-scala-elixir-service-$suffix"
$serviceName = "dilock-m8-scala-elixir-$Backend-$suffix"
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
$python = Join-Path $root '.tools/venv/Scripts/python.exe'
$dartWorker = Join-Path $root 'validation/implementation/m8-worker-build-20261007/dilock-dart-worker'
$swiftWorker = Join-Path $root 'sdk/swift/.build/x86_64-unknown-linux-gnu/debug/dilock-swift-worker'
$swiftResources = Join-Path $root 'sdk/swift/.build/x86_64-unknown-linux-gnu/debug/Dilock_DilockRedis.resources'
$runDir = Join-Path $root "validation/implementation/run-m8-scala-elixir-extended-$suffix"
$reportFullPath = Join-Path $root $ReportPath
$manifest = 'sdk/scala/conformance/m8-cross-workers.json'
if ($PairSet -eq 'full') {
    $sdkNames = @('scala', 'elixir')
    $partners = @('php', 'ruby', 'dart', 'swift')
    $pairDescription = 'Scala and Elixir against PHP, Ruby, Dart and Swift in both directions'
} elseif ($PairSet -eq 'php-dart') {
    $sdkNames = @('php')
    $partners = @('dart')
    $pairDescription = 'PHP and Dart in both directions'
} else {
    $sdkNames = @('scala')
    $partners = @('elixir')
    $pairDescription = 'Scala and Elixir in both directions'
}
$plannedCells = $sdkNames.Count * $partners.Count * 2
$runs = [Collections.Generic.List[object]]::new()
$cells = [Collections.Generic.List[object]]::new()
$serviceStarted = $false
$report = [ordered]@{
    schema_version = 1
    date = (Get-Date).ToString('yyyy-MM-dd')
    scope = $pairDescription
    pair_set = $PairSet
    backend = $Backend
    status = 'in_progress'
    backend_endpoint = $endpoint
    worker_endpoint = $workerEndpoint
    backend_image = $image
    service_container = $serviceName
    worker_network = $workerNetwork
    service_network = $serviceNetwork
    manifest = $manifest
    run_directory = [IO.Path]::GetRelativePath($root, $runDir)
    counts = [ordered]@{ planned = $plannedCells; passed = 0; failed = 0; blocked = 0; not_run = $plannedCells }
    runs = $runs
    cells = $cells
    cleanup = [ordered]@{ status = 'pending' }
}

function Save-Report {
    $report.runs = @($runs.ToArray())
    $report.cells = @($cells.ToArray())
    $report.counts.passed = @($cells | Where-Object status -eq 'passed').Count
    $report.counts.failed = @($cells | Where-Object status -eq 'failed').Count
    $report.counts.blocked = @($cells | Where-Object status -eq 'blocked').Count
    $report.counts.not_run = @($cells | Where-Object status -eq 'not_run').Count
    $report | ConvertTo-Json -Depth 14 | Set-Content -LiteralPath $reportFullPath -Encoding utf8
}

function Test-ServiceReady {
    for ($i = 0; $i -lt 40; $i++) {
        if ($Backend -eq 'redis') {
            docker exec $serviceName redis-cli ping 2>$null | Out-Null
        } else {
            docker exec $serviceName /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health 2>$null | Out-Null
        }
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Milliseconds 250
    }
    return $false
}

try {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Repository Python environment is missing: $python" }
    if (-not (Test-Path -LiteralPath $dartWorker -PathType Leaf)) { throw "Dart AOT worker missing: $dartWorker" }
    if (-not (Test-Path -LiteralPath $swiftWorker -PathType Leaf)) { throw "Swift worker missing: $swiftWorker" }
    if (-not (Test-Path -LiteralPath $swiftResources -PathType Container)) { throw "Swift Redis resource bundle missing: $swiftResources" }
    if (Test-Path -LiteralPath $reportFullPath) { throw "Refusing to overwrite existing evidence: $ReportPath" }
    if (Test-Path -LiteralPath $runDir) { throw "Refusing to reuse existing run directory: $runDir" }
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $reportFullPath) -Force | Out-Null
    foreach ($sdk in $sdkNames) {
        foreach ($partner in $partners) {
            foreach ($direction in @(@($sdk, $partner), @($partner, $sdk))) {
                $cells.Add([ordered]@{ backend = $Backend; holder = $direction[0]; contender = $direction[1]; status = 'not_run'; scenarios = @(); report = $null; detail = 'Pair has not started.' })
            }
        }
    }
    Save-Report

    docker network create --internal $workerNetwork | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create isolated worker network.' }
    docker network create $serviceNetwork | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create backend bridge network.' }
    if ($Backend -eq 'redis') {
        docker run -d --rm --pull=never --network $serviceNetwork --publish "127.0.0.1:${hostPort}:6379" --name $serviceName $image redis-server --save '' --appendonly no --port 6379 | Out-Null
    } else {
        docker run -d --rm --pull=never --network $serviceNetwork --publish "127.0.0.1:${hostPort}:2379" --name $serviceName $image /usr/local/bin/etcd `
            '--name=etcd' '--data-dir=/tmp/dilock-etcd.data' '--listen-client-urls=http://0.0.0.0:2379' `
            '--advertise-client-urls=http://etcd:2379' '--listen-peer-urls=http://0.0.0.0:2380' `
            '--initial-advertise-peer-urls=http://etcd:2380' '--initial-cluster=etcd=http://etcd:2380' `
            "--initial-cluster-token=dilock-m8-scala-elixir-$suffix" '--initial-cluster-state=new' | Out-Null
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not start disposable $Backend service." }
    $serviceStarted = $true
    docker network connect --alias $serviceAlias $workerNetwork $serviceName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not connect backend to the isolated worker network.' }
    if (-not (Test-ServiceReady)) { throw "Disposable $Backend service failed bounded readiness check." }

    $env:DILOCK_M8_DOCKER_NETWORK = $workerNetwork
    $env:DILOCK_M8_WORKER_ENDPOINT = $workerEndpoint
    $env:DILOCK_M8_DART_WORKER = $dartWorker
    $env:DILOCK_SCALA_WORKER_NETWORK = $workerNetwork
    $env:DILOCK_SCALA_REDIS_WORKER_ENDPOINT = if ($Backend -eq 'redis') { $workerEndpoint } else { $null }
    $env:DILOCK_SCALA_ETCD_WORKER_ENDPOINT = if ($Backend -eq 'etcd') { $workerEndpoint } else { $null }
    foreach ($sdk in $sdkNames) {
        foreach ($partner in $partners) {
            $name = "$sdk-$Backend-$partner"
            $outputRelative = [IO.Path]::GetRelativePath($root, (Join-Path $runDir "$name.json"))
            $logPath = Join-Path $runDir "$name.log"
            $command = "$python conformance/run.py --backend $Backend --workers $manifest --sdks $sdk $partner --endpoint $endpoint --output $outputRelative"
            Write-Host "Running $name ($workerEndpoint)"
            & $python 'conformance/run.py' --backend $Backend --workers $manifest --sdks $sdk $partner --endpoint $endpoint --output $outputRelative 2>&1 | Tee-Object -FilePath $logPath
            $exitCode = $LASTEXITCODE
            $pairCells = @()
            $pairReportPath = Join-Path $root $outputRelative
            if (Test-Path -LiteralPath $pairReportPath) {
                try {
                    $pairReport = Get-Content -LiteralPath $pairReportPath -Raw | ConvertFrom-Json
                    $pairCells = @($pairReport.results | ForEach-Object { [ordered]@{ holder = $_.holder; contender = $_.contender; status = $_.status; seconds = $_.seconds; scenarios = @($_.scenarios) } })
                } catch { $pairCells = @([ordered]@{ status = 'failed'; detail = "Pair report parse failed: $($_.Exception.Message)" }) }
            }
            if ($pairCells.Count -eq 0) {
                $pairStatus = if ($exitCode -eq 124) { 'blocked' } else { 'blocked' }
                $detail = if (Test-Path -LiteralPath $logPath) { (Get-Content -LiteralPath $logPath -Raw).Trim() } else { 'Runner produced neither pair results nor a diagnostic log.' }
                foreach ($cell in $cells | Where-Object { $_.holder -in @($sdk, $partner) -and $_.contender -in @($sdk, $partner) -and $_.status -eq 'not_run' -and (($sdk -eq $_.holder -and $partner -eq $_.contender) -or ($partner -eq $_.holder -and $sdk -eq $_.contender)) }) {
                    $cell.status = $pairStatus
                    $cell.detail = $detail
                    $cell.report = [IO.Path]::GetRelativePath($root, $logPath)
                }
            } else {
                foreach ($pairCell in $pairCells) {
                    $cell = $cells | Where-Object { $_.holder -eq $pairCell.holder -and $_.contender -eq $pairCell.contender } | Select-Object -First 1
                    if ($cell) {
                        $cell.status = $pairCell.status
                        $cell.scenarios = @($pairCell.scenarios)
                        $cell.report = $outputRelative
                        $cell.detail = $null
                    }
                }
                $pairStatus = if ($exitCode -eq 0 -and @($pairCells | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } else { 'failed' }
            }
            $runs.Add([ordered]@{ sdk = $sdk; partner = $partner; status = $pairStatus; command = $command; exit_code = $exitCode; report = if (Test-Path -LiteralPath $pairReportPath) { $outputRelative } else { $null }; log = [IO.Path]::GetRelativePath($root, $logPath); cells = @($pairCells) })
            Save-Report
            if ($pairStatus -ne 'passed' -and -not (Test-Path -LiteralPath $pairReportPath)) { break }
        }
    }
    $report.status = if (@($cells | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } elseif (@($cells | Where-Object status -eq 'passed').Count -gt 0) { 'partial' } else { 'blocked' }
}
catch {
    $report.status = 'blocked'
    $report.blocker = $_.Exception.Message
    foreach ($cell in $cells | Where-Object status -eq 'not_run') { $cell.status = 'not_run'; $cell.detail = "Setup stopped before pair execution: $($report.blocker)" }
    Save-Report
}
finally {
    foreach ($variable in @('DILOCK_M8_DOCKER_NETWORK', 'DILOCK_M8_WORKER_ENDPOINT', 'DILOCK_M8_DART_WORKER', 'DILOCK_SCALA_WORKER_NETWORK', 'DILOCK_SCALA_REDIS_WORKER_ENDPOINT', 'DILOCK_SCALA_ETCD_WORKER_ENDPOINT')) {
        Remove-Item "Env:\$variable" -ErrorAction SilentlyContinue
    }
    docker stop $serviceName 2>$null | Out-Null
    docker network rm $workerNetwork 2>$null | Out-Null
    docker network rm $serviceNetwork 2>$null | Out-Null
    $inventoryOk = $true
    $remainingContainers = @(docker ps -a --filter "name=$serviceName" --format '{{.Names}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingWorkerNetworks = @(docker network ls --filter "name=$workerNetwork" --format '{{.Name}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingServiceNetworks = @(docker network ls --filter "name=$serviceNetwork" --format '{{.Name}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $report.cleanup = [ordered]@{
        status = if (-not $inventoryOk) { 'blocked' } elseif ($remainingContainers.Count + $remainingWorkerNetworks.Count + $remainingServiceNetworks.Count -eq 0) { 'passed' } else { 'failed' }
        inventory_status = if ($inventoryOk) { 'completed' } else { 'blocked' }
        remaining_containers = $remainingContainers
        remaining_networks = @($remainingWorkerNetworks + $remainingServiceNetworks)
        service_was_started = $serviceStarted
    }
    if ($report.status -eq 'in_progress') { $report.status = 'not_run' }
    Save-Report
}

if ($report.status -ne 'passed' -or $report.cleanup.status -ne 'passed') { exit 1 }
Write-Host "All $plannedCells directed $Backend cells passed. Evidence: $ReportPath"
