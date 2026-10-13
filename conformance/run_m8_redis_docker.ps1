param([string[]]$Sdks = @('dart', 'swift'))
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$network = "dilock-m8-internal-$suffix"
$redisNetwork = "dilock-m8-redis-net-$suffix"
$redis = "dilock-m8-redis-$suffix"
$redisAlias = 'dilock-m8-redis'
$redisImage = 'redis@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935'
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$hostPort = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
$listener.Stop()
$exitCode = 1

try {
    docker network create --internal $network | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker network create failed: $LASTEXITCODE" }
    docker network create $redisNetwork | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker network create Redis bridge failed: $LASTEXITCODE" }

    docker run -d --rm --pull=never --network $redisNetwork --publish "127.0.0.1:${hostPort}:6379" --name $redis $redisImage redis-server --save '' --appendonly no --port 6379 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker run Redis failed: $LASTEXITCODE" }
    docker network connect --alias $redisAlias $network $redis
    if ($LASTEXITCODE -ne 0) { throw "docker network connect Redis to worker network failed: $LASTEXITCODE" }
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        docker exec $redis redis-cli ping 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw 'task-owned Redis did not become ready within 7.5 seconds' }

    $env:DILOCK_M8_DOCKER_NETWORK = $network
    $env:DILOCK_M8_WORKER_ENDPOINT = "redis://${redisAlias}:6379"
    if ($Sdks.Count -lt 1) { throw 'Specify at least one M8 worker SDK.' }
    if ($Sdks -contains 'dart') {
        $env:DILOCK_M8_DART_WORKER = Join-Path $root 'validation/implementation/m8-worker-build-20261007/dilock-dart-worker'
        if (-not (Test-Path -LiteralPath $env:DILOCK_M8_DART_WORKER -PathType Leaf)) {
            throw "Dart AOT executable missing; build the current source before running this smoke: $env:DILOCK_M8_DART_WORKER"
        }
    }
    if ($Sdks -contains 'swift') {
        $swiftWorker = Join-Path $root 'sdk/swift/.build/x86_64-unknown-linux-gnu/debug/dilock-swift-worker'
        if (-not (Test-Path -LiteralPath $swiftWorker -PathType Leaf)) {
            throw "Swift worker binary missing; compile the current source in the pinned Docker image first: $swiftWorker"
        }
        $env:DILOCK_WORKER_DIAGNOSTICS = '1'
    }
    $python = Join-Path $root '.tools/venv/Scripts/python.exe'
    Push-Location $root
    try {
        & $python conformance/run.py --backend redis --workers conformance/workers-m8.json --sdks $Sdks --endpoint "redis://127.0.0.1:${hostPort}" --output validation/implementation/m8-dart-swift-redis-pairs-rerun-20261007.json
        $exitCode = $LASTEXITCODE
    }
    finally { Pop-Location }
}
finally {
    Remove-Item Env:\DILOCK_M8_DOCKER_NETWORK -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_M8_WORKER_ENDPOINT -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_M8_DART_WORKER -ErrorAction SilentlyContinue
    Remove-Item Env:\DILOCK_WORKER_DIAGNOSTICS -ErrorAction SilentlyContinue
    docker stop $redis 2>$null | Out-Null
    docker network rm $network 2>$null | Out-Null
    docker network rm $redisNetwork 2>$null | Out-Null
}

exit $exitCode
