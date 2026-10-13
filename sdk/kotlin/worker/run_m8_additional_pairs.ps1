param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('redis', 'etcd')]
    [string]$Backend,
    [string]$ReportPath = "validation/implementation/kotlin-m8-additional-$((Get-Date).ToString('yyyyMMdd-HHmmss'))-$Backend.json",
    [ValidateSet('scala', 'elixir', 'c')]
    [string[]]$Partners = @('scala', 'elixir', 'c'),
    [string]$CBuilderImageOverride = '',
    [switch]$SkipCBuild
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$network = "dilock-kotlin-m8-additional-$suffix"
$serviceName = "dilock-kotlin-m8-additional-$Backend-$suffix"
$cContainer = "dilock-kotlin-m8-c-worker-$suffix"
$serviceAlias = if ($Backend -eq 'redis') { 'redis' } else { 'etcd' }
$redisImage = 'redis@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935'
$etcdImage = 'quay.io/coreos/etcd@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd'
$cBuilderImage = if ([string]::IsNullOrWhiteSpace($CBuilderImageOverride)) {
    'dilock-cpp-etcd-builder:20261006@sha256:f189fb7ee3d738ae3ead5455e9eb535fbdbb4ec6bb0c756826b89fba7c6c34ed'
} else {
    $CBuilderImageOverride
}
$image = if ($Backend -eq 'redis') { $redisImage } else { $etcdImage }
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$hostPort = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
$listener.Stop()
$endpoint = if ($Backend -eq 'redis') { "redis://127.0.0.1:$hostPort" } else { "http://127.0.0.1:$hostPort" }
$workerEndpoint = if ($Backend -eq 'redis') { 'redis://redis:6379' } else { 'http://etcd:2379' }
$python = Join-Path $root '.tools/venv/Scripts/python.exe'
$runDir = Join-Path $root "validation/implementation/run-m8-kotlin-additional-$suffix"
$reportFullPath = Join-Path $root $ReportPath
$partners = @($Partners | Select-Object -Unique)
$runs = [Collections.Generic.List[object]]::new()
$serviceStarted = $false
$cBuilderStarted = $false
$cBuildStatus = 'not_run'
$cBuildError = $null
$cRuntimeLibs = '/tmp/c-current-prefix/lib:/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu:/workspace/.tools/cppinstall/lib:/workspace/.tools/cppdeps/usr/lib/x86_64-linux-gnu:/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu'
$depPrefix = '/workspace/.tools/grpc-root/usr;/workspace/.tools/cppinstall;/workspace/.tools/cppdeps;/workspace/.tools/openssl/usr;/workspace/.tools/openssl-runtime/usr'
$installPrefix = '/tmp/c-current-prefix'
$cMakeFlags = @(
    '-DCMAKE_BUILD_TYPE=Release',
    "-DCMAKE_PREFIX_PATH=$depPrefix",
    '-DCMAKE_INSTALL_PREFIX=/tmp/c-current-prefix',
    '-DBUILD_SHARED_LIBS=ON',
    '-DDILOCK_BUILD_TESTS=OFF',
    '-DDILOCK_BUILD_ETCD=ON',
    '-DDILOCK_BUILD_C_API=ON',
    '-DOPENSSL_ROOT_DIR=/workspace/.tools/openssl/usr',
    '-DOPENSSL_INCLUDE_DIR=/workspace/.tools/openssl/usr/include',
    '-DOPENSSL_CRYPTO_LIBRARY=/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu/libcrypto.so.3',
    '-DOPENSSL_SSL_LIBRARY=/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu/libssl.so.3',
    '-DOPENSSL_USE_STATIC_LIBS=FALSE',
    '-DZLIB_LIBRARY=/usr/lib/x86_64-linux-gnu/libz.so.1',
    '-DZLIB_INCLUDE_DIR=/workspace/.tools/grpc-root/usr/include',
    '-DgRPC_DIR=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/cmake/grpc',
    '-DProtobuf_INCLUDE_DIR=/workspace/.tools/grpc-root/usr/include',
    '-DProtobuf_LIBRARY=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/libprotobuf.so',
    '-DProtobuf_PROTOC_EXECUTABLE=/workspace/.tools/grpc-root/usr/bin/protoc',
    '-DPKG_CONFIG_EXECUTABLE=/workspace/.tools/grpc-root/usr/bin/pkg-config',
    '-Dnlohmann_json_DIR=/workspace/.tools/cppdeps/usr/share/cmake/nlohmann_json',
    '-Dredis++_DIR=/workspace/.tools/cppinstall/share/cmake/redis++',
    '-Dhiredis_DIR=/workspace/.tools/cppinstall/lib/cmake/hiredis'
)
$report = [ordered]@{
    schema_version = 1
    date = (Get-Date).ToString('yyyy-MM-dd')
    sdk = 'kotlin'
    backend = $Backend
    scope = 'Kotlin against Scala, Elixir and C in both directions'
    status = 'in_progress'
    endpoint = $endpoint
    worker_endpoint = $workerEndpoint
    backend_image = $image
    c_builder_image = $cBuilderImage
    service_container = $serviceName
    c_worker_container = $cContainer
    docker_network = $network
    run_directory = [IO.Path]::GetRelativePath($root, $runDir)
    c_build = [ordered]@{ status = 'not_run' }
    runs = $runs
    cleanup = [ordered]@{ status = 'pending' }
}

function Save-Report {
    $report.runs = @($runs.ToArray())
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

function Invoke-CWorkerBuild {
    $configureFlags = $cMakeFlags
    docker exec -e 'PKG_CONFIG_SYSROOT_DIR=/workspace/.tools/grpc-root' `
        -e 'PKG_CONFIG_PATH=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/pkgconfig' `
        -e 'PKG_CONFIG_LIBDIR=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/pkgconfig' `
        -e "LD_LIBRARY_PATH=$cRuntimeLibs" `
        $cContainer cmake -S /workspace/sdk/cpp -B /tmp/c-current-build -G 'Unix Makefiles' @configureFlags
    if ($LASTEXITCODE -ne 0) { throw 'Current C ABI/C++ backend CMake configure failed in the pinned local builder.' }
    docker exec -e "LD_LIBRARY_PATH=$cRuntimeLibs" $cContainer cmake --build /tmp/c-current-build --parallel 3
    if ($LASTEXITCODE -ne 0) { throw 'Current C ABI/C++ backend build failed in the pinned local builder.' }
    docker exec $cContainer cmake --install /tmp/c-current-build
    if ($LASTEXITCODE -ne 0) { throw 'Current C ABI install failed in the pinned local builder.' }
    $workerPrefix = "/tmp/c-current-prefix;$depPrefix"
    docker exec -e 'PKG_CONFIG_SYSROOT_DIR=/workspace/.tools/grpc-root' `
        -e 'PKG_CONFIG_PATH=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/pkgconfig' `
        -e 'PKG_CONFIG_LIBDIR=/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu/pkgconfig' `
        -e "LD_LIBRARY_PATH=$cRuntimeLibs" `
        $cContainer cmake -S /workspace/sdk/c/tests/conformance -B /tmp/c-m8-worker -G 'Unix Makefiles' `
        '-DCMAKE_BUILD_TYPE=Release' "-DCMAKE_PREFIX_PATH=$workerPrefix" `
        '-DDilockCpp_DIR=/tmp/c-current-prefix/lib/cmake/DilockCpp' `
        '-Dnlohmann_json_DIR=/workspace/.tools/cppdeps/usr/share/cmake/nlohmann_json'
    if ($LASTEXITCODE -ne 0) { throw 'C ABI conformance worker configure failed in the pinned local builder.' }
    docker exec -e "LD_LIBRARY_PATH=$cRuntimeLibs" $cContainer cmake --build /tmp/c-m8-worker --parallel 2
    if ($LASTEXITCODE -ne 0) { throw 'C ABI conformance worker build failed in the pinned local builder.' }
}

try {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Repository Python environment is missing: $python" }
    if (Test-Path -LiteralPath $reportFullPath) { throw "Refusing to overwrite existing evidence: $ReportPath" }
    if (Test-Path -LiteralPath $runDir) { throw "Refusing to reuse existing run directory: $runDir" }
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    New-Item -ItemType Directory -Path (Split-Path -Parent $reportFullPath) -Force | Out-Null

    docker network create $network | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the task-owned Docker network.' }
    if ($Backend -eq 'redis') {
        docker run -d --rm --pull=never --network $network --network-alias redis `
            --publish "127.0.0.1:${hostPort}:6379" --name $serviceName $image `
            redis-server --save '' --appendonly no --port 6379 | Out-Null
    } else {
        docker run -d --rm --pull=never --network $network --network-alias etcd `
            --publish "127.0.0.1:${hostPort}:2379" --name $serviceName $image `
            /usr/local/bin/etcd '--name=etcd' '--data-dir=/tmp/dilock-etcd.data' `
            '--listen-client-urls=http://0.0.0.0:2379' '--advertise-client-urls=http://etcd:2379' `
            '--listen-peer-urls=http://0.0.0.0:2380' '--initial-advertise-peer-urls=http://etcd:2380' `
            '--initial-cluster=etcd=http://etcd:2380' "--initial-cluster-token=dilock-kotlin-m8-$suffix" `
            '--initial-cluster-state=new' | Out-Null
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not start disposable $Backend service." }
    $serviceStarted = $true
    if (-not (Test-ServiceReady)) { throw "Disposable $Backend service failed its bounded readiness check." }

    if ($SkipCBuild) {
        $cBuildStatus = 'blocked'
        $cBuildError = 'C build was explicitly skipped by the caller; Kotlin/C directed cells were not run.'
    } else {
        docker run -d --rm --pull=never --network $network --name $cContainer `
            --mount "type=bind,source=$($root.Replace('\','/')),target=/workspace,readonly" `
            --entrypoint sh $cBuilderImage -c 'sleep infinity' | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Could not start task-owned C worker builder container.' }
        $cBuilderStarted = $true
        try {
            Invoke-CWorkerBuild
            $cBuildStatus = 'passed'
        } catch {
            $cBuildStatus = 'blocked'
            $cBuildError = $_.Exception.Message
        }
    }
    $report.c_build = [ordered]@{
        status = $cBuildStatus
        error = $cBuildError
        image = $cBuilderImage
        binary = '/tmp/c-m8-worker/dilock_c_conformance_worker'
        runtime_library_path = $cRuntimeLibs
        commands = @(
            'cmake -S /workspace/sdk/cpp -B /tmp/c-current-build -G Unix Makefiles [pinned cached dependency paths] -DBUILD_SHARED_LIBS=ON -DDILOCK_BUILD_TESTS=OFF -DDILOCK_BUILD_ETCD=ON -DDILOCK_BUILD_C_API=ON',
            'docker exec -e LD_LIBRARY_PATH=<cached-runtime-paths> <builder> cmake --build /tmp/c-current-build --parallel 3',
            'cmake --install /tmp/c-current-build',
            'cmake -S /workspace/sdk/c/tests/conformance -B /tmp/c-m8-worker -G Unix Makefiles -DDilockCpp_DIR=/tmp/c-current-prefix/lib/cmake/DilockCpp',
            'cmake --build /tmp/c-m8-worker --parallel 2'
        )
    }
    Save-Report

    $env:DILOCK_M8_DOCKER_NETWORK = $network
    $env:DILOCK_M8_WORKER_ENDPOINT = $workerEndpoint
    $env:DILOCK_SCALA_WORKER_NETWORK = $network
    $env:DILOCK_SCALA_REDIS_WORKER_ENDPOINT = if ($Backend -eq 'redis') { $workerEndpoint } else { $null }
    $env:DILOCK_SCALA_ETCD_WORKER_ENDPOINT = if ($Backend -eq 'etcd') { $workerEndpoint } else { $null }
    $env:DILOCK_C_WORKER_CONTAINER = $cContainer
    $env:DILOCK_C_WORKER_BINARY = '/tmp/c-m8-worker/dilock_c_conformance_worker'
    $env:DILOCK_C_WORKER_LD_LIBRARY_PATH = $cRuntimeLibs
    foreach ($partner in $partners) {
        $name = "kotlin-$Backend-$partner"
        $outputRelative = [IO.Path]::GetRelativePath($root, (Join-Path $runDir "$name.json"))
        $logPath = Join-Path $runDir "$name.log"
        if ($partner -eq 'c' -and $cBuildStatus -ne 'passed') {
            $runs.Add([ordered]@{ partner = $partner; status = 'blocked'; command = 'not_run: C worker build blocked'; exit_code = $null; report = $null; log = $null; cells = @(@{ status = 'blocked'; detail = $cBuildError }) })
            Save-Report
            continue
        }
        $command = "$python conformance/run.py --backend $Backend --workers sdk/kotlin/worker/m8-additional-workers.json --sdks kotlin $partner --endpoint $endpoint --output $outputRelative"
        Write-Host "Running $name on $workerEndpoint"
        & $python 'conformance/run.py' --backend $Backend --workers 'sdk/kotlin/worker/m8-additional-workers.json' --sdks kotlin $partner --endpoint $endpoint --output $outputRelative 2>&1 | Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
        $cells = @()
        $pairPath = Join-Path $root $outputRelative
        if (Test-Path -LiteralPath $pairPath) {
            try {
                $pairReport = Get-Content -LiteralPath $pairPath -Raw | ConvertFrom-Json
                $cells = @($pairReport.results | ForEach-Object { [ordered]@{ holder = $_.holder; contender = $_.contender; status = $_.status; seconds = $_.seconds; scenarios = $_.scenarios } })
            } catch { $cells = @([ordered]@{ status = 'report_parse_failed'; detail = $_.Exception.Message }) }
        } else {
            $cells = @([ordered]@{ status = 'not_run'; detail = 'Pair runner produced no JSON report.' })
        }
        $status = if ($exitCode -eq 0 -and $cells.Count -eq 2 -and @($cells | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } else { 'failed' }
        $runs.Add([ordered]@{ partner = $partner; status = $status; command = $command; exit_code = $exitCode; report = $outputRelative; log = [IO.Path]::GetRelativePath($root, $logPath); cells = $cells })
        Save-Report
    }
    $report.status = if (@($runs | Where-Object status -ne 'passed').Count -eq 0) { 'passed' } elseif (@($runs | Where-Object status -eq 'passed').Count -gt 0) { 'partial' } else { 'failed' }
}
catch {
    $report.status = 'blocked'
    $report.blocker = $_.Exception.Message
    Save-Report
    throw
}
finally {
    foreach ($variable in @('DILOCK_M8_DOCKER_NETWORK','DILOCK_M8_WORKER_ENDPOINT','DILOCK_SCALA_WORKER_NETWORK','DILOCK_SCALA_REDIS_WORKER_ENDPOINT','DILOCK_SCALA_ETCD_WORKER_ENDPOINT','DILOCK_C_WORKER_CONTAINER','DILOCK_C_WORKER_BINARY','DILOCK_C_WORKER_LD_LIBRARY_PATH')) {
        Remove-Item "Env:\$variable" -ErrorAction SilentlyContinue
    }
    docker stop $cContainer 2>$null | Out-Null
    docker stop $serviceName 2>$null | Out-Null
    docker network rm $network 2>$null | Out-Null
    $inventoryOk = $true
    $remainingContainers = @(docker ps -a --filter "name=$serviceName" --format '{{.Names}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingCContainers = @(docker ps -a --filter "name=$cContainer" --format '{{.Names}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $remainingNetworks = @(docker network ls --filter "name=$network" --format '{{.Name}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $inventoryOk = $false }
    $report.cleanup = [ordered]@{
        status = if (-not $inventoryOk) { 'blocked' } elseif ($remainingContainers.Count + $remainingCContainers.Count + $remainingNetworks.Count -eq 0) { 'passed' } else { 'failed' }
        inventory_status = if ($inventoryOk) { 'completed' } else { 'blocked' }
        remaining_containers = @($remainingContainers + $remainingCContainers)
        remaining_networks = $remainingNetworks
        service_was_started = $serviceStarted
        c_builder_was_started = $cBuilderStarted
    }
    if ($report.status -eq 'in_progress') { $report.status = 'not_run' }
    Save-Report
}

if ($report.status -ne 'passed' -or $report.cleanup.status -ne 'passed') { exit 1 }
Write-Host "All directed Kotlin/$Backend pairs for $($partners -join ', ') passed. Evidence: $ReportPath"

