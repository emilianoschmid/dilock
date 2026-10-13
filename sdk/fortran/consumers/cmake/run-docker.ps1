$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
$cAbiRoot = Join-Path $repoRoot '.tools\cabi-ffi-linked-20261006'
$cAbiInstall = Join-Path $cAbiRoot 'install'
$cAbiDeps = Join-Path $cAbiRoot 'deps'
foreach ($requiredPath in @($cAbiInstall, $cAbiDeps)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Required local C ABI prefix is missing: $requiredPath"
    }
}

$containerName = 'dilock-fortran-cmake-consumer-' + [Guid]::NewGuid().ToString('N').Substring(0, 8)
$containerScript = @'
set -e
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gfortran cmake ninja-build libssl-dev nlohmann-json3-dev libprotobuf-dev protobuf-compiler-grpc libgrpc++-dev >/dev/null
cmake -S /workspace/sdk/fortran -B /tmp/fortran-build -G Ninja -DCMAKE_PREFIX_PATH='/abi/install;/abi/deps' -DBUILD_TESTING=ON
cmake --build /tmp/fortran-build --parallel 2
ctest --test-dir /tmp/fortran-build --output-on-failure
cmake --install /tmp/fortran-build --prefix /tmp/fortran-install
cmake -S /workspace/sdk/fortran/consumers/cmake -B /tmp/fortran-consumer -G Ninja -DCMAKE_PREFIX_PATH='/tmp/fortran-install;/abi/install;/abi/deps'
cmake --build /tmp/fortran-consumer --parallel 2
LD_LIBRARY_PATH='/abi/install/lib:/abi/deps/lib' /tmp/fortran-consumer/dilock_fortran_installed_consumer
'@

& docker run --rm --name $containerName `
    --mount "type=bind,source=$repoRoot,target=/workspace,readonly" `
    --mount "type=bind,source=$cAbiInstall,target=/abi/install,readonly" `
    --mount "type=bind,source=$cAbiDeps,target=/abi/deps,readonly" `
    --workdir /workspace `
    'debian@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251' `
    bash -lc $containerScript
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
