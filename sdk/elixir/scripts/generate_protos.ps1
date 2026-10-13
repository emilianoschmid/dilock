$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$protobufRoot = Join-Path $projectRoot 'deps/protobuf'
$protoRoot = Join-Path $projectRoot 'proto/etcd-v3.7.2'
$outputRoot = Join-Path $projectRoot 'lib/dilock/etcd/generated'
$elixirPlugin = Join-Path $protobufRoot 'protoc-gen-elixir'
if (-not (Test-Path -LiteralPath $elixirPlugin)) {
    if (-not (Test-Path -LiteralPath (Join-Path $protobufRoot 'mix.exs'))) {
        throw "Missing the pinned Protobuf dependency at '$protobufRoot'; run 'mix deps.get' in '$projectRoot' first"
    }
    if (-not (Get-Command mix -ErrorAction SilentlyContinue)) {
        throw "Cannot build protoc-gen-elixir: Mix is unavailable. Install the Elixir toolchain, then run 'mix escript.build' in '$protobufRoot'"
    }
    Push-Location $protobufRoot
    $previousMixEnv = $env:MIX_ENV
    try {
        $env:MIX_ENV = 'prod'
        & mix deps.get --only prod
        if ($LASTEXITCODE -ne 0) { throw "mix deps.get --only prod exited with $LASTEXITCODE" }
        & mix escript.build
        if ($LASTEXITCODE -ne 0) { throw "mix escript.build exited with $LASTEXITCODE" }
    } finally {
        if ($null -eq $previousMixEnv) { Remove-Item Env:MIX_ENV -ErrorAction SilentlyContinue }
        else { $env:MIX_ENV = $previousMixEnv }
        Pop-Location
    }
    if (-not (Test-Path -LiteralPath $elixirPlugin)) {
        throw "mix escript.build succeeded but did not create the expected plugin at '$elixirPlugin'"
    }
}
Push-Location $projectRoot
try {
    if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) {
        throw "protoc is unavailable; install the pinned protoc 31.1 compiler or add it to PATH"
    }
    $protocVersion = & protoc --version
    if ($LASTEXITCODE -ne 0) { throw "protoc --version failed with $LASTEXITCODE" }
    if ($protocVersion -ne 'libprotoc 31.1') {
        throw "Expected pinned protoc 31.1, found '$protocVersion'"
    }
    New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
    & protoc "--plugin=protoc-gen-elixir=$elixirPlugin" "--elixir_out=plugins=grpc:$outputRoot" "--proto_path=$protoRoot" `
        (Join-Path $protoRoot 'kv.proto') `
        (Join-Path $protoRoot 'rpc.proto') `
        (Join-Path $protoRoot 'v3lock.proto')
    if ($LASTEXITCODE -ne 0) { throw "protoc exited with $LASTEXITCODE" }
} finally {
    Pop-Location
}
