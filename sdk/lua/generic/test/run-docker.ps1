$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$context = $root.Replace('\', '/')
$repo = (Resolve-Path (Join-Path $root '..\..\..')).Path
$vectors = (Join-Path $repo 'spec\vectors').Replace('\', '/')
$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 12)
$network = "dilock-lua-generic-$suffix"
$redis = "dilock-lua-redis-$suffix"
$baseImage = 'debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251'
$redisImage = 'redis:8.10.2@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935'
$exitCode = 1
$networkCreated = $false

try {
    & docker network create $network
    if ($LASTEXITCODE -ne 0) { throw "docker network create failed: $LASTEXITCODE" }
    $networkCreated = $true

    & docker run -d --name $redis --network $network --network-alias redis $redisImage
    if ($LASTEXITCODE -ne 0) { throw "Redis container failed: $LASTEXITCODE" }
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        & docker exec $redis redis-cli ping 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($LASTEXITCODE -ne 0) { throw 'local Redis did not become ready in 30 seconds' }

    $installAndTest = @'
set -eu
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends lua5.1 lua5.1-dev luajit libluajit-5.1-dev luarocks build-essential libssl-dev ca-certificates
luarocks --lua-version=5.1 install luasocket 3.1.0-1
luarocks --lua-version=5.1 install luaossl 20260910-0
luarocks --lua-version=5.1 install dkjson 2.11-1
luarocks --lua-version=5.1 install luasystem 0.7.1-1
luarocks --lua-version=5.1 install redis-lua 2.0.4-1
exec /src/test/run-consumer.sh
'@
    & docker run --rm --network $network --mount "type=bind,source=$context,target=/src,readonly" --mount "type=bind,source=$vectors,target=/spec/vectors,readonly" -e REDIS_HOST=redis $baseImage sh -ec $installAndTest
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) { throw "Lua consumer validation failed: $exitCode" }
}
finally {
    & docker rm -f $redis 2>$null | Out-Null
    if ($networkCreated) { & docker network rm $network 2>$null | Out-Null }
}

if ($exitCode -ne 0) { exit $exitCode }
Write-Output 'Docker validation and owned-resource cleanup: PASS'
