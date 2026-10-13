$ErrorActionPreference = 'Stop'
$packageRoot = Split-Path -Parent $PSScriptRoot
$protoc = Join-Path $packageRoot '..\..\..\.tools\protoc-31.1\bin\protoc.exe'
$packageConfig = Join-Path $packageRoot '.dart_tool\package_config.json'
$plugin = Join-Path $packageRoot '.dart_tool\pub\bin\protoc_plugin\protoc-gen-dart.bat'
$protoRoot = Join-Path $packageRoot 'proto\etcd-v3.7.2'
$output = Join-Path $packageRoot 'lib\src\generated'

if (-not (Test-Path -LiteralPath $protoc)) {
  throw 'protoc was not found. Install the pinned local Protocol Buffers compiler.'
}
if (-not (Test-Path -LiteralPath $packageConfig)) {
  throw 'Dart package configuration is missing. Run dart pub get before generating stubs.'
}

$config = Get-Content -LiteralPath $packageConfig -Raw | ConvertFrom-Json
$pluginPackage = $config.packages | Where-Object name -eq 'protoc_plugin' | Select-Object -First 1
if ($null -eq $pluginPackage) {
  throw 'protoc_plugin is not in .dart_tool/package_config.json. Resolve the declared dev dependency first.'
}
$rootUri = [Uri]$pluginPackage.rootUri
$pluginRoot = if ($rootUri.IsAbsoluteUri) { $rootUri.LocalPath } else { Join-Path $packageRoot $pluginPackage.rootUri }
$pluginScript = Join-Path $pluginRoot 'bin\protoc_plugin.dart'
if (-not (Test-Path -LiteralPath $pluginScript)) {
  throw 'The resolved protoc_plugin package does not contain bin/protoc_plugin.dart.'
}

# protoc needs an executable plugin. Run the resolved project package with the
# package configuration that dart pub get generated for this package.
$dartCommand = Get-Command dart -ErrorAction Stop
$pluginDirectory = Split-Path -Parent $plugin
New-Item -ItemType Directory -Force -Path $pluginDirectory | Out-Null
$pluginWrapper = @"
@echo off
call "$($dartCommand.Source)" --packages="$packageConfig" "$pluginScript" -c %*
exit /b %ERRORLEVEL%
"@
Set-Content -LiteralPath $plugin -Value $pluginWrapper -Encoding Ascii

New-Item -ItemType Directory -Force -Path $output | Out-Null
& $protoc "-I$protoRoot" "--plugin=protoc-gen-dart=$plugin" "--dart_out=grpc:$output" `
  (Join-Path $protoRoot 'kv.proto') (Join-Path $protoRoot 'rpc.proto') (Join-Path $protoRoot 'v3lock.proto')
if ($LASTEXITCODE -ne 0) { throw "protoc failed with exit code $LASTEXITCODE" }
