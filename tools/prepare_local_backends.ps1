param([switch]$StartEtcd)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path $PSScriptRoot -Parent
$toolRoot = Join-Path $taskRoot '.tools'
New-Item -ItemType Directory -Force $toolRoot | Out-Null
# Official upstream binaries, isolated from installed services and user data.
$version = 'v3.7.2'
$zip = Join-Path $toolRoot "etcd-$version-windows-amd64.zip"
if (!(Test-Path $zip)) {
  Invoke-WebRequest "https://github.com/etcd-io/etcd/releases/download/$version/etcd-$version-windows-amd64.zip" -OutFile $zip
}
Get-FileHash $zip -Algorithm SHA256 | Format-List
$dest = Join-Path $toolRoot 'etcd'
if (!(Test-Path $dest)) { Expand-Archive -LiteralPath $zip -DestinationPath $dest }
if ($StartEtcd) {
  $binary = Get-ChildItem $dest -Recurse -Filter etcd.exe | Select-Object -First 1
  $data = Join-Path $taskRoot '.tmp/etcd-functional'
  if (Test-Path $data) { throw 'Use a fresh data directory; do not reuse or overwrite an earlier run.' }
  Start-Process -FilePath $binary.FullName -WindowStyle Hidden -ArgumentList @('--name','dilock-test','--data-dir',$data,'--listen-client-urls','http://127.0.0.1:22379','--advertise-client-urls','http://127.0.0.1:22379','--listen-peer-urls','http://127.0.0.1:22380','--initial-advertise-peer-urls','http://127.0.0.1:22380','--initial-cluster','dilock-test=http://127.0.0.1:22380') -RedirectStandardOutput (Join-Path $taskRoot 'validation/implementation/etcd.stdout.log') -RedirectStandardError (Join-Path $taskRoot 'validation/implementation/etcd.stderr.log') -PassThru | Select-Object Id
}
