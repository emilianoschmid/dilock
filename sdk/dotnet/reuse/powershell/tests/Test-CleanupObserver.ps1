$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..' '..' '..' '..')).Path
$coreAssembly = Join-Path $repoRoot 'sdk' 'dotnet' 'Dilock.Core' 'bin' 'Release' 'net8.0' 'Dilock.Core.dll'
if (-not (Test-Path -LiteralPath $coreAssembly -PathType Leaf)) {
    throw "Current Dilock.Core assembly is unavailable: $coreAssembly"
}
Add-Type -Path (Resolve-Path -LiteralPath $coreAssembly).Path

$modulePath = Join-Path $PSScriptRoot '..' 'module' 'Dilock' 'Dilock.psm1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath $modulePath).Path, [ref] $tokens, [ref] $parseErrors)
if ($parseErrors.Count -ne 0) { throw "Module parse failed: $($parseErrors[0].Message)" }

foreach ($functionName in @('New-DilockRedisProvider', 'New-DilockEtcdProvider')) {
    $provider = $ast.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $functionName
    }, $true)
    if ($null -eq $provider -or $provider.Body.Extent.Text -notmatch '\$configuration\.CleanupObserver\s*=\s*New-DilockCleanupObserver') {
        throw "$functionName does not connect the .NET cleanup observer to provider scope cleanup."
    }
}

$observerAst = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'New-DilockCleanupObserver'
}, $true)
if ($null -eq $observerAst) { throw 'New-DilockCleanupObserver is missing.' }
if ($observerAst.Body.Extent.Text -notmatch '\[Dilock\.ReleaseOutcome\]::Unknown' -or
    $observerAst.Body.Extent.Text -notmatch '\[Console\]::Error\.WriteLine') {
    throw 'The observer must report Unknown cleanup to standard error.'
}
Invoke-Expression $observerAst.Extent.Text
$observer = New-DilockCleanupObserver

$examplePath = Join-Path $PSScriptRoot '..' 'Invoke-DilockExample.ps1'
$exampleTokens = $null
$exampleErrors = $null
$exampleAst = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath $examplePath).Path, [ref] $exampleTokens, [ref] $exampleErrors)
if ($exampleErrors.Count -ne 0) { throw "Standalone example parse failed: $($exampleErrors[0].Message)" }
if ($exampleAst.Extent.Text -notmatch '\$providerOptions\.CleanupObserver\s*=\s*\$cleanupObserver') {
    throw 'Standalone Redis/etcd example does not configure the cleanup observer.'
}

$originalError = [Console]::Error
$writer = [System.IO.StringWriter]::new()
try {
    [Console]::SetError($writer)
    $observer.Invoke([Dilock.CleanupObservation]::new(
        'redis-single-authority-v1', [Dilock.ReleaseOutcome]::Released, 'explicit release'))
    if ($writer.ToString().Length -ne 0) { throw 'Acknowledged cleanup unexpectedly emitted an uncertainty warning.' }

    $observer.Invoke([Dilock.CleanupObservation]::new(
        'etcd-native-lock-v1', [Dilock.ReleaseOutcome]::Unknown, 'lease scope disposal'))
    $warning = $writer.ToString()
    if ($warning -notmatch 'etcd-native-lock-v1' -or $warning -notmatch 'lease scope disposal') {
        throw 'Unknown cleanup was not reported with backend and reason.'
    }
}
finally {
    [Console]::SetError($originalError)
    $writer.Dispose()
}

Write-Output 'PASS PowerShell Redis/etcd provider options report Unknown scope cleanup without warning on acknowledged cleanup.'
