$ErrorActionPreference = 'Stop'

$modulePath = Join-Path $PSScriptRoot '..' 'module' 'Dilock' 'Dilock.psm1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path $modulePath).Path, [ref] $tokens, [ref] $parseErrors)
if ($parseErrors.Count -ne 0) { throw "Module parse failed: $($parseErrors[0].Message)" }
$scopeFunction = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Invoke-DilockLeaseScope'
}, $true)
if ($null -eq $scopeFunction) { throw 'Invoke-DilockLeaseScope is missing.' }
Invoke-Expression $scopeFunction.Extent.Text

$script:ExitFailure = $null
$script:CleanupFailure = $null
$script:ExitOutcome = 'Released'
function Exit-Dilock {
    param($Lease)
    if ($null -ne $script:ExitFailure) { throw $script:ExitFailure }
    return $script:ExitOutcome
}
function Close-DilockLease {
    param($Lease)
    if ($null -ne $script:CleanupFailure) { throw $script:CleanupFailure }
}

$script:CleanupFailure = [InvalidOperationException]::new('cleanup acknowledgement unknown')
$bodyFailure = $null
try {
    Invoke-DilockLeaseScope -Lease ([pscustomobject]@{}) -ScriptBlock {
        throw [InvalidOperationException]::new('application failed')
    } | Out-Null
}
catch { $bodyFailure = $_.Exception }
if ($null -eq $bodyFailure -or $bodyFailure.Message -ne 'application failed') {
    throw 'Cleanup replaced the application failure.'
}
if ($bodyFailure.Data['Dilock.CleanupFailure'].Message -ne 'cleanup acknowledgement unknown') {
    throw 'Cleanup failure was not attached to the primary exception.'
}

$script:ExitFailure = [InvalidOperationException]::new('explicit release failed')
$releaseFailure = $null
try {
    Invoke-DilockLeaseScope -Lease ([pscustomobject]@{}) -ScriptBlock { 'work completed' } | Out-Null
}
catch { $releaseFailure = $_.Exception }
if ($null -eq $releaseFailure -or $releaseFailure.Message -ne 'explicit release failed') {
    throw 'Cleanup replaced the explicit release failure.'
}
if ($releaseFailure.Data['Dilock.CleanupFailure'].Message -ne 'cleanup acknowledgement unknown') {
    throw 'Cleanup failure was not attached to the release exception.'
}

$script:CleanupFailure = $null
$script:ExitFailure = $null
$script:ExitOutcome = 'AlreadyNotOwned'
$lostLeaseFailure = $null
try {
    Invoke-DilockLeaseScope -Lease ([pscustomobject]@{}) -ScriptBlock { 'work completed' } | Out-Null
}
catch { $lostLeaseFailure = $_.Exception }
if ($null -eq $lostLeaseFailure -or $lostLeaseFailure.Message -notlike '*Released acknowledgement*') {
    throw 'Scope returned successfully without a Released acknowledgement.'
}

Write-Output 'PASS PowerShell scope requires Released and preserves body/release failures when cleanup also fails.'
