$ErrorActionPreference = 'Stop'

$modulePath = Join-Path $PSScriptRoot '..' 'module' 'Dilock' 'Dilock.psm1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath $modulePath).Path, [ref] $tokens, [ref] $parseErrors)
if ($parseErrors.Count -ne 0) { throw "Module parse failed: $($parseErrors[0].Message)" }

$provider = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'New-DilockRedisProvider'
}, $true)
if ($null -eq $provider) { throw 'New-DilockRedisProvider is missing.' }

foreach ($expected in @{
    WaitTimeout = '([TimeSpan]::FromSeconds(5))'
    LeaseDuration = '([TimeSpan]::FromSeconds(30))'
}.GetEnumerator()) {
    $parameter = $provider.Body.ParamBlock.Parameters | Where-Object { $_.Name.VariablePath.UserPath -eq $expected.Key }
    if ($null -eq $parameter -or $null -eq $parameter.DefaultValue) {
        throw "Redis provider parameter '$($expected.Key)' must have an explicit default."
    }
    if ($parameter.DefaultValue.Extent.Text -ne $expected.Value) {
        throw "Redis provider parameter '$($expected.Key)' default was '$($parameter.DefaultValue.Extent.Text)', expected '$($expected.Value)'."
    }
}

$body = $provider.Body.Extent.Text
if ($body -notmatch 'New-DilockAcquireOptions\s+-WaitTimeout\s+\$WaitTimeout\s+-LeaseDuration\s+\$LeaseDuration') {
    throw 'Redis provider must pass the defaulted durations into the provider acquisition policy.'
}

Write-Output 'PASS Redis provider supplies the shared 5s wait and 30s lease defaults to its default acquisition policy.'
