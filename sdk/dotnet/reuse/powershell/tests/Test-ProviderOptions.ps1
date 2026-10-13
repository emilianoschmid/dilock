$ErrorActionPreference = 'Stop'

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
    if ($null -eq $provider) { throw "$functionName is missing." }

    $parameterNames = @($provider.Body.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
    if ('Options' -notin $parameterNames) { throw "$functionName must accept a complete AcquireOptions policy." }

    $body = $provider.Body.Extent.Text
    foreach ($required in @(
        '$PSBoundParameters.ContainsKey(''Options'')',
        'Pass either -Options or individual acquisition options, not both.',
        '$Options.Validate()',
        '$configuration.DefaultAcquisition = $acquisition'
    )) {
        if ($body -notlike "*$required*") { throw "$functionName does not honor provider option invariant: $required" }
    }
}

$redis = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'New-DilockRedisProvider'
}, $true)
foreach ($expected in @{
    WaitTimeout = '([TimeSpan]::FromSeconds(5))'
    LeaseDuration = '([TimeSpan]::FromSeconds(30))'
    RpcTimeout = '([TimeSpan]::FromSeconds(2))'
    CleanupTimeout = '([TimeSpan]::FromSeconds(2))'
}.GetEnumerator()) {
    $parameter = $redis.Body.ParamBlock.Parameters | Where-Object { $_.Name.VariablePath.UserPath -eq $expected.Key }
    if ($null -eq $parameter -or $null -eq $parameter.DefaultValue -or
        $parameter.DefaultValue.Extent.Text -ne $expected.Value) {
        throw "Redis provider '$($expected.Key)' default must be $($expected.Value)."
    }
}

Write-Output 'PASS Redis and etcd provider default and complete-policy source regressions.'
