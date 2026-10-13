$ErrorActionPreference = 'Stop'

$coreAssembly = Join-Path $PSScriptRoot '..' '..' '..' 'Dilock.Core' 'bin' 'Release' 'net8.0' 'Dilock.Core.dll'
if (-not (Test-Path -LiteralPath $coreAssembly -PathType Leaf)) {
    throw "Current Dilock.Core assembly is unavailable for API-boundary checks: $coreAssembly"
}
Add-Type -Path (Resolve-Path -LiteralPath $coreAssembly).Path

$modulePath = Join-Path $PSScriptRoot '..' 'module' 'Dilock' 'Dilock.psm1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath $modulePath).Path, [ref] $tokens, [ref] $parseErrors)
if ($parseErrors.Count -ne 0) { throw "Module parse failed: $($parseErrors[0].Message)" }
$function = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Invoke-DilockAcquire'
}, $true)
if ($null -eq $function) { throw 'Invoke-DilockAcquire is missing.' }
Invoke-Expression $function.Extent.Text

$fakeLock = [pscustomobject]@{ Calls = [System.Collections.Generic.List[object]]::new() }
$fakeLock | Add-Member ScriptMethod AcquireAsync {
    $this.Calls.Add(@($args))
    return [System.Threading.Tasks.Task[object]]::FromResult([object]'lease')
}

$token = [System.Threading.CancellationToken]::None
$result = Invoke-DilockAcquire -Lock $fakeLock -CancellationToken $token
if ($result -ne 'lease' -or $fakeLock.Calls.Count -ne 1) { throw 'Default acquisition did not make exactly one call.' }
$defaultCall = $fakeLock.Calls[0]
if ($defaultCall.Count -ne 1 -or $defaultCall[0] -isnot [System.Threading.CancellationToken]) {
    throw 'Default acquisition did not use AcquireAsync(CancellationToken), so provider defaults would be ignored.'
}

$fakeLock.Calls.Clear()
$result = Invoke-DilockAcquire -Lock $fakeLock -WaitTimeout ([TimeSpan]::FromSeconds(9)) -CancellationToken $token
if ($result -ne 'lease' -or $fakeLock.Calls.Count -ne 1) { throw 'Explicit acquisition did not make exactly one call.' }
$customCall = $fakeLock.Calls[0]
if ($customCall.Count -ne 2 -or $customCall[0] -isnot [Dilock.AcquireOptions]) {
    throw 'Explicit acquisition did not use the complete AcquireOptions overload.'
}
if ($customCall[0].WaitTimeout -ne [TimeSpan]::FromSeconds(9)) {
    throw 'Explicit wait timeout was not preserved.'
}

$fakeLock.Calls.Clear()
$explicit = [Dilock.AcquireOptions]::new()
$result = Invoke-DilockAcquire -Lock $fakeLock -Options $explicit -CancellationToken $token
if ($result -ne 'lease' -or $fakeLock.Calls.Count -ne 1 -or $fakeLock.Calls[0].Count -ne 2) {
    throw 'Explicit Options did not use the complete policy overload.'
}

$conflict = $false
try {
    Invoke-DilockAcquire -Lock $fakeLock -Options $explicit -WaitTimeout ([TimeSpan]::FromSeconds(3)) -CancellationToken $token | Out-Null
}
catch { $conflict = $_.Exception.Message -match 'either -Options or individual acquisition options' }
if (-not $conflict) { throw 'Combining -Options with individual policy parameters was not rejected.' }

Write-Output 'PASS PowerShell provider-default, explicit-policy, and conflicting-policy dispatch checks.'
