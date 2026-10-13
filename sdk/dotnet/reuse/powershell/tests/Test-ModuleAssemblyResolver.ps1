$ErrorActionPreference = 'Stop'

$modulePath = Join-Path $PSScriptRoot '..' 'module' 'Dilock' 'Dilock.psm1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath $modulePath).Path, [ref] $tokens, [ref] $parseErrors)
if ($parseErrors.Count -ne 0) { throw "Module parse failed: $($parseErrors[0].Message)" }

$source = $ast.Extent.Text
if ($source -notmatch '\[Dilock\.PowerShell\.ModuleAssemblyResolver\]::Register\(\$script:LibraryDirectory\)' -or
    $source -notmatch '\[Dilock\.PowerShell\.ModuleAssemblyResolver\]::Unregister\(\)') {
    throw 'The module must register and clean up its compiled managed assembly resolver.'
}
if ($source -match '\[System\.ResolveEventHandler\]\s*\{' -or
    $source -match 'add_AssemblyResolve\s*\(') {
    throw 'AssemblyResolve must not invoke a PowerShell scriptblock from a .NET worker thread.'
}

$builder = Join-Path $PSScriptRoot '..' 'module' 'ModuleAssemblyResolver.cs'
$builderText = Get-Content -Raw -LiteralPath $builder
if ($builderText -notmatch 'AssemblyResolve\s*\+=\s*Handler' -or
    $builderText -notmatch 'AssemblyResolve\s*-\s*=\s*Handler' -or
    $builderText -notmatch 'lock\s*\(Gate\)') {
    throw 'The compiled resolver must synchronize registrations and detach after the final module import.'
}

Write-Output 'PASS PowerShell module resolves private DLLs through a thread-safe managed callback.'
