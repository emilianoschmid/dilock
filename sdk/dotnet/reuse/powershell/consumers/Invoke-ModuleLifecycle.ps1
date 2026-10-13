[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('Redis', 'Etcd')][string] $Backend,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Endpoint,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $Epoch,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string] $ModuleDirectory
)

$ErrorActionPreference = 'Stop'
if (-not (Test-Path (Join-Path $ModuleDirectory 'Dilock.psm1') -PathType Leaf)) {
    throw 'Extracted module directory does not contain Dilock.psm1.'
}
Import-Module -Name (Join-Path $ModuleDirectory 'Dilock.psd1') -Force
    foreach ($command in @(
        'New-DilockRedisProvider', 'New-DilockEtcdProvider', 'Get-DilockLock',
        'Enter-Dilock', 'Exit-Dilock', 'Close-DilockLease', 'Close-DilockProvider', 'Invoke-Dilock'
    )) {
        if (-not (Get-Command -Name $command -ErrorAction SilentlyContinue)) { throw "Module export is missing: $command" }
    }

    # Prove the wrapper preserves pre-cancellation before it performs backend I/O.
    $cancelled = [System.Threading.CancellationTokenSource]::new()
    $cancelled.Cancel()
    try {
        if ($Backend -eq 'Redis') {
            New-DilockRedisProvider -Endpoint $Endpoint -Namespace 'default' -Epoch $Epoch -CancellationToken $cancelled.Token | Out-Null
        }
        else {
            New-DilockEtcdProvider -Endpoint $Endpoint -Namespace 'default' -Epoch $Epoch -CancellationToken $cancelled.Token | Out-Null
        }
        throw 'Pre-cancelled provider connection unexpectedly succeeded.'
    }
    catch [System.OperationCanceledException] { }
    finally { $cancelled.Dispose() }

    $provider = if ($Backend -eq 'Redis') {
        New-DilockRedisProvider -Endpoint $Endpoint -Namespace 'default' -Epoch $Epoch
    }
    else {
        New-DilockEtcdProvider -Endpoint $Endpoint -Namespace 'default' -Epoch $Epoch
    }
    try {
        $resource = 'powershell/module-consumer/' + [Guid]::NewGuid().ToString('N')
        $ownerHandle = Get-DilockLock -Provider $provider -Resource $resource
        $waiterHandle = Get-DilockLock -Provider $provider -Resource $resource
        $owner = Enter-Dilock -Lock $ownerHandle
        try {
            if ($owner.State -ne [Dilock.LeaseState]::Held) { throw 'Owner did not report Held.' }

            $waitPolicy = [Dilock.AcquireOptions]::new()
            $waitPolicy.WaitTimeout = [TimeSpan]::FromSeconds(10)
            $cts = [System.Threading.CancellationTokenSource]::new()
            try {
                $waiting = $waiterHandle.AcquireAsync($waitPolicy, $cts.Token)
                Start-Sleep -Milliseconds 350
                $cts.Cancel()
                $cancelledWait = $false
                try {
                    $unexpected = $waiting.GetAwaiter().GetResult()
                    if ($null -ne $unexpected) { Close-DilockLease -Lease $unexpected }
                }
                catch [System.OperationCanceledException] { $cancelledWait = $true }
                if (-not $cancelledWait) { throw 'Queued cancellation was not reported.' }
                if ($owner.State -ne [Dilock.LeaseState]::Held) { throw 'Queued cancellation changed the held owner state.' }
            }
            finally { $cts.Dispose() }

            $firstRelease = Exit-Dilock -Lease $owner
            $secondRelease = Exit-Dilock -Lease $owner
            if ($firstRelease -ne [Dilock.ReleaseOutcome]::Released -or $secondRelease -ne $firstRelease) {
                throw "Unexpected repeated release outcome: $firstRelease then $secondRelease"
            }
        }
        finally { Close-DilockLease -Lease $owner }

        $successor = Enter-Dilock -Lock $waiterHandle
        try {
            if ($successor.State -ne [Dilock.LeaseState]::Held) { throw 'Successor did not report Held.' }
            if ($Backend -eq 'Etcd' -and $successor.CreateRevision -le 0) { throw 'etcd successor did not expose a positive create_revision.' }
            $outcome = Exit-Dilock -Lease $successor
            if ($outcome -ne [Dilock.ReleaseOutcome]::Released) { throw "Successor release not acknowledged: $outcome" }
        }
        finally { Close-DilockLease -Lease $successor }

        $scopeResource = $resource + '/scope'
        $scopeHandle = Get-DilockLock -Provider $provider -Resource $scopeResource
        $scopeResult = Invoke-Dilock -Lock $scopeHandle -ScriptBlock {
            param($lease)
            if ($lease.State -ne [Dilock.LeaseState]::Held) { throw 'Scoped lease did not report Held.' }
            'scope-body-ran'
        }
        if ($scopeResult -notcontains 'scope-body-ran' -or $scopeResult -notcontains [Dilock.ReleaseOutcome]::Released) {
            throw "Invoke-Dilock did not run the body and return its release result: $scopeResult"
        }

    Write-Output "PASS ${Backend}: pre-cancelled connect, exports, held owner, queued cancellation, repeated release, successor handoff, scoped release"
    if ($Backend -eq 'Etcd') { Write-Output "PASS etcd create_revision=$($successor.CreateRevision)" }
}
finally { Close-DilockProvider -Provider $provider }
Remove-Module -Name Dilock -ErrorAction Stop
Write-Output 'PASS module resolver cleanup on Remove-Module'
