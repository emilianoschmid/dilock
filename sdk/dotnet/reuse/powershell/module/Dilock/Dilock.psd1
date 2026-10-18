@{
    RootModule        = 'Dilock.psm1'
    ModuleVersion     = '0.1.0'
    GUID              = '60fe6f98-b314-4729-9f3f-7be4ea53a8a9'
    Author            = 'Dilock contributors'
    Description       = 'PowerShell functions over the Dilock .NET Redis, etcd, Consul and PostgreSQL SDKs.'
    PowerShellVersion = '7.4'
    FunctionsToExport = @(
        'New-DilockRedisProvider',
        'New-DilockEtcdProvider',
        'New-DilockConsulProvider',
        'New-DilockPostgresProvider',
        'Get-DilockLock',
        'Enter-Dilock',
        'Exit-Dilock',
        'Close-DilockLease',
        'Close-DilockProvider',
        'Invoke-Dilock'
    )
    CmdletsToExport   = @()
    VariablesToExport = @()
    AliasesToExport   = @()
    RequiredAssemblies = @()
    PrivateData       = @{
        PSData = @{
            Tags         = @('distributed-lock', 'redis', 'etcd', 'consul', 'postgresql')
            LicenseUri   = 'https://www.apache.org/licenses/LICENSE-2.0'
            ReleaseNotes = 'Local experimental module candidate. No Gallery publication is implied.'
        }
    }
}
