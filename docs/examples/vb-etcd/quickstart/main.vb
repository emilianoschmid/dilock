Imports System
Imports System.Threading
Imports System.Threading.Tasks
Imports Dilock
Imports Dilock.Etcd

Module MainModule
    Private Async Function WithAsyncDisposeAsync(resource As IAsyncDisposable, body As Func(Of Task)) As Task
        Dim bodyFailure As Exception = Nothing
        Try
            Await body()
        Catch ex As Exception
            bodyFailure = ex
        End Try

        Dim cleanupFailure As Exception = Nothing
        Try
            Await resource.DisposeAsync()
        Catch ex As Exception
            cleanupFailure = ex
        End Try

        If bodyFailure IsNot Nothing Then
            If cleanupFailure IsNot Nothing Then bodyFailure.Data("Dilock.CleanupFailure") = cleanupFailure
            Throw bodyFailure
        End If
        If cleanupFailure IsNot Nothing Then Throw cleanupFailure
    End Function

    Public Sub Main()
        MainAsync().GetAwaiter().GetResult()
    End Sub

    Private Async Function MainAsync() As Task
        Dim endpoint = If(Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT"), "http://127.0.0.1:2379")
        Dim epoch = Environment.GetEnvironmentVariable("DILOCK_COORDINATION_EPOCH")
        If String.IsNullOrWhiteSpace(epoch) Then Throw New InvalidOperationException("Set DILOCK_COORDINATION_EPOCH to the provisioned epoch.")

        Dim options = New EtcdProviderOptions With {.Endpoint = endpoint, .ExpectedEpoch = epoch}
        Dim provider = Await EtcdProvider.ConnectAsync(options, CancellationToken.None)
        Await WithAsyncDisposeAsync(provider, Async Function()
            Dim lease = Await provider.Lock("reports/daily").AcquireAsync(CancellationToken.None)
            Await WithAsyncDisposeAsync(lease, Async Function()
                If lease.State <> LeaseState.Held Then Throw New InvalidOperationException("Lease is no longer locally held.")
                Dim etcdLease = DirectCast(lease, IEtcdLease)
                Await RebuildDailyReportAsync(etcdLease.Epoch, etcdLease.CreateRevision)
                Dim outcome = Await lease.ReleaseAsync()
                If outcome <> ReleaseOutcome.Released Then Throw New InvalidOperationException($"etcd release was not acknowledged: {outcome}")
            End Function)
            Dim policy = New AcquireOptions With {
                .WaitTimeout = TimeSpan.FromSeconds(12),
                .Lease = New LeaseOptions With {.LeaseDuration = TimeSpan.FromSeconds(45)}
            }
            Dim monthly = Await provider.Lock("reports/monthly").AcquireAsync(policy, CancellationToken.None)
            Await WithAsyncDisposeAsync(monthly, Async Function()
                If monthly.State <> LeaseState.Held Then Throw New InvalidOperationException("Monthly lease is no longer locally held.")
                Dim monthlyEtcdLease = DirectCast(monthly, IEtcdLease)
                Await RebuildDailyReportAsync(monthlyEtcdLease.Epoch, monthlyEtcdLease.CreateRevision)
                Dim outcome = Await monthly.ReleaseAsync()
                If outcome <> ReleaseOutcome.Released Then Throw New InvalidOperationException($"etcd monthly release was not acknowledged: {outcome}")
            End Function)
        End Function)
    End Function

    Private Function RebuildDailyReportAsync(epoch As String, revision As Long) As Task
        ' Atomically reject stale epoch/revision fences at the protected resource.
        Console.WriteLine($"Applying work under fence {epoch}/{revision}")
        Return Task.CompletedTask
    End Function
End Module
