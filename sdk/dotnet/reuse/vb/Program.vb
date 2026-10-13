Imports System
Imports System.IO
Imports System.Threading
Imports System.Threading.Tasks
Imports System.Runtime.ExceptionServices
Imports Dilock
Imports Dilock.Etcd
Imports Dilock.Redis

Module Program
    Private NotInheritable Class FaultingAsyncDisposable
        Implements IAsyncDisposable

        Private ReadOnly failure As Exception

        Public Sub New(failure As Exception)
            Me.failure = failure
        End Sub

        Public Function DisposeAsync() As ValueTask Implements IAsyncDisposable.DisposeAsync
            Return New ValueTask(Task.FromException(failure))
        End Function
    End Class

    ' Preserve an application or cancellation exception if async cleanup also
    ' fails; retain the cleanup exception for diagnostics in Exception.Data.
    Private Async Function WithAsyncDisposeAsync(resource As IAsyncDisposable,
                                                 body As Func(Of Task)) As Task
        Dim bodyFailure As Exception = Nothing
        Try
            Await body()
        Catch ex As Exception
            bodyFailure = ex
        End Try

        Try
            Await resource.DisposeAsync()
        Catch cleanupFailure As Exception
            If bodyFailure Is Nothing Then Throw
            Try
                bodyFailure.Data("Dilock.CleanupFailure") = cleanupFailure
            Catch
                ' Diagnostics must not replace the primary failure.
            End Try
        End Try

        If bodyFailure IsNot Nothing Then ExceptionDispatchInfo.Capture(bodyFailure).Throw()
    End Function

    Private Async Function VerifyDisposeExceptionPrecedenceAsync() As Task
        Dim primary = New InvalidOperationException("body failed")
        Dim cleanup = New IOException("cleanup failed")
        Dim observed As Exception = Nothing
        Try
            Await WithAsyncDisposeAsync(New FaultingAsyncDisposable(cleanup),
                Function() Task.FromException(primary))
        Catch ex As Exception
            observed = ex
        End Try
        Require(Object.ReferenceEquals(observed, primary), "VB cleanup replaced the body exception.")
        Require(Object.ReferenceEquals(observed.Data("Dilock.CleanupFailure"), cleanup),
                "VB cleanup failure was not attached to the primary exception.")
    End Function

    Private Sub ObserveCleanup(observation As CleanupObservation)
        If observation.Outcome = ReleaseOutcome.Unknown Then
            Console.Error.WriteLine(
                "Dilock cleanup is unknown ({0}, {1}); reconcile the protected resource before assuming cleanup.",
                observation.Backend,
                observation.Reason)
        End If
    End Sub

    Public Sub Main()
        MainAsync().GetAwaiter().GetResult()
    End Sub

    Private Async Function MainAsync() As Task
        Dim identity = ResourceIdentity.Hash("default", "vb/consumer")
        If identity.Length <> 64 Then Throw New InvalidOperationException("Dilock identity API is unavailable.")
        VerifyPolicyComposition()
        Await VerifyDisposeExceptionPrecedenceAsync()
        Await VerifyPreCancelledConnectAsync()
        Console.WriteLine("Visual Basic policy composition and pre-cancelled connection checks passed.")

        Dim endpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_URL")
        Dim epoch = Environment.GetEnvironmentVariable("DILOCK_EPOCH")
        Dim etcdEndpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")
        Dim namespaceName = Environment.GetEnvironmentVariable("DILOCK_NAMESPACE")
        Dim selectedBackend = ResolveConfiguredBackend(endpoint, etcdEndpoint)
        If selectedBackend <> "none" AndAlso String.IsNullOrWhiteSpace(epoch) Then
            Throw New InvalidOperationException(
                "A selected Dilock backend requires DILOCK_EPOCH; no coordinator connection was attempted.")
        End If

        If selectedBackend = "etcd" AndAlso Not String.IsNullOrWhiteSpace(epoch) Then
            If String.IsNullOrWhiteSpace(namespaceName) Then namespaceName = "default"
            Await RunEtcdAsync(etcdEndpoint, namespaceName, epoch)
            Console.WriteLine("Visual Basic consumer and etcd operation passed.")
            Return
        End If

        If selectedBackend = "none" OrElse String.IsNullOrWhiteSpace(epoch) Then
            Console.WriteLine("Visual Basic consumer API passed; live Redis/etcd NOT_RUN (configure one backend endpoint and DILOCK_EPOCH).")
            Return
        End If

        ' Easy path: default namespace, five-second wait, and thirty-second lease.
        Dim options = New RedisProviderOptions With {
            .Endpoint = endpoint,
            .ExpectedEpoch = epoch,
            .CleanupObserver = AddressOf ObserveCleanup
        }
        Dim provider = Await RedisProvider.ConnectAsync(options, CancellationToken.None)
        Await WithAsyncDisposeAsync(provider, Async Function()
            Await VerifyLeaseLifecycleAsync(provider, "Redis", CancellationToken.None)
        End Function)
        Console.WriteLine("Visual Basic consumer and Redis operation passed.")
    End Function

    Private Function ResolveConfiguredBackend(redisEndpoint As String,
                                               etcdEndpoint As String) As String
        Dim requested = Environment.GetEnvironmentVariable("DILOCK_BACKEND")
        Dim hasRedis = Not String.IsNullOrWhiteSpace(redisEndpoint)
        Dim hasEtcd = Not String.IsNullOrWhiteSpace(etcdEndpoint)

        If String.IsNullOrWhiteSpace(requested) Then
            If hasRedis AndAlso hasEtcd Then
                Throw New InvalidOperationException(
                    "Both Redis and etcd endpoints are configured; set DILOCK_BACKEND=redis or DILOCK_BACKEND=etcd to select explicitly.")
            End If
            If hasRedis Then Return "redis"
            If hasEtcd Then Return "etcd"
            Return "none"
        End If

        Select Case requested.Trim().ToLowerInvariant()
            Case "redis"
                If Not hasRedis Then
                    Throw New InvalidOperationException(
                        "DILOCK_BACKEND=redis requires DILOCK_REDIS_URL; no other coordinator will be selected.")
                End If
                Return "redis"
            Case "etcd"
                If Not hasEtcd Then
                    Throw New InvalidOperationException(
                        "DILOCK_BACKEND=etcd requires DILOCK_ETCD_ENDPOINT; no other coordinator will be selected.")
                End If
                Return "etcd"
            Case Else
                Throw New InvalidOperationException(
                    "Unsupported DILOCK_BACKEND; choose redis or etcd.")
        End Select
    End Function

    Private Async Function RunEtcdAsync(endpoint As String, namespaceName As String, epoch As String) As Task
        Dim options = New EtcdProviderOptions With {
            .Endpoint = endpoint,
            .Namespace = namespaceName,
            .ExpectedEpoch = epoch,
            .CleanupObserver = AddressOf ObserveCleanup
        }
        Dim provider = Await EtcdProvider.ConnectAsync(options, CancellationToken.None)
        Await WithAsyncDisposeAsync(provider, Async Function()
            Await VerifyLeaseLifecycleAsync(provider, "etcd", CancellationToken.None)
        End Function)
    End Function

    Private Sub VerifyPolicyComposition()
        Dim defaults = New AcquireOptions()
        Require(defaults.WaitTimeout = TimeSpan.FromSeconds(5), "VB acquisition wait default changed.")
        Require(defaults.Lease.LeaseDuration = TimeSpan.FromSeconds(30), "VB lease default changed.")
        Dim custom = New AcquireOptions With {
            .WaitTimeout = TimeSpan.FromSeconds(12),
            .Lease = New LeaseOptions With {.LeaseDuration = TimeSpan.FromSeconds(45)}
        }
        custom.Validate()
        Require(custom.WaitTimeout = TimeSpan.FromSeconds(12), "VB custom wait policy was not composed.")
        Require(custom.Lease.LeaseDuration = TimeSpan.FromSeconds(45), "VB custom lease policy was not composed.")
    End Sub

    Private Async Function VerifyPreCancelledConnectAsync() As Task
        Using cancellation As New CancellationTokenSource()
            cancellation.Cancel()
            Dim redisCancelled = False
            Try
                Dim options = New RedisProviderOptions With {
                    .Endpoint = "redis://127.0.0.1:1",
                    .ExpectedEpoch = "vb-test"
                }
                Dim provider = Await RedisProvider.ConnectAsync(options, cancellation.Token)
                Await provider.DisposeAsync()
            Catch ex As OperationCanceledException
                redisCancelled = True
            End Try
            Require(redisCancelled, "Redis ConnectAsync did not honor cancellation before I/O.")

            Dim etcdCancelled = False
            Try
                Dim options = New EtcdProviderOptions With {
                    .Endpoint = "http://127.0.0.1:1",
                    .ExpectedEpoch = "vb-test"
                }
                Dim provider = Await EtcdProvider.ConnectAsync(options, cancellation.Token)
                Await provider.DisposeAsync()
            Catch ex As OperationCanceledException
                etcdCancelled = True
            End Try
            Require(etcdCancelled, "etcd ConnectAsync did not honor cancellation before I/O.")
        End Using
    End Function

    Private Async Function VerifyLeaseLifecycleAsync(provider As ILockProvider,
                                                       backend As String,
                                                       cancellationToken As CancellationToken) As Task
        Dim resource = "vb/semantic-" & Guid.NewGuid().ToString("N")
        ' Reusable resource handles share this provider and its backend client/channel.
        Dim ownerHandle = provider.Lock(resource)
        Dim waiterHandle = provider.Lock(resource)
        Dim owner = Await ownerHandle.AcquireAsync(cancellationToken)
        Await WithAsyncDisposeAsync(owner, Async Function()
            Require(owner.State = LeaseState.Held, backend & " owner did not enter Held state.")
            Require(owner.EffectiveLeaseMilliseconds = 30000L, backend & " default lease duration changed.")

            Using cancellation As New CancellationTokenSource()
                Dim policy = New AcquireOptions With {
                    .WaitTimeout = TimeSpan.FromSeconds(8),
                    .Lease = New LeaseOptions With {.LeaseDuration = TimeSpan.FromSeconds(30)}
                }
                Dim waiting = waiterHandle.AcquireAsync(policy, cancellation.Token)
                Await Task.Delay(TimeSpan.FromMilliseconds(400))
                cancellation.Cancel()
                Dim wasCancelled = False
                Try
                    Dim unexpected = Await waiting
                    Await unexpected.DisposeAsync()
                Catch ex As OperationCanceledException
                    wasCancelled = True
                End Try
                Require(wasCancelled, backend & " queued acquisition did not report cancellation.")
                Require(owner.State = LeaseState.Held, backend & " contender cancellation affected the owner lease.")
            End Using

            Dim released = Await owner.ReleaseAsync()
            Require(released = ReleaseOutcome.Released, backend & " owner release was not acknowledged.")
            Dim repeated = Await owner.ReleaseAsync()
            Require(repeated = released, backend & " repeated release changed its outcome.")
        End Function)

        Dim successor = Await waiterHandle.AcquireAsync(cancellationToken)
        Await WithAsyncDisposeAsync(successor, Async Function()
            Require(successor.State = LeaseState.Held, backend & " successor did not acquire after release.")
            Dim outcome = Await successor.ReleaseAsync()
            Require(outcome = ReleaseOutcome.Released, backend & " successor release was not acknowledged.")
        End Function)
    End Function

    Private Sub Require(condition As Boolean, message As String)
        If Not condition Then Throw New InvalidOperationException(message)
    End Sub
End Module
