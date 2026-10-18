open System
open System.IO
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open System.Runtime.ExceptionServices
open Dilock
open Dilock.Etcd
open Dilock.Consul
open Dilock.Redis
open Dilock.Postgres

type ConfiguredBackend =
    | ConfiguredRedis of string
    | ConfiguredEtcd of string
    | ConfiguredConsul of string
    | ConfiguredPostgres of string

let requiredEnvironment name =
    match Environment.GetEnvironmentVariable(name) with
    | value when String.IsNullOrWhiteSpace(value) -> None
    | value -> Some value

let resolveBackend () =
    let redisEndpoint = requiredEnvironment "DILOCK_REDIS_URL"
    let etcdEndpoint = requiredEnvironment "DILOCK_ETCD_ENDPOINT"
    let consulEndpoint = requiredEnvironment "DILOCK_CONSUL_ENDPOINT"
    let postgresEndpoint = requiredEnvironment "DILOCK_POSTGRES_CONNECTION_STRING"
    let requested =
        requiredEnvironment "DILOCK_BACKEND"
        |> Option.map (fun value -> value.Trim().ToLowerInvariant())

    match requested, redisEndpoint, etcdEndpoint, consulEndpoint, postgresEndpoint with
    | Some "redis", Some endpoint, _, _, _ -> Some (ConfiguredRedis endpoint)
    | Some "redis", None, _, _, _ -> invalidOp "DILOCK_BACKEND=redis requires DILOCK_REDIS_URL; no other coordinator will be selected."
    | Some "etcd", _, Some endpoint, _, _ -> Some (ConfiguredEtcd endpoint)
    | Some "etcd", _, None, _, _ -> invalidOp "DILOCK_BACKEND=etcd requires DILOCK_ETCD_ENDPOINT; no other coordinator will be selected."
    | Some "consul", _, _, _, _ ->
        Some (consulEndpoint |> Option.map ConfiguredConsul
                              |> Option.defaultWith (fun () -> invalidOp "DILOCK_BACKEND=consul requires DILOCK_CONSUL_ENDPOINT; no other coordinator will be selected."))
    | Some "postgres", _, _, _, Some endpoint -> Some (ConfiguredPostgres endpoint)
    | Some "postgres", _, _, _, None -> invalidOp "DILOCK_BACKEND=postgres requires DILOCK_POSTGRES_CONNECTION_STRING; no other coordinator will be selected."
    | Some backend, _, _, _, _ -> invalidOp (sprintf "Unsupported DILOCK_BACKEND '%s'; choose redis, etcd, consul or postgres." backend)
    | None, Some endpoint, None, None, None -> Some (ConfiguredRedis endpoint)
    | None, None, Some endpoint, None, None -> Some (ConfiguredEtcd endpoint)
    | None, None, None, Some endpoint, None -> Some (ConfiguredConsul endpoint)
    | None, None, None, None, Some endpoint -> Some (ConfiguredPostgres endpoint)
    | None, None, None, None, None -> None
    | None, _, _, _, _ -> invalidOp "Set DILOCK_BACKEND when multiple coordinator endpoints are configured."

let observeCleanup (observation: CleanupObservation) =
    if observation.Outcome = ReleaseOutcome.Unknown then
        eprintfn "Dilock cleanup is unknown (%s, %s); reconcile the protected resource before assuming cleanup."
            observation.Backend observation.Reason

let require condition message =
    if not condition then invalidOp message

/// Dispose an asynchronous resource while preserving a body or cancellation
/// exception if cleanup also fails. The cleanup error remains available in
/// Exception.Data under Dilock.CleanupFailure.
let withAsyncDispose (resource: IAsyncDisposable) (body: unit -> Task<'T>) = task {
    let! bodyResult = task {
        try
            let! value = body()
            return Choice1Of2 value
        with error ->
            return Choice2Of2 error
    }
    let mutable cleanupFailure: exn option = None
    try
        do! resource.DisposeAsync().AsTask()
    with error ->
        cleanupFailure <- Some error

    match bodyResult, cleanupFailure with
    | Choice2Of2 primary, Some cleanup ->
        try primary.Data["Dilock.CleanupFailure"] <- cleanup with _ -> ()
        ExceptionDispatchInfo.Capture(primary).Throw()
        return Unchecked.defaultof<'T>
    | Choice2Of2 primary, None ->
        ExceptionDispatchInfo.Capture(primary).Throw()
        return Unchecked.defaultof<'T>
    | Choice1Of2 _, Some cleanup -> return raise cleanup
    | Choice1Of2 value, None -> return value
}

let verifyDisposeExceptionPrecedence () = task {
    let primary = InvalidOperationException("body failed")
    let cleanup = IOException("cleanup failed")
    let resource =
        { new IAsyncDisposable with
            member _.DisposeAsync() = ValueTask(Task.FromException(cleanup)) }
    let mutable observed: exn option = None
    try
        let! _ = withAsyncDispose resource (fun () -> Task.FromException<int>(primary))
        ()
    with error -> observed <- Some error
    match observed with
    | Some error ->
        require (Object.ReferenceEquals(error, primary)) "F# cleanup replaced the body exception."
        require (Object.ReferenceEquals(error.Data["Dilock.CleanupFailure"], cleanup))
            "F# cleanup failure was not attached to the primary exception."
    | None -> invalidOp "F# scope did not preserve the body exception."
}

let verifyPolicyComposition () =
    let defaults = AcquireOptions()
    require (defaults.WaitTimeout = TimeSpan.FromSeconds 5.0) "F# acquisition wait default changed."
    require (defaults.Lease.LeaseDuration = TimeSpan.FromSeconds 30.0) "F# lease default changed."
    let custom = AcquireOptions(WaitTimeout = TimeSpan.FromSeconds 12.0,
                                Lease = LeaseOptions(LeaseDuration = TimeSpan.FromSeconds 45.0))
    custom.Validate()
    require (custom.WaitTimeout = TimeSpan.FromSeconds 12.0) "F# custom wait policy was not composed."
    require (custom.Lease.LeaseDuration = TimeSpan.FromSeconds 45.0) "F# custom lease policy was not composed."

let verifyPreCancelledConnect () = task {
    use cancellation = new CancellationTokenSource()
    cancellation.Cancel()
    let redisOptions = RedisProviderOptions(Endpoint = "redis://127.0.0.1:1", ExpectedEpoch = "fsharp-test")
    let mutable redisCancelled = false
    try
        let! provider = RedisProvider.ConnectAsync(redisOptions, cancellation.Token)
        do! provider.DisposeAsync().AsTask()
    with :? OperationCanceledException -> redisCancelled <- true
    require redisCancelled "Redis ConnectAsync did not honor cancellation before I/O."

    let etcdOptions = EtcdProviderOptions(Endpoint = "http://127.0.0.1:1", ExpectedEpoch = "fsharp-test")
    let mutable etcdCancelled = false
    try
        let! provider = EtcdProvider.ConnectAsync(etcdOptions, cancellation.Token)
        do! provider.DisposeAsync().AsTask()
    with :? OperationCanceledException -> etcdCancelled <- true
    require etcdCancelled "etcd ConnectAsync did not honor cancellation before I/O."
}

let verifyLeaseLifecycle (provider: ILockProvider) backend expectLeaseDuration cancellationToken = task {
    let resource = "fsharp/semantic-" + Guid.NewGuid().ToString("N")
    // Reusable resource handles share this provider and its backend client/channel.
    let ownerHandle = provider.Lock(resource)
    let waiterHandle = provider.Lock(resource)
    let! owner = ownerHandle.AcquireAsync(cancellationToken)
    do! withAsyncDispose owner (fun () -> task {
        require (owner.State = LeaseState.Held) (backend + " owner did not enter Held state.")
        if expectLeaseDuration then
            require (owner.EffectiveLeaseMilliseconds = 30_000L) (backend + " default lease duration changed.")

        use cancellation = new CancellationTokenSource()
        let policy = AcquireOptions(WaitTimeout = TimeSpan.FromSeconds 8.0,
                                    Lease = LeaseOptions(LeaseDuration = TimeSpan.FromSeconds 30.0))
        let waiting = waiterHandle.AcquireAsync(policy, cancellation.Token)
        do! Task.Delay(TimeSpan.FromMilliseconds 400.0)
        cancellation.Cancel()
        let mutable wasCancelled = false
        try
            let! unexpected = waiting
            do! unexpected.DisposeAsync().AsTask()
        with :? OperationCanceledException -> wasCancelled <- true
        require wasCancelled (backend + " queued acquisition did not report cancellation.")
        require (owner.State = LeaseState.Held) (backend + " contender cancellation affected the owner lease.")

        let! released = owner.ReleaseAsync()
        require (released = ReleaseOutcome.Released) (backend + " owner release was not acknowledged.")
        let! repeated = owner.ReleaseAsync()
        require (repeated = released) (backend + " repeated release changed its outcome.")
    })

    let! successor = waiterHandle.AcquireAsync(cancellationToken)
    do! withAsyncDispose successor (fun () -> task {
        require (successor.State = LeaseState.Held) (backend + " successor did not acquire after release.")
        let! outcome = successor.ReleaseAsync()
        require (outcome = ReleaseOutcome.Released) (backend + " successor release was not acknowledged.")
    })
}

let runExample endpoint epoch cancellationToken = task {
    // Easy path: default namespace, five-second wait, and thirty-second lease.
    let options = RedisProviderOptions(Endpoint = endpoint, ExpectedEpoch = epoch, CleanupObserver = observeCleanup)
    let! provider = RedisProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> verifyLeaseLifecycle provider "Redis" true cancellationToken)
}

let runEtcd endpoint namespaceName epoch cancellationToken = task {
    let options = EtcdProviderOptions(Endpoint = endpoint, Namespace = namespaceName,
                                      ExpectedEpoch = epoch, CleanupObserver = observeCleanup)
    let! provider = EtcdProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> verifyLeaseLifecycle provider "etcd" true cancellationToken)
}

let runConsul endpoint namespaceName epoch cancellationToken = task {
    let options = ConsulProviderOptions(Endpoint = endpoint, Namespace = namespaceName,
                                        ExpectedEpoch = epoch,
                                        Token = Environment.GetEnvironmentVariable("CONSUL_HTTP_TOKEN"),
                                        CleanupObserver = observeCleanup)
    let! provider = ConsulProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> verifyLeaseLifecycle provider "Consul" true cancellationToken)
}

let runPostgres endpoint namespaceName epoch cancellationToken = task {
    let options = PostgresProviderOptions(ConnectionString = endpoint, Namespace = namespaceName,
                                          ExpectedEpoch = epoch, CleanupObserver = observeCleanup)
    let! provider = PostgresProvider.ConnectAsync(options, cancellationToken)
    do! withAsyncDispose provider (fun () -> verifyLeaseLifecycle provider "PostgreSQL" false cancellationToken)
}

let identity = ResourceIdentity.Hash("default", "fsharp/consumer")
require (identity.Length = 64) "Dilock identity API is unavailable."
verifyPolicyComposition ()
verifyDisposeExceptionPrecedence().GetAwaiter().GetResult()
verifyPreCancelledConnect().GetAwaiter().GetResult()
printfn "F# policy composition and pre-cancelled connection checks passed."

match resolveBackend (), requiredEnvironment "DILOCK_NAMESPACE", requiredEnvironment "DILOCK_EPOCH" with
| Some _, _, None ->
    invalidOp "A selected Dilock backend requires DILOCK_EPOCH; no coordinator connection was attempted."
| Some (ConfiguredEtcd endpoint), namespaceName, Some epoch ->
    runEtcd endpoint (defaultArg namespaceName "default") epoch CancellationToken.None
    |> fun work -> work.GetAwaiter().GetResult()
    printfn "F# consumer and etcd operation passed."
| Some (ConfiguredConsul endpoint), namespaceName, Some epoch ->
    runConsul endpoint (defaultArg namespaceName "default") epoch CancellationToken.None
    |> fun work -> work.GetAwaiter().GetResult()
    printfn "F# consumer and Consul operation passed."
| Some (ConfiguredPostgres endpoint), namespaceName, Some epoch ->
    runPostgres endpoint (defaultArg namespaceName "default") epoch CancellationToken.None
    |> fun work -> work.GetAwaiter().GetResult()
    printfn "F# consumer and PostgreSQL operation passed."
| Some (ConfiguredRedis endpoint), _, Some epoch ->
    runExample endpoint epoch CancellationToken.None |> fun work -> work.GetAwaiter().GetResult()
    printfn "F# consumer and Redis operation passed."
| _ ->
    printfn "F# consumer API passed; no live backend run (configure DILOCK_BACKEND, its endpoint and DILOCK_EPOCH)."
