open System
open System.Threading
open System.Threading.Tasks
open Dilock
open Dilock.Etcd

let withAsyncDispose (resource: IAsyncDisposable) (body: unit -> Task) = task {
    let mutable bodyFailure: exn option = None
    try do! body() with error -> bodyFailure <- Some error
    let mutable cleanupFailure: exn option = None
    try do! resource.DisposeAsync().AsTask() with error -> cleanupFailure <- Some error
    match bodyFailure, cleanupFailure with
    | Some error, Some cleanup -> error.Data["Dilock.CleanupFailure"] <- cleanup; raise error
    | Some error, None -> raise error
    | None, Some cleanup -> raise cleanup
    | None, None -> return ()
}

let rebuildDailyReport epoch revision = task {
    // Atomically reject stale epoch/revision fences at the protected resource.
    printfn "Applying work under fence %s/%d" epoch revision
}

let run () = task {
    let endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")
                   |> Option.ofObj |> Option.defaultValue "http://127.0.0.1:2379"
    let epoch = Environment.GetEnvironmentVariable("DILOCK_COORDINATION_EPOCH")
    if String.IsNullOrWhiteSpace(epoch) then invalidOp "Set DILOCK_COORDINATION_EPOCH to the provisioned epoch."

    let options = EtcdProviderOptions(Endpoint = endpoint, ExpectedEpoch = epoch)
    let! provider = EtcdProvider.ConnectAsync(options, CancellationToken.None)
    do! withAsyncDispose provider (fun () -> task {
        let! lease = provider.Lock("reports/daily").AcquireAsync(CancellationToken.None)
        do! withAsyncDispose lease (fun () -> task {
            if lease.State <> LeaseState.Held then invalidOp "Lease is no longer locally held."
            let etcdLease = lease :?> IEtcdLease
            do! rebuildDailyReport etcdLease.Epoch etcdLease.CreateRevision
            let! outcome = lease.ReleaseAsync()
            if outcome <> ReleaseOutcome.Released then invalidOp $"etcd release was not acknowledged: {outcome}"
        })
        let policy = AcquireOptions(WaitTimeout = TimeSpan.FromSeconds 12.0,
                                    Lease = LeaseOptions(LeaseDuration = TimeSpan.FromSeconds 45.0))
        let! monthly = provider.Lock("reports/monthly").AcquireAsync(policy, CancellationToken.None)
        do! withAsyncDispose monthly (fun () -> task {
            if monthly.State <> LeaseState.Held then invalidOp "Monthly lease is no longer locally held."
            let monthlyEtcdLease = monthly :?> IEtcdLease
            do! rebuildDailyReport monthlyEtcdLease.Epoch monthlyEtcdLease.CreateRevision
            let! outcome = monthly.ReleaseAsync()
            if outcome <> ReleaseOutcome.Released then invalidOp $"etcd monthly release was not acknowledged: {outcome}"
        })
    })
}

run().GetAwaiter().GetResult()
