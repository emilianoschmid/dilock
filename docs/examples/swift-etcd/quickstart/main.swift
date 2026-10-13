import DilockCore
import DilockEtcd
import Foundation

let endpoint = URL(string: ProcessInfo.processInfo.environment["DILOCK_ETCD_ENDPOINT"] ?? "http://127.0.0.1:2379")!
let epoch = ProcessInfo.processInfo.environment["DILOCK_COORDINATION_EPOCH"] ?? "local-etcd-epoch"
let options = try EtcdProviderOptions(endpoint: endpoint, epoch: epoch)
let provider = try await EtcdProvider(options: options)
do {
    // Separate handles reuse one provider and its gRPC channel.
    let daily = try await provider.lock("reports/daily")
    try await daily.withLease { lease in
        guard let fence = await lease.fence else { throw DilockError.leaseLost }
        print("Default etcd operation; enforce fence \(fence.epoch)/\(fence.createRevision)")
    }
    let explicit = try AcquireOptions(wait: .seconds(3), lease: .seconds(20))
    let weekly = try await provider.lock("reports/weekly")
    try await weekly.withLease(explicit) { lease in
        guard let fence = await lease.fence else { throw DilockError.leaseLost }
        print("Configured etcd operation; enforce fence \(fence.epoch)/\(fence.createRevision)")
    }
    await provider.close()
} catch {
    await provider.close()
    throw error
}
