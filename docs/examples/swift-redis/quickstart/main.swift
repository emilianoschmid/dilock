import DilockCore
import DilockRedis
import Foundation
import NIOPosix
import RediStack

let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
do {
    let provider = try await RedisProvider.connect(
        configuration: .init(url: ProcessInfo.processInfo.environment["DILOCK_REDIS_URL"] ?? "redis://127.0.0.1:6379"),
        on: group,
        epoch: ProcessInfo.processInfo.environment["DILOCK_COORDINATION_EPOCH"] ?? "local-redis-epoch"
    )
    do {
        // Separate handles reuse one provider and its shared Redis connections.
        try await provider.lock("reports/daily").withLease { _ in
            print("Default: run one short, idempotent report operation here")
        }
        let explicit = try AcquireOptions(wait: .seconds(3), lease: .seconds(20))
        try await provider.lock("reports/weekly").withLease(explicit) { _ in
            print("Configured: run one short, idempotent report operation here")
        }
    } catch {
        await provider.close()
        throw error
    }
    await provider.close()
    try await group.shutdownGracefully()
} catch {
    try? await group.shutdownGracefully()
    throw error
}
