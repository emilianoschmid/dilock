import DilockCore
import Foundation
import NIOCore
import NIOPosix
@preconcurrency import RediStack
import Testing
@testable import DilockRedis

// The test transfers this setup-only list to one cleanup task after assertions finish.
private final class RedisLiveCleanup: @unchecked Sendable {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 5)
    var connections: [RedisConnection] = []

    func close() async {
        for connection in connections {
            try? await connection.close().get()
        }
        try? await group.shutdownGracefully()
    }
}

private final class RedisFactoryLiveCleanup: @unchecked Sendable {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 3)
    var admin: RedisConnection?
    var provider: RedisProvider?

    func close() async {
        if let provider { await provider.close() }
        if let admin { try? await admin.close().get() }
        try? await group.shutdownGracefully()
    }
}

struct RedisLiveIntegrationTests {
    // The shortest live lease is 250 ms; keep the RPC cap at 100 ms so it leaves
    // the provider's required 10 ms lease-safety margin.
    private static let liveRPCTimeout = Duration.milliseconds(100)

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"] != nil))
    func connectFactoryOwnsIndependentConnectionsAndLeavesEventLoopGroupWithCaller() async throws {
        let endpoint = try #require(ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"])
        let cleanup = RedisFactoryLiveCleanup()
        let group = cleanup.group
        var cleanupCompleted = false
        defer {
            if !cleanupCompleted { Task { await cleanup.close() } }
        }

        let adminConnection = try await RedisConnection.make(
            configuration: .init(url: endpoint),
            boundEventLoop: group.next()
        ).get()
        cleanup.admin = adminConnection
        let namespace = "swift-connect-\(UUID().uuidString.lowercased())"
        let epoch = "swift-connect-\(UUID().uuidString.lowercased())"
        let metadataKey = "dilock:namespace:\(namespace):meta"
        let metadata = """
        {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"\(epoch)","fixed_lease_ms":null}
        """
        let allocator = ByteBufferAllocator()
        let response = try await adminConnection.send(command: "SET", with: [
            .bulkString(allocator.buffer(string: metadataKey)),
            .bulkString(allocator.buffer(string: metadata)),
            .bulkString(allocator.buffer(string: "PX")),
            .bulkString(allocator.buffer(string: "60000")),
        ]).get()
        #expect(response.string == "OK")

        let connectedProvider = try await RedisProvider.connect(
            configuration: .init(url: endpoint),
            on: group,
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(2)),
            rpcTimeout: Self.liveRPCTimeout
        )
        cleanup.provider = connectedProvider
        let lock = try await connectedProvider.lock("factory-resource")
        let lease = try #require(try await lock.tryAcquire())
        #expect(await lease.release() == .released)

        await connectedProvider.close()
        await connectedProvider.close()
        // The factory closes its own two connections, not the caller's event-loop group.
        let stillUsable = try await RedisConnection.make(
            configuration: .init(url: endpoint),
            boundEventLoop: group.next()
        ).get()
        try await stillUsable.close().get()
        _ = try await adminConnection.send(command: "DEL", with: [
            .bulkString(allocator.buffer(string: metadataKey)),
        ]).get()

        try await adminConnection.close().get()
        try await group.shutdownGracefully()
        cleanup.provider = nil
        cleanup.admin = nil
        cleanupCompleted = true
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"] != nil))
    func independentProvidersContendExpireAndPreserveSuccessor() async throws {
        let endpoint = try #require(ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"])
        let cleanup = RedisLiveCleanup()
        var cleanupCompleted = false
        defer {
            if !cleanupCompleted { Task { await cleanup.close() } }
        }

        func connect() async throws -> RedisConnection {
            let connection = try await RedisConnection.make(
                configuration: .init(url: endpoint),
                boundEventLoop: cleanup.group.next()
            ).get()
            cleanup.connections.append(connection)
            return connection
        }

        let admin = try await connect()
        let firstConnection = try await connect()
        let firstCleanup = try await connect()
        let secondConnection = try await connect()
        let secondCleanup = try await connect()
        let namespace = "swift-live-\(UUID().uuidString.lowercased())"
        let epoch = "swift-live-\(UUID().uuidString.lowercased())"
        let metadataKey = "dilock:namespace:\(namespace):meta"
        let metadata = """
        {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"\(epoch)","fixed_lease_ms":null}
        """
        let allocator = ByteBufferAllocator()
        let response = try await admin.send(
            command: "SET",
            with: [
                .bulkString(allocator.buffer(string: metadataKey)),
                .bulkString(allocator.buffer(string: metadata)),
                .bulkString(allocator.buffer(string: "PX")),
                .bulkString(allocator.buffer(string: "60000")),
            ]
        ).get()
        #expect(response.string == "OK")

        let firstProvider = try await RedisProvider(
            connection: firstConnection,
            cleanupConnection: firstCleanup,
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(2)),
            rpcTimeout: Self.liveRPCTimeout
        )
        let secondProvider = try await RedisProvider(
            connection: secondConnection,
            cleanupConnection: secondCleanup,
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(2)),
            rpcTimeout: Self.liveRPCTimeout
        )
        let firstLock = try await firstProvider.lock("shared-resource")
        let secondLock = try await secondProvider.lock("shared-resource")

        let scopedResource = "with-lease-resource"
        let scopedLock = try await firstProvider.lock(scopedResource)
        let scopedResult = try await scopedLock.withLease { lease in
            #expect(await lease.state == .held)
            return "scope completed"
        }
        #expect(scopedResult == "scope completed")
        let successorAfterScope = try #require(try await scopedLock.tryAcquire())
        #expect(await successorAfterScope.release() == .released)

        let expiredScopeResource = "with-lease-expired-resource"
        let expiredScopeLock = try await firstProvider.lock(expiredScopeResource)
        let shortScope = try AcquireOptions(wait: .seconds(2), lease: .milliseconds(250))
        do {
            _ = try await expiredScopeLock.withLease(shortScope) { _ in
                try await Task.sleep(for: .milliseconds(400))
                return "must not be reported as protected success"
            }
            Issue.record("Expected an expired Redis scoped lease to report leaseLost")
        } catch let error as DilockError {
            #expect(error == .leaseLost)
        } catch {
            Issue.record("Unexpected scoped Redis error: \(error)")
        }
        let successorAfterExpiry = try #require(try await expiredScopeLock.tryAcquire())
        #expect(await successorAfterExpiry.release() == .released)

        let original = try #require(try await firstLock.tryAcquire())
        #expect(try await secondLock.tryAcquire() == nil)
        #expect(await original.release() == .released)

        let handoff = try #require(try await secondLock.tryAcquire())
        #expect(await handoff.release() == .released)

        let staleOptions = try AcquireOptions(wait: .seconds(5), lease: .milliseconds(250))
        let staleOwner = try await firstProvider.lock("expiring-resource").acquire(staleOptions)
        try await Task.sleep(for: .milliseconds(400))
        let successor = try #require(try await secondProvider.lock("expiring-resource").tryAcquire())
        #expect(await staleOwner.release() == .alreadyNotOwned)
        #expect(try await firstProvider.lock("expiring-resource").tryAcquire() == nil)
        #expect(await successor.release() == .released)

        let renewableOptions = try AcquireOptions(wait: .seconds(5), lease: .milliseconds(300))
        let renewable = try await firstProvider.lock("renewable-resource").acquire(renewableOptions)
        try await renewable.renew(for: Duration.seconds(2))
        try await Task.sleep(for: .milliseconds(500))
        #expect(try await secondProvider.lock("renewable-resource").tryAcquire() == nil)
        #expect(await renewable.release() == .released)

        let resourceID = try Identity.resourceID(namespace: namespace, resource: "shared-resource")
        let scopedResourceID = try Identity.resourceID(namespace: namespace, resource: scopedResource)
        let expiredScopeResourceID = try Identity.resourceID(namespace: namespace, resource: expiredScopeResource)
        let lockKey = Identity.redisKey(resourceID: resourceID)
        _ = try await admin.send(command: "DEL", with: [
            .bulkString(allocator.buffer(string: lockKey)),
            .bulkString(allocator.buffer(string: Identity.redisKey(resourceID: scopedResourceID))),
            .bulkString(allocator.buffer(string: Identity.redisKey(resourceID: expiredScopeResourceID))),
            .bulkString(allocator.buffer(string: "dilock:namespace:\(namespace):meta")),
        ]).get()
        await cleanup.close()
        cleanupCompleted = true
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"] != nil))
    func concurrentReleaseSharesOneLiveRedisCleanupCommand() async throws {
        let endpoint = try #require(ProcessInfo.processInfo.environment["DILOCK_TEST_REDIS_ENDPOINT"])
        let cleanup = RedisLiveCleanup()
        var cleanupCompleted = false
        defer {
            if !cleanupCompleted { Task { await cleanup.close() } }
        }

        func connect() async throws -> RedisConnection {
            let connection = try await RedisConnection.make(
                configuration: .init(url: endpoint),
                boundEventLoop: cleanup.group.next()
            ).get()
            cleanup.connections.append(connection)
            return connection
        }

        let admin = try await connect()
        let normal = try await connect()
        let cleanupConnection = try await connect()
        let namespace = "swift-release-\(UUID().uuidString.lowercased())"
        let epoch = "swift-release-\(UUID().uuidString.lowercased())"
        let metadataKey = "dilock:namespace:\(namespace):meta"
        let metadata = """
        {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"\(epoch)","fixed_lease_ms":null}
        """
        let allocator = ByteBufferAllocator()
        let metadataResult = try await admin.send(command: "SET", with: [
            .bulkString(allocator.buffer(string: metadataKey)),
            .bulkString(allocator.buffer(string: metadata)),
            .bulkString(allocator.buffer(string: "PX")),
            .bulkString(allocator.buffer(string: "60000")),
        ]).get()
        #expect(metadataResult.string == "OK")

        let provider = try await RedisProvider(
            connection: normal,
            cleanupConnection: cleanupConnection,
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(2)),
            rpcTimeout: Self.liveRPCTimeout
        )
        let lock = try await provider.lock("concurrent-release")
        let lease = try #require(try await lock.tryAcquire())

        _ = try await admin.send(command: "CONFIG", with: [
            .bulkString(allocator.buffer(string: "RESETSTAT")),
        ]).get()
        _ = try await admin.send(command: "CLIENT", with: [
            .bulkString(allocator.buffer(string: "PAUSE")),
            .bulkString(allocator.buffer(string: "750")),
            .bulkString(allocator.buffer(string: "ALL")),
        ]).get()
        let outcomes = await withTaskGroup(of: ReleaseOutcome.self) { group in
            for _ in 0..<24 { group.addTask { await lease.release() } }
            var results: [ReleaseOutcome] = []
            for await result in group { results.append(result) }
            return results
        }
        #expect(outcomes.count == 24)
        #expect(outcomes.allSatisfy { $0 == .released })

        #expect(await lease.state == .released)

        let identity = try Identity.resourceID(namespace: namespace, resource: "concurrent-release")
        _ = try await admin.send(command: "DEL", with: [
            .bulkString(allocator.buffer(string: Identity.redisKey(resourceID: identity))),
            .bulkString(allocator.buffer(string: metadataKey)),
        ]).get()
        await cleanup.close()
        cleanupCompleted = true
    }
}
