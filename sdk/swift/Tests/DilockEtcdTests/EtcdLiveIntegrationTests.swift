import DilockCore
import Foundation
import Testing
@testable import DilockEtcd

private actor EtcdAcquisitionSignal {
    private(set) var completed = false
    func markCompleted() { completed = true }
}

struct EtcdLiveIntegrationTests {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_ENDPOINT"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_NAMESPACE"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_EPOCH"] != nil))
    func independentProvidersQueueHandoffAndAdvanceFence() async throws {
        let environment = ProcessInfo.processInfo.environment
        let endpoint = try #require(environment["DILOCK_TEST_ETCD_ENDPOINT"])
        let namespace = try #require(environment["DILOCK_TEST_ETCD_NAMESPACE"])
        let epoch = try #require(environment["DILOCK_TEST_ETCD_EPOCH"])
        let url = try #require(URL(string: endpoint))
        let options = try EtcdProviderOptions(
            endpoint: url,
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(6))
        )
        let first = try await EtcdProvider(options: options)
        let second = try await EtcdProvider(options: options)
        defer { Task { await first.close(); await second.close() } }
        let resource = "swift-etcd-live-\(UUID().uuidString.lowercased())"
        let firstLock = try await first.lock(resource)
        let secondLock = try await second.lock(resource)

        let scopedResource = "swift-etcd-with-lease-\(UUID().uuidString.lowercased())"
        let scopedLock = try await first.lock(scopedResource)
        let scopedResult = try await scopedLock.withLease { lease in
            #expect(await lease.state == .held)
            #expect(await lease.fence != nil)
            return "scope completed"
        }
        #expect(scopedResult == "scope completed")
        let successorAfterScope = try await second.lock(scopedResource).acquire()
        #expect(await successorAfterScope.release() == .released)

        let firstLease = try await firstLock.acquire()
        let firstFence = try #require(await firstLease.fence)
        let signal = EtcdAcquisitionSignal()

        let queued = Task {
            let lease = try await secondLock.acquire()
            await signal.markCompleted()
            return lease
        }

        try await Task.sleep(for: .milliseconds(250))
        #expect(await signal.completed == false)
        #expect(await firstLease.release() == .released)

        let secondLease = try await queued.value
        let secondFence = try #require(await secondLease.fence)
        #expect(firstFence.epoch == epoch)
        #expect(secondFence.epoch == epoch)
        let firstRevision = try #require(UInt64(firstFence.createRevision))
        let secondRevision = try #require(UInt64(secondFence.createRevision))
        #expect(secondRevision > firstRevision)
        #expect(await secondLease.release() == .released)

        await first.close()
        await second.close()
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_ENDPOINT"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_NAMESPACE"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_EPOCH"] != nil))
    func nativeLockQueueMayWaitLongerThanRpcTimeout() async throws {
        let environment = ProcessInfo.processInfo.environment
        let endpoint = try #require(environment["DILOCK_TEST_ETCD_ENDPOINT"])
        let namespace = try #require(environment["DILOCK_TEST_ETCD_NAMESPACE"])
        let epoch = try #require(environment["DILOCK_TEST_ETCD_EPOCH"])
        let options = try EtcdProviderOptions(
            endpoint: try #require(URL(string: endpoint)),
            namespace: namespace,
            epoch: epoch,
            rpcTimeout: .seconds(2),
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(6))
        )
        let first = try await EtcdProvider(options: options)
        let second = try await EtcdProvider(options: options)
        defer { Task { await first.close(); await second.close() } }
        let resource = "swift-etcd-lock-budget-\(UUID().uuidString.lowercased())"
        let firstLease = try await first.lock(resource).acquire()
        let signal = EtcdAcquisitionSignal()
        let queued = Task {
            let lease = try await second.lock(resource).acquire()
            await signal.markCompleted()
            return lease
        }

        // etcd's native Lock queues until ownership is available. Keep the
        // owner beyond rpcTimeout while remaining inside the five-second wait.
        try await Task.sleep(for: .milliseconds(2_200))
        #expect(await signal.completed == false)
        #expect(await firstLease.release() == .released)
        let secondLease = try await queued.value
        #expect(await secondLease.release() == .released)

        await first.close()
        await second.close()
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_ENDPOINT"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_NAMESPACE"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_EPOCH"] != nil))
    func concurrentReleaseReturnsSharedLiveEtcdOutcome() async throws {
        let environment = ProcessInfo.processInfo.environment
        let endpoint = try #require(environment["DILOCK_TEST_ETCD_ENDPOINT"])
        let namespace = try #require(environment["DILOCK_TEST_ETCD_NAMESPACE"])
        let epoch = try #require(environment["DILOCK_TEST_ETCD_EPOCH"])
        let options = try EtcdProviderOptions(
            endpoint: try #require(URL(string: endpoint)),
            namespace: namespace,
            epoch: epoch,
            defaults: try AcquireOptions(wait: .seconds(2), lease: .seconds(6))
        )
        let provider = try await EtcdProvider(options: options)
        defer { Task { await provider.close() } }
        let lease = try await provider.lock("swift-etcd-concurrent-release-\(UUID().uuidString.lowercased())").acquire()

        let outcomes = await withTaskGroup(of: ReleaseOutcome.self) { group in
            for _ in 0..<24 { group.addTask { await lease.release() } }
            var results: [ReleaseOutcome] = []
            for await result in group { results.append(result) }
            return results
        }
        #expect(outcomes.count == 24)
        #expect(outcomes.allSatisfy { $0 == .released })
        await provider.close()
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_ENDPOINT"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_NAMESPACE"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_ETCD_EPOCH"] != nil))
    func liveEtcdNotFoundRevokeIsNotReportedAsPositiveAcknowledgement() async throws {
        let environment = ProcessInfo.processInfo.environment
        let endpoint = try #require(environment["DILOCK_TEST_ETCD_ENDPOINT"])
        let namespace = try #require(environment["DILOCK_TEST_ETCD_NAMESPACE"])
        let epoch = try #require(environment["DILOCK_TEST_ETCD_EPOCH"])
        let provider = try await EtcdProvider(options: EtcdProviderOptions(
            endpoint: try #require(URL(string: endpoint)),
            namespace: namespace,
            epoch: epoch
        ))
        defer { Task { await provider.close() } }

        #expect(await provider.revokeObservation(
            leaseID: Int64.max,
            cleanup: .seconds(2)
        ) == .notFound)
        await provider.close()
    }
}
