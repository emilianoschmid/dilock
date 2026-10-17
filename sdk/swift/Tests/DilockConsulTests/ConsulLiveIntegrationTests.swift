import DilockCore
import Foundation
import Testing
import DilockConsul

struct ConsulLiveIntegrationTests {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_TEST_CONSUL_ENDPOINT"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_CONSUL_NAMESPACE"] != nil
        && ProcessInfo.processInfo.environment["DILOCK_TEST_CONSUL_EPOCH"] != nil))
    func acquireContentionAndAdvanceLockIndex() async throws {
        let env = ProcessInfo.processInfo.environment
        let endpointValue = try #require(env["DILOCK_TEST_CONSUL_ENDPOINT"])
        let endpoint = try #require(URL(string: endpointValue))
        let options = try ConsulProviderOptions(
            endpoint: endpoint,
            namespace: try #require(env["DILOCK_TEST_CONSUL_NAMESPACE"]),
            epoch: try #require(env["DILOCK_TEST_CONSUL_EPOCH"]),
            token: env["DILOCK_TEST_CONSUL_TOKEN"],
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(12))
        )
        let first = try await ConsulProvider(options: options)
        let second = try await ConsulProvider(options: options)
        defer { Task { await first.close(); await second.close() } }
        let name = "swift-consul-live-\(UUID().uuidString.lowercased())"
        let firstLock = try await first.lock(name)
        let secondLock = try await second.lock(name)
        let lease = try await firstLock.acquire()
        let fence = try #require(await lease.fence)
        #expect(fence.epoch == options.epoch)
        #expect(UInt64(fence.lockIndex) != nil)
        let contended = try await secondLock.tryAcquire()
        #expect(contended == nil)
        #expect(await lease.release() == .released)
        let successor = try await secondLock.acquire()
        let successorFence = try #require(await successor.fence)
        let oldIndex = try #require(UInt64(fence.lockIndex))
        let newIndex = try #require(UInt64(successorFence.lockIndex))
        #expect(newIndex > oldIndex)
        #expect(await successor.release() == .released)
        await first.close()
        await second.close()
    }
}
