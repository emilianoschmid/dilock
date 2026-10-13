// Local-only Swift concurrency example. No TTL, network, renewal or wire format.
import Foundation

enum ReleaseOutcome: Sendable { case released, alreadyNotOwned }
enum StyleError: Error { case emptyResource, busy, workFailure }

/// This actor serializes the local fixture, not a distributed coordinator.
actor MemoryStore {
    private var owners: [String: UUID] = [:]
    func claim(_ resource: String, owner: UUID) -> Bool {
        guard owners[resource] == nil else { return false }
        owners[resource] = owner
        return true
    }
    func release(_ resource: String, owner: UUID) -> ReleaseOutcome {
        guard owners[resource] == owner else { return .alreadyNotOwned }
        owners.removeValue(forKey: resource)
        return .released
    }
    func expireForTest(_ resource: String) { owners.removeValue(forKey: resource) }
}

/// One acquisition. No async cleanup is attempted from deinit.
actor Lease {
    private let store: MemoryStore
    private let resource: String
    private let owner: UUID
    private var closed = false
    init(store: MemoryStore, resource: String, owner: UUID) {
        self.store = store; self.resource = resource; self.owner = owner
    }
    func release() async -> ReleaseOutcome {
        guard !closed else { return .alreadyNotOwned }
        closed = true // Terminal before await: actor reentrancy cannot double-release.
        return await store.release(resource, owner: owner)
    }
}
struct ResourceLock: Sendable {
    let store: MemoryStore
    let resource: String
    func tryAcquire() async throws -> Lease? {
        try Task.checkCancellation()
        let owner = UUID() // Local fixture identity; not the shared wire token.
        guard await store.claim(resource, owner: owner) else { return nil }
        if Task.isCancelled {
            _ = await store.release(resource, owner: owner)
            throw CancellationError()
        }
        return Lease(store: store, resource: resource, owner: owner)
    }
}
struct Provider: Sendable {
    let store: MemoryStore
    func lock(_ resource: String) throws -> ResourceLock {
        guard !resource.isEmpty else { throw StyleError.emptyResource }
        return ResourceLock(store: store, resource: resource)
    }
}

@main struct Main {
    static func check(_ value: Bool) { precondition(value, "style assertion failed") }
    static func main() async throws {
        let store = MemoryStore()
        let resource = try Provider(store: store).lock("example/resource")
        guard let first = try await resource.tryAcquire() else { throw StyleError.busy }
        check(try await resource.tryAcquire() == nil)
        await store.expireForTest("example/resource")
        guard let second = try await resource.tryAcquire() else { throw StyleError.busy }
        check(await first.release() == .alreadyNotOwned)
        check(try await resource.tryAcquire() == nil)
        check(await second.release() == .released)
        check(await second.release() == .alreadyNotOwned)
        guard let held = try await resource.tryAcquire() else { throw StyleError.busy }
        do {
            throw StyleError.workFailure
        } catch {
            _ = await held.release() // Explicit awaited cleanup, not unstructured Task in defer.
        }
        guard let final = try await resource.tryAcquire() else { throw StyleError.busy }
        _ = await final.release()
        print("PASS swift: busy, stale owner, idempotence, actors, awaited cleanup")
    }
}
