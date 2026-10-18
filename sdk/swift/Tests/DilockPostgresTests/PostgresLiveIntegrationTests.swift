import DilockCore
import DilockPostgres
import Foundation
import Testing

struct PostgresLiveIntegrationTests {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["DILOCK_POSTGRES_PORT"] != nil))
    func contentionRenewReleaseAndHandoff() async throws {
        let port = try #require(Int(ProcessInfo.processInfo.environment["DILOCK_POSTGRES_PORT"]!))
        let options = try PostgresProviderOptions(
            host: "127.0.0.1",
            port: port,
            username: "postgres",
            password: "postgres",
            database: "dilock",
            namespace: "swift-postgres-live",
            epoch: "swift-postgres-live-epoch",
            defaults: try AcquireOptions(wait: .seconds(5), lease: .seconds(10))
        )
        _ = try await PostgresProvider.provisionNamespace(options: options)
        let first = try await PostgresProvider(options: options)
        let second = try await PostgresProvider(options: options)
        defer { Task { await first.close(); await second.close() } }

        let resource = "swift-postgres-live-\(UUID().uuidString.lowercased())"
        let firstLease = try #require(try await first.lock(resource).tryAcquire())
        #expect(try await second.lock(resource).tryAcquire() == nil)
        #expect(try await firstLease.renew())
        #expect(await firstLease.release() == .released)

        let successor = try #require(try await second.lock(resource).tryAcquire())
        #expect(await successor.release() == .released)
        await first.close()
        await second.close()
    }
}
