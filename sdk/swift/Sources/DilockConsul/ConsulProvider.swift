import DilockCore
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

private final class ConsulNoRedirect: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

private struct ConsulMetadata: Codable, Equatable {
    let protocolMajor: Int
    let identityScheme: String
    let profile: String
    let coordinationEpoch: String
    let fixedLeaseMs: Int?
    enum CodingKeys: String, CodingKey {
        case protocolMajor = "protocol_major", identityScheme = "identity_scheme"
        case profile, coordinationEpoch = "coordination_epoch", fixedLeaseMs = "fixed_lease_ms"
    }
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(protocolMajor, forKey: .protocolMajor)
        try container.encode(identityScheme, forKey: .identityScheme)
        try container.encode(profile, forKey: .profile)
        try container.encode(coordinationEpoch, forKey: .coordinationEpoch)
        if let fixedLeaseMs { try container.encode(fixedLeaseMs, forKey: .fixedLeaseMs) }
        else { try container.encodeNil(forKey: .fixedLeaseMs) }
    }
}

struct ConsulKVRow: Decodable {
    let key: String
    let session: String?
    let lockIndex: UInt64
    let value: String?
    enum CodingKeys: String, CodingKey { case key = "Key", session = "Session", lockIndex = "LockIndex", value = "Value" }
}

private struct ConsulSessionRequest: Encodable {
    let name: String
    let ttl: String
    let behavior: String
    let lockDelay: String
    let checks: [String]
    enum CodingKeys: String, CodingKey { case name = "Name", ttl = "TTL", behavior = "Behavior", lockDelay = "LockDelay", checks = "Checks" }
}

private struct ConsulSessionRow: Decodable {
    let id: String
    let ttl: String?
    enum CodingKeys: String, CodingKey { case id = "ID", ttl = "TTL" }
}

/// Validated configuration for the Consul KV/session profile. The token is sent
/// only as X-Consul-Token and never appears in URLs or diagnostics.
public struct ConsulProviderOptions: Sendable, Equatable {
    public let endpoint: URL
    public let namespace: String
    public let epoch: String
    public let token: String?
    public let rpcTimeout: Duration
    public let cleanupTimeout: Duration
    public let defaults: AcquireOptions

    public init(endpoint: URL, namespace: String = "default", epoch: String,
                token: String? = nil, rpcTimeout: Duration = .seconds(2),
                cleanupTimeout: Duration = .seconds(2),
                defaults: AcquireOptions = try! AcquireOptions()) throws {
        guard ["http", "https"].contains(endpoint.scheme?.lowercased() ?? ""),
              endpoint.host != nil, endpoint.user == nil, endpoint.password == nil,
              endpoint.path.isEmpty || endpoint.path == "/", endpoint.query == nil, endpoint.fragment == nil else {
            throw DilockError.invalidConfiguration("Consul endpoint must be an HTTP(S) authority without credentials, query, or path")
        }
        _ = try Identity.resourceID(namespace: namespace, resource: "validation")
        guard !epoch.isEmpty, epoch.utf8.count <= 1024, !epoch.unicodeScalars.contains(where: { $0.value == 0 }) else {
            throw DilockError.invalidConfiguration("epoch must contain 1..1024 UTF-8 bytes and no NUL")
        }
        if let token {
            guard !token.isEmpty, token.utf8.allSatisfy({ (0x21...0x7e).contains($0) }) else {
                throw DilockError.invalidConfiguration("Consul ACL token must be printable non-space ASCII")
            }
        }
        guard rpcTimeout > .zero, cleanupTimeout > .zero,
              Self.durationSeconds(rpcTimeout) <= 86_400,
              Self.durationSeconds(cleanupTimeout) <= 86_400 else {
            throw DilockError.invalidConfiguration("RPC and cleanup timeouts must be positive and no longer than 24 hours")
        }
        self.endpoint = endpoint; self.namespace = namespace; self.epoch = epoch
        self.token = token; self.rpcTimeout = rpcTimeout; self.cleanupTimeout = cleanupTimeout; self.defaults = defaults
    }

    private static func durationSeconds(_ duration: Duration) -> Double {
        let parts = duration.components
        return Double(parts.seconds) + Double(parts.attoseconds) / 1e18
    }
}

public struct ConsulFence: Sendable, Equatable {
    public let epoch: String
    /// Decimal text preserves the server's unsigned 64-bit LockIndex.
    public let lockIndex: String
}

public enum ConsulMetadataProvision: Sendable, Equatable { case created, alreadyCompatible }

/// Consul KV/session adapter for `consul-kv-session-v1`.
public actor ConsulProvider {
    fileprivate let options: ConsulProviderOptions
    private let session: URLSession
    private var closed = false
    private var leases: [UUID: ConsulLease] = [:]
    private var shutdown: Task<Void, Never>?

    public init(options: ConsulProviderOptions) async throws {
        self.options = options
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.httpCookieStorage = nil
        configuration.urlCache = nil
        self.session = URLSession(configuration: configuration, delegate: ConsulNoRedirect(), delegateQueue: nil)
        do {
            let row = try await Self.readKV(key: Self.metadataKey(options.namespace), options: options, session: session, timeout: options.rpcTimeout)
            guard let row, let encoded = row.value, let bytes = Data(base64Encoded: encoded), bytes.count <= 8192,
                  Self.isExactMetadata(bytes, options: options),
                  let metadata = try? JSONDecoder().decode(ConsulMetadata.self, from: bytes),
                  metadata == Self.metadata(options), row.key == Self.metadataKey(options.namespace) else {
                throw DilockError.protocolMismatch("Consul namespace metadata is absent or incompatible")
            }
        } catch let error as DilockError { throw error }
        catch { throw DilockError.backendUnavailable("failed to validate Consul namespace metadata: \(error)") }
    }

    /// Explicit, idempotent CAS=0 namespace setup. Normal provider construction never provisions.
    public static func provisionNamespace(options: ConsulProviderOptions) async throws -> ConsulMetadataProvision {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false; configuration.httpCookieStorage = nil; configuration.urlCache = nil
        let session = URLSession(configuration: configuration, delegate: ConsulNoRedirect(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let key = metadataKey(options.namespace)
        let body = try JSONEncoder().encode(metadata(options))
        do {
            let response = try await request("PUT", path: "/v1/kv/\(key)?cas=0", body: body, options: options, session: session, timeout: options.rpcTimeout)
            if response.status == 200, response.data == Data("true".utf8) { return .created }
        } catch { /* A lost CAS response is resolved with a linearizable read below. */ }
        guard let row = try await readKV(key: key, options: options, session: session, timeout: options.rpcTimeout),
              let value = row.value, let bytes = Data(base64Encoded: value),
              isExactMetadata(bytes, options: options) else {
            throw DilockError.acquisitionOutcomeUnknown("Consul metadata CAS did not confirm creation and the resulting value is incompatible or unknown")
        }
        return .alreadyCompatible
    }

    public func lock(_ resource: String) throws -> ConsulLock {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        let id = try Identity.resourceID(namespace: options.namespace, resource: resource)
        return ConsulLock(provider: self, resource: resource, key: "dilock/locks/\(id)", defaults: options.defaults)
    }

    public func close() async {
        if let shutdown { await shutdown.value; return }
        closed = true
        let active = Array(leases.values); leases.removeAll()
        let session = self.session
        let operation = Task {
            for lease in active { _ = await lease.release() }
            session.invalidateAndCancel()
        }
        shutdown = operation
        await operation.value
    }

    fileprivate func acquire(key: String, policy: AcquireOptions, zeroWait: Bool = false) async throws -> ConsulLease? {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        try Task.checkCancellation()
        let requestedMs = try Self.milliseconds(policy.lease)
        guard requestedMs >= 10_000, requestedMs <= 86_400_000 else {
            throw DilockError.invalidConfiguration("Consul TTL must be between 10 seconds and 24 hours")
        }
        guard Self.seconds(policy.wait) <= 86_400 else {
            throw DilockError.invalidConfiguration("acquisition wait must be no longer than 24 hours")
        }
        let effectiveMs = ((requestedMs + 999) / 1000) * 1000
        let margin = max(100, effectiveMs / 100)
        guard Self.seconds(options.rpcTimeout) * 1000 + Double(margin) < Double(effectiveMs) else {
            throw DilockError.invalidConfiguration("RPC timeout plus lease safety margin must be less than the Consul TTL")
        }
        let started = ContinuousClock.now
        let acquisitionBudget = zeroWait ? options.rpcTimeout + options.rpcTimeout + options.rpcTimeout : policy.wait
        let deadline = started + acquisitionBudget
        let id = UUID()
        let sessionID: String
        do {
            let payload = try JSONEncoder().encode(ConsulSessionRequest(name: "dilock \(options.namespace)", ttl: "\(effectiveMs / 1000)s", behavior: "release", lockDelay: "0s", checks: []))
            let remaining = deadline - ContinuousClock.now
            guard remaining > .zero else { throw DilockError.timedOut }
            let response = try await Self.request("PUT", path: "/v1/session/create", body: payload, options: options, session: session, timeout: min(options.rpcTimeout, remaining))
            guard response.status == 200, let row = try? JSONDecoder().decode(ConsulSessionRow.self, from: response.data), !row.id.isEmpty else {
                throw DilockError.acquisitionOutcomeUnknown("Consul did not confirm session creation")
            }
            sessionID = row.id
        } catch let error as DilockError { throw error }
        catch { throw DilockError.acquisitionOutcomeUnknown("Consul session creation outcome is unknown: \(error)") }

        let lease = ConsulLease(provider: self, identity: id, key: key, sessionID: sessionID,
                                ttl: .milliseconds(effectiveMs), deadline: started + .milliseconds(effectiveMs - margin),
                                epoch: options.epoch, cleanupTimeout: options.cleanupTimeout, state: .waiting)
        leases[id] = lease
        var nextRenewal = ContinuousClock.now + .milliseconds(max(500, effectiveMs / 3))
        while ContinuousClock.now < deadline {
            if Task.isCancelled {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw CancellationError() }
                throw DilockError.acquisitionOutcomeUnknown("cancelled Consul acquisition; session cleanup is unconfirmed")
            }
            guard !closed else {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw DilockError.invalidConfiguration("provider closed during Consul acquisition") }
                throw DilockError.acquisitionOutcomeUnknown("provider closed during Consul acquisition and session cleanup is unconfirmed")
            }
            do {
                let response = try await Self.request("PUT", path: "/v1/kv/\(key)?acquire=\(sessionID)", body: Data(), options: options, session: session, timeout: min(options.rpcTimeout, max(.milliseconds(1), deadline - ContinuousClock.now)))
                if response.status == 200, response.data == Data("true".utf8) {
                    guard let owner = try await Self.readKV(key: key, options: options, session: session, timeout: options.rpcTimeout),
                          owner.session == sessionID, owner.lockIndex > 0,
                          await lease.markHeld(index: owner.lockIndex) else {
                        let cleaned = await lease.cleanupAttempt()
                        leases.removeValue(forKey: id)
                        if cleaned { throw DilockError.backendUnavailable("Consul acquire was acknowledged but owner verification failed") }
                        throw DilockError.acquisitionOutcomeUnknown("Consul owner verification failed and session cleanup is unconfirmed")
                    }
                    guard zeroWait || ContinuousClock.now < deadline else {
                        let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                        if cleaned { throw DilockError.timedOut }
                        throw DilockError.acquisitionOutcomeUnknown("Consul grant arrived after the acquisition deadline and cleanup is unconfirmed")
                    }
                    await lease.startRenewals()
                    return lease
                }
                if response.status >= 500 { throw DilockError.acquisitionOutcomeUnknown("Consul KV acquire returned HTTP \(response.status)") }
            } catch is CancellationError {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw CancellationError() }
                throw DilockError.acquisitionOutcomeUnknown("cancelled Consul acquisition; session cleanup is unconfirmed")
            } catch let error as DilockError {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw error }
                throw DilockError.acquisitionOutcomeUnknown("Consul acquisition failed and session cleanup is unconfirmed")
            } catch {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw DilockError.acquisitionOutcomeUnknown("Consul KV acquire outcome is unknown: \(error)") }
                throw DilockError.acquisitionOutcomeUnknown("Consul KV acquire and session cleanup are unknown: \(error)")
            }
            if zeroWait { _ = await lease.cleanupAttempt(); leases.removeValue(forKey: id); return nil }
            if ContinuousClock.now >= nextRenewal {
                guard await lease.renewWaiting() else {
                    let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                    if cleaned { throw DilockError.backendUnavailable("Consul waiting session renewal failed") }
                    throw DilockError.acquisitionOutcomeUnknown("Consul waiting session renewal and cleanup are unconfirmed")
                }
                nextRenewal = ContinuousClock.now + .milliseconds(max(500, effectiveMs / 3))
            }
            do { try await Task.sleep(for: .milliseconds(50)) }
            catch {
                let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
                if cleaned { throw CancellationError() }
                throw DilockError.acquisitionOutcomeUnknown("cancelled Consul wait; session cleanup is unconfirmed")
            }
        }
        let cleaned = await lease.cleanupAttempt(); leases.removeValue(forKey: id)
        if cleaned { if zeroWait { return nil }; throw DilockError.timedOut }
        throw DilockError.acquisitionOutcomeUnknown("Consul wait ended and session cleanup is unconfirmed")
    }

    fileprivate func renew(sessionID: String) async throws -> ConsulSessionRow? {
        let response = try await Self.request("PUT", path: "/v1/session/renew/\(sessionID)", body: Data(), options: options, session: session, timeout: options.rpcTimeout)
        guard response.status == 200 else { return nil }
        return try? JSONDecoder().decode([ConsulSessionRow].self, from: response.data).first
    }

    fileprivate func owner(key: String) async throws -> ConsulKVRow? {
        try await Self.readKV(key: key, options: options, session: session, timeout: options.rpcTimeout)
    }

    fileprivate func release(key: String, sessionID: String, cleanup: Duration) async -> ReleaseOutcome {
        var outcome: ReleaseOutcome = .unknown
        do {
            let result = try await Self.request("PUT", path: "/v1/kv/\(key)?release=\(sessionID)", body: Data(), options: options, session: session, timeout: cleanup)
            if result.status == 200, result.data == Data("true".utf8) { outcome = .released }
            if result.status == 200, result.data == Data("false".utf8) {
                let row = try await owner(key: key)
                outcome = row?.session == sessionID ? .unknown : .alreadyNotOwned
            }
        } catch { }
        guard await destroy(sessionID: sessionID, timeout: cleanup) else { return .unknown }
        return outcome
    }

    fileprivate func destroy(sessionID: String, timeout: Duration) async -> Bool {
        do {
            let response = try await Self.request("PUT", path: "/v1/session/destroy/\(sessionID)", body: Data(), options: options, session: session, timeout: timeout)
            return response.status == 200 && response.data == Data("true".utf8)
        } catch { return false }
    }

    fileprivate func leaseEnded(_ id: UUID) { leases.removeValue(forKey: id) }

    private static func metadata(_ options: ConsulProviderOptions) -> ConsulMetadata {
        ConsulMetadata(protocolMajor: 1, identityScheme: "sha256-framed-utf8-v1", profile: "consul-kv-session-v1", coordinationEpoch: options.epoch, fixedLeaseMs: nil)
    }
    private static func metadataKey(_ namespace: String) -> String { "dilock/namespaces/\(namespace)/meta" }
    private static func milliseconds(_ duration: Duration) throws -> Int64 {
        let c = duration.components
        let value = c.seconds.multipliedReportingOverflow(by: 1000)
        guard !value.overflow else { throw DilockError.invalidConfiguration("duration overflows milliseconds") }
        let sum = value.partialValue.addingReportingOverflow(c.attoseconds / 1_000_000_000_000_000)
        guard !sum.overflow, sum.partialValue > 0 else { throw DilockError.invalidConfiguration("duration must be finite and positive") }
        let roundsUp = c.attoseconds % 1_000_000_000_000_000 != 0
        let rounded = sum.partialValue.addingReportingOverflow(roundsUp ? 1 : 0)
        guard !rounded.overflow else { throw DilockError.invalidConfiguration("duration overflows milliseconds") }
        return rounded.partialValue
    }
    private static func seconds(_ duration: Duration) -> Double {
        let c = duration.components; return Double(c.seconds) + Double(c.attoseconds) / 1e18
    }
    private static func isExactMetadata(_ data: Data, options: ConsulProviderOptions) -> Bool {
        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(json.keys) == Set(["protocol_major", "identity_scheme", "profile", "coordination_epoch", "fixed_lease_ms"]),
              let decoded = try? JSONDecoder().decode(ConsulMetadata.self, from: data) else { return false }
        return decoded == metadata(options)
    }
    private struct Response { let status: Int; let data: Data }
    private static func request(_ method: String, path: String, body: Data?, options: ConsulProviderOptions,
                                session: URLSession, timeout: Duration) async throws -> Response {
        var components = URLComponents(url: options.endpoint, resolvingAgainstBaseURL: false)!
        let parts = path.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
        components.path = "/" + String(parts.first ?? "").trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        if parts.count > 1 { components.percentEncodedQuery = String(parts[1]) }
        guard let url = components.url else { throw DilockError.invalidConfiguration("invalid Consul API path") }
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = max(0.001, seconds(timeout))
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body { request.httpBody = body; request.setValue("application/json", forHTTPHeaderField: "Content-Type") }
        if let token = options.token { request.setValue(token, forHTTPHeaderField: "X-Consul-Token") }
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw URLError(.badServerResponse) }
        guard (200..<300).contains(http.statusCode) || http.statusCode == 404 else {
            if (method == "PUT" || method == "POST"), http.statusCode >= 500 {
                throw DilockError.acquisitionOutcomeUnknown("Consul mutation returned HTTP \(http.statusCode)")
            }
            throw DilockError.backendUnavailable("Consul request returned HTTP \(http.statusCode)")
        }
        return Response(status: http.statusCode, data: data)
    }
    private static func readKV(key: String, options: ConsulProviderOptions, session: URLSession, timeout: Duration) async throws -> ConsulKVRow? {
        let response = try await request("GET", path: "/v1/kv/\(key)?consistent", body: nil, options: options, session: session, timeout: timeout)
        guard response.status != 404 else { return nil }
        return try decodeKVRow(response.data, expectedKey: key)
    }

    static func decodeKVRow(_ data: Data, expectedKey: String) throws -> ConsulKVRow {
        guard let rows = try? JSONDecoder().decode([ConsulKVRow].self, from: data),
              rows.count == 1, rows[0].key == expectedKey else {
            throw DilockError.protocolMismatch("Consul KV response did not contain one valid row for the requested key")
        }
        return rows[0]
    }
}

public struct ConsulLock: Sendable {
    fileprivate let provider: ConsulProvider
    public let resource: String
    fileprivate let key: String
    private let defaults: AcquireOptions
    fileprivate init(provider: ConsulProvider, resource: String, key: String, defaults: AcquireOptions) {
        self.provider = provider; self.resource = resource; self.key = key; self.defaults = defaults
    }
    public func acquire(_ policy: AcquireOptions? = nil) async throws -> ConsulLease {
        guard let lease = try await provider.acquire(key: key, policy: policy ?? defaults) else { throw DilockError.timedOut }
        return lease
    }
    public func tryAcquire(lease: Duration? = nil) async throws -> ConsulLease? {
        let policy = try AcquireOptions(wait: .milliseconds(1), lease: lease ?? defaults.lease)
        return try await provider.acquire(key: key, policy: policy, zeroWait: true)
    }
    public func withLease<Value: Sendable>(_ policy: AcquireOptions? = nil,
                                           operation: @Sendable (ConsulLease) async throws -> Value) async throws -> Value {
        let acquired = try await acquire(policy)
        return try await withLeaseScope(acquired, cleanup: { lease in await lease.releaseForScope() }, operation: operation)
    }
}

public actor ConsulLease: Lease {
    public enum State: Sendable, Equatable { case waiting, held, releasing, released, lost, releasedUnknown }
    fileprivate let identity: UUID
    private let provider: ConsulProvider
    private let key: String
    private let sessionID: String
    private let ttl: Duration
    private let epoch: String
    private let cleanupTimeout: Duration
    private var expiry: ContinuousClock.Instant
    private var index: UInt64 = 0
    private var currentState: State
    private var renewer: Task<Void, Never>?

    fileprivate init(provider: ConsulProvider, identity: UUID, key: String, sessionID: String, ttl: Duration,
                     deadline: ContinuousClock.Instant, epoch: String, cleanupTimeout: Duration, state: State) {
        self.provider = provider; self.identity = identity; self.key = key; self.sessionID = sessionID
        self.ttl = ttl; self.expiry = deadline; self.epoch = epoch; self.cleanupTimeout = cleanupTimeout; self.currentState = state
    }
    public var state: State { currentState }
    public var effectiveTTL: Duration { ttl }
    public var fence: ConsulFence? { index > 0 ? ConsulFence(epoch: epoch, lockIndex: String(index)) : nil }
    public var isValid: Bool { (currentState == .waiting || currentState == .held) && ContinuousClock.now < expiry }
    fileprivate func markHeld(index: UInt64) -> Bool {
        guard currentState == .waiting, index > 0, ContinuousClock.now < expiry else { return false }
        self.index = index; currentState = .held; return true
    }
    fileprivate func startRenewals() {
        guard renewer == nil else { return }
        let interval = max(.milliseconds(500), ttl / 3)
        renewer = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: interval) } catch { return }
                guard let self, !(await self.renewOnce()) else { continue }
                await self.markLost(); return
            }
        }
    }
    fileprivate func renewWaiting() async -> Bool { await renewOnce() }
    private func renewOnce() async -> Bool {
        guard currentState == .waiting || currentState == .held else { return false }
        let sentAt = ContinuousClock.now
        do {
            guard let result = try await provider.renew(sessionID: sessionID), result.id == sessionID else { return false }
            guard currentState == .waiting || currentState == .held else { return false }
            guard let owner = try await provider.owner(key: key) else {
                guard currentState == .waiting else { return false }
                let ms = Int64(max(10, ttl.components.seconds * 1000 + ttl.components.attoseconds / 1_000_000_000_000_000))
                expiry = sentAt + .milliseconds(ms - max(100, ms / 10))
                return ContinuousClock.now < expiry
            }
            guard currentState == .waiting || currentState == .held else { return false }
            if currentState == .held && (owner.session != sessionID || owner.lockIndex != index) { return false }
            let ms = Int64(max(10, ttl.components.seconds * 1000 + ttl.components.attoseconds / 1_000_000_000_000_000))
            expiry = sentAt + .milliseconds(ms - max(100, ms / 10))
            return ContinuousClock.now < expiry
        } catch { return false }
    }
    private func markLost() { guard currentState == .waiting || currentState == .held else { return }; currentState = .lost; renewer?.cancel(); renewer = nil }
    fileprivate func cleanupAttempt() async -> Bool {
        renewer?.cancel(); renewer = nil
        let released = await provider.release(key: key, sessionID: sessionID, cleanup: cleanupTimeout)
        let destroyed = await provider.destroy(sessionID: sessionID, timeout: cleanupTimeout)
        if released == .released || (released == .alreadyNotOwned && destroyed) { currentState = .released; return true }
        currentState = .releasedUnknown; return false
    }
    public func release() async -> ReleaseOutcome {
        guard currentState == .held || currentState == .waiting || currentState == .lost else {
            return currentState == .released ? .released : .unknown
        }
        currentState = .releasing
        let pendingRenewal = renewer
        pendingRenewal?.cancel(); renewer = nil
        await pendingRenewal?.value
        let result = await provider.release(key: key, sessionID: sessionID, cleanup: cleanupTimeout)
        let destroyed = await provider.destroy(sessionID: sessionID, timeout: cleanupTimeout)
        if result == .released { _ = destroyed; currentState = .released; await provider.leaseEnded(identity); return .released }
        currentState = .releasedUnknown
        await provider.leaseEnded(identity)
        return result == .alreadyNotOwned ? .alreadyNotOwned : .unknown
    }
    fileprivate func releaseForScope() async -> (outcome: ReleaseOutcome, ownershipWasValid: Bool) {
        let valid = isValid
        return (await release(), valid)
    }
}
