import DilockCore
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import EtcdProtos

actor EtcdReleaseOperation {
    private var task: Task<ReleaseOutcome, Never>?

    func perform(_ operation: @escaping @Sendable () async -> ReleaseOutcome) async -> ReleaseOutcome {
        if let task { return await task.value }
        let task = Task { await operation() }
        self.task = task
        return await task.value
    }
}

enum EtcdReleaseOwnership: Equatable {
    case owned
    case notOwned
    case unknown
}

enum EtcdRevokeObservation: Equatable {
    case acknowledged
    case notFound
    case unknown
}

/// Shares one provider shutdown operation among concurrent close callers.
final class EtcdCloseOperation: @unchecked Sendable {
    private let lock = NSLock()
    private var task: Task<Void, Never>?

    func run(_ operation: @escaping @Sendable () async -> Void) -> Task<Void, Never> {
        lock.lock()
        defer { lock.unlock() }
        if let task { return task }
        let task = Task.detached { await operation() }
        self.task = task
        return task
    }
}

/// etcd v3 native-Lock adapter. Metadata is read at connection time and is never
/// provisioned by the client. This profile requires a single approved epoch.
public actor EtcdProvider {
    fileprivate let options: EtcdProviderOptions
    private var closed = false
    private var activeLeases: [UUID: EtcdLease] = [:]
    private var pendingAttempts: [UUID: EtcdLease] = [:]
    private let closeOperation = EtcdCloseOperation()

    public init(options: EtcdProviderOptions) async throws {
        self.options = options
        guard options.endpoint.scheme == "http" else {
            throw DilockError.unsupportedCapability("the Swift etcd transport currently enables plaintext HTTP/2 only")
        }
        do {
            let rpcLimit = try Self.milliseconds(options.rpcTimeout, maximum: 86_400_000)
            let response = try await Self.withTimeout(.milliseconds(rpcLimit)) {
                try await withGRPCClient(transport: try Self.transport(options.endpoint)) { client in
                    let kv = Etcdserverpb_KV.Client(wrapping: client)
                    var request = Etcdserverpb_RangeRequest()
                    request.key = Data("/dilock/namespaces/\(options.namespace)/meta".utf8)
                    return try await kv.range(request)
                }
            }
            guard let value = response.kvs.first?.value,
                  value.count <= 8192,
                  let metadata = try? JSONDecoder().decode(Metadata.self, from: value),
                  metadata.protocolMajor == 1,
                  metadata.identityScheme == "sha256-framed-utf8-v1",
                  metadata.profile == "etcd-native-lock-v1",
                  metadata.coordinationEpoch == options.epoch,
                  metadata.fixedLeaseMs == nil,
                  response.kvs.count == 1,
                  response.kvs[0].key == Data("/dilock/namespaces/\(options.namespace)/meta".utf8) else {
                throw DilockError.protocolMismatch("etcd namespace metadata is absent or incompatible")
            }
        } catch let error as DilockError { throw error }
        catch { throw DilockError.backendUnavailable("failed to validate etcd namespace metadata: \(error)") }
    }

    public func lock(_ resource: String) throws -> EtcdLock {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        _ = try Identity.resourceID(namespace: options.namespace, resource: resource)
        return EtcdLock(provider: self, resource: resource, defaults: options.defaults)
    }

    /// Stops this provider and revokes each tracked acquisition with bounded cleanup.
    /// Concurrent callers await the same shutdown; cleanup does not inherit caller cancellation.
    /// Unknown cleanup remains bounded by the server's lease TTL.
    public func close() async {
        let leases: [EtcdLease]
        let attempts: [EtcdLease]
        if closed {
            leases = []
            attempts = []
        } else {
            closed = true
            leases = Array(activeLeases.values)
            attempts = Array(pendingAttempts.values)
            activeLeases.removeAll()
            pendingAttempts.removeAll()
        }
        let cleanupTimeout = options.cleanupTimeout
        let task = closeOperation.run {
            for lease in leases { _ = await lease.release() }
            for attempt in attempts { _ = await attempt.revokeBounded(cleanup: cleanupTimeout) }
        }
        await task.value
    }

    fileprivate func acquire(resource: String, policy: AcquireOptions) async throws -> EtcdLease {
        guard !closed else { throw DilockError.invalidConfiguration("provider is closed") }
        try Task.checkCancellation()
        guard policy.wait > .zero else {
            throw DilockError.unsupportedCapability("etcd native Lock queues and cannot implement zero-wait acquire")
        }
        let endpoint = options.endpoint
        let leaseMillis = try Self.milliseconds(policy.lease, maximum: 86_400_000)
        _ = try Self.milliseconds(policy.wait, maximum: 86_400_000)
        let rpcMillis = try Self.milliseconds(options.rpcTimeout, maximum: 86_400_000)
        let cleanupMillis = try Self.milliseconds(options.cleanupTimeout, maximum: 86_400_000)
        let maximumLifetime = try Self.milliseconds(options.maximumLeaseLifetime, maximum: 86_400_000)
        guard maximumLifetime >= leaseMillis else {
            throw DilockError.invalidConfiguration("maximum lease lifetime cannot be shorter than the requested lease")
        }
        let margin = max(100, leaseMillis / 10)
        guard rpcMillis + margin < leaseMillis else {
            throw DilockError.invalidConfiguration("RPC timeout plus lease safety margin must be less than the lease")
        }
        // Every attempt gets a fresh lease ID. The native Lock RPC queues on the
        // server; keepalive starts before the waiter is submitted and stays active.
        let acquisitionStarted = ContinuousClock.now
        let acquisitionDeadline = acquisitionStarted + policy.wait
        let grantSent = ContinuousClock.now
        let grant: Etcdserverpb_LeaseGrantResponse
        let grantBudget = min(options.rpcTimeout, acquisitionDeadline - ContinuousClock.now)
        guard grantBudget > .zero else { throw DilockError.timedOut }
        do {
            grant = try await Self.withTimeout(grantBudget) {
                try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                    let lease = Etcdserverpb_Lease.Client(wrapping: client)
                    var request = Etcdserverpb_LeaseGrantRequest()
                    request.ttl = (leaseMillis + 999) / 1000
                    return try await lease.leaseGrant(request)
                }
            }
        } catch {
            throw DilockError.acquisitionOutcomeUnknown("etcd did not confirm the attempt lease grant: \(error)")
        }
        let leaseID = grant.id
        let effective = grant.ttl.multipliedReportingOverflow(by: 1000)
        guard !effective.overflow else {
            let revoked: Bool
            if leaseID != 0 {
                revoked = await Self.revokeLease(
                    endpoint: endpoint,
                    leaseID: leaseID,
                    timeout: options.cleanupTimeout
                )
            } else {
                revoked = false
            }
            throw Self.invalidGrantFailure(
                hasLeaseID: leaseID != 0,
                revokeAcknowledged: revoked,
                detail: "etcd returned an overflowing effective lease TTL"
            )
        }
        let effectiveMillis = effective.partialValue
        guard leaseID != 0 else {
            throw Self.invalidGrantFailure(
                hasLeaseID: false,
                revokeAcknowledged: false,
                detail: "etcd returned an invalid granted lease"
            )
        }
        guard effectiveMillis > 0 else {
            let revoked = await Self.revokeLease(
                endpoint: endpoint,
                leaseID: leaseID,
                timeout: options.cleanupTimeout
            )
            throw Self.invalidGrantFailure(
                hasLeaseID: true,
                revokeAcknowledged: revoked,
                detail: "etcd returned a nonpositive effective lease TTL"
            )
        }
        guard !closed else {
            _ = await Self.revokeLease(endpoint: endpoint, leaseID: leaseID, timeout: options.cleanupTimeout)
            throw DilockError.invalidConfiguration("provider closed while granting an acquisition lease")
        }

        let attempt = EtcdLease(
            provider: self,
            leaseID: leaseID,
            effectiveLease: .milliseconds(effectiveMillis),
            deadline: grantSent + .milliseconds(effectiveMillis - max(100, effectiveMillis / 10)),
            endpoint: options.endpoint,
            epoch: options.epoch,
            cleanupTimeout: options.cleanupTimeout,
            rpcTimeout: options.rpcTimeout
        )
        pendingAttempts[attempt.identity] = attempt
        let keepAliveBudget = min(options.rpcTimeout, acquisitionDeadline - ContinuousClock.now)
        guard keepAliveBudget > .zero else {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.timedOut }
            throw DilockError.acquisitionOutcomeUnknown("acquisition wait budget elapsed before etcd keepalive; cleanup is unconfirmed")
        }
        do {
            guard try await attempt.startKeepAlive(rpcTimeout: keepAliveBudget) else {
                let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
                pendingAttempts.removeValue(forKey: attempt.identity)
                if revoked { throw DilockError.backendUnavailable("etcd LeaseKeepAlive did not confirm the attempt lease before the RPC deadline") }
                throw DilockError.acquisitionOutcomeUnknown("etcd LeaseKeepAlive did not start and lease revocation is unconfirmed")
            }
        } catch is CancellationError {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw CancellationError() }
            throw DilockError.acquisitionOutcomeUnknown("cancelled while starting etcd keepalive; lease revocation is unconfirmed")
        } catch {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.backendUnavailable("failed to start etcd keepalive: \(error)") }
            throw DilockError.acquisitionOutcomeUnknown("failed to start etcd keepalive and lease revocation is unconfirmed")
        }
        guard !closed else {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.invalidConfiguration("provider closed while starting etcd keepalive") }
            throw DilockError.acquisitionOutcomeUnknown("provider closed while starting keepalive; cleanup is unconfirmed")
        }

        let id = try Identity.resourceID(namespace: options.namespace, resource: resource)
        let lockName = Data(Identity.etcdLockName(resourceID: id).utf8)
        let remainingWait = policy.wait - (ContinuousClock.now - acquisitionStarted)
        guard remainingWait > .zero else {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.timedOut }
            throw DilockError.acquisitionOutcomeUnknown("the complete acquisition wait budget elapsed before etcd Lock; cleanup is unconfirmed")
        }
        let locked: V3lockpb_LockResponse
        // The native Lock RPC intentionally waits in etcd's queue. Its deadline
        // is the caller's remaining acquisition budget, not the short RPC
        // timeout used for non-queueing operations such as Grant and Range.
        let lockBudget = remainingWait
        do {
            locked = try await Self.withTimeout(lockBudget) {
                try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                    let lock = V3lockpb_Lock.Client(wrapping: client)
                    var request = V3lockpb_LockRequest()
                    request.name = lockName
                    request.lease = leaseID
                    return try await lock.lock(request)
                }
            }
        } catch is CancellationError {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw CancellationError() }
            throw DilockError.acquisitionOutcomeUnknown("cancelled etcd Lock wait; lease revocation was not confirmed")
        } catch is TimeoutError {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.timedOut }
            throw DilockError.acquisitionOutcomeUnknown("etcd Lock timed out; queued attempt cleanup is unknown")
        } catch {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.backendUnavailable("etcd Lock failed: \(error)") }
            throw DilockError.acquisitionOutcomeUnknown("etcd Lock failed and queued attempt cleanup is unknown: \(error)")
        }

        if Task.isCancelled {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw CancellationError() }
            throw DilockError.acquisitionOutcomeUnknown("cancelled after etcd Lock returned; attempt cleanup is unconfirmed")
        }
        guard !closed else {
            let revoked = await attempt.revokeBounded(cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            if revoked { throw DilockError.invalidConfiguration("provider closed during etcd Lock") }
            throw DilockError.acquisitionOutcomeUnknown("provider closed during etcd Lock; cleanup is unconfirmed")
        }

        let exactKey = locked.key
        guard !exactKey.isEmpty else {
            _ = await attempt.unlockAndRevoke(key: exactKey, cleanup: .milliseconds(cleanupMillis))
            pendingAttempts.removeValue(forKey: attempt.identity)
            throw DilockError.acquisitionOutcomeUnknown("etcd Lock returned an empty opaque key")
        }
        let verificationBudget = min(options.rpcTimeout, policy.wait - (ContinuousClock.now - acquisitionStarted))
        guard verificationBudget > .zero else {
            let outcome = await attempt.unlockAndRevoke(
                key: exactKey,
                cleanup: .milliseconds(cleanupMillis)
            )
            pendingAttempts.removeValue(forKey: attempt.identity)
            if let failure = Self.waitDeadlineFailure(remaining: .zero, cleanupOutcome: outcome) {
                throw failure
            }
            throw DilockError.timedOut
        }
        do {
            let proof = try await Self.withTimeout(verificationBudget) {
                try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                    let kv = Etcdserverpb_KV.Client(wrapping: client)
                    var request = Etcdserverpb_RangeRequest()
                    request.key = exactKey
                    return try await kv.range(request)
                }
            }
            guard let row = proof.kvs.first, row.key == exactKey,
                  row.lease == leaseID, row.createRevision > 0,
                  await attempt.isValid else {
                _ = await attempt.unlockAndRevoke(key: exactKey, cleanup: .milliseconds(cleanupMillis))
                pendingAttempts.removeValue(forKey: attempt.identity)
                throw DilockError.acquisitionOutcomeUnknown("linearizable Range did not verify exact key, attempt lease and positive create_revision")
            }
            guard await attempt.remainingValidity > .zero else {
                _ = await attempt.unlockAndRevoke(key: exactKey, cleanup: .milliseconds(cleanupMillis))
                pendingAttempts.removeValue(forKey: attempt.identity)
                throw DilockError.acquisitionOutcomeUnknown("verification exceeded the confirmed lease budget")
            }
            if Task.isCancelled {
                let outcome = await attempt.unlockAndRevoke(key: exactKey, cleanup: .milliseconds(cleanupMillis))
                pendingAttempts.removeValue(forKey: attempt.identity)
                if outcome != .unknown { throw CancellationError() }
                throw DilockError.acquisitionOutcomeUnknown("cancelled after exact-key verification; cleanup is unconfirmed")
            }
            let remainingWait = policy.wait - (ContinuousClock.now - acquisitionStarted)
            if remainingWait <= .zero {
                let outcome = await attempt.unlockAndRevoke(
                    key: exactKey,
                    cleanup: .milliseconds(cleanupMillis)
                )
                pendingAttempts.removeValue(forKey: attempt.identity)
                if let failure = Self.waitDeadlineFailure(
                    remaining: remainingWait,
                    cleanupOutcome: outcome
                ) {
                    throw failure
                }
            }
            guard await attempt.markAcquired(
                key: exactKey,
                revision: row.createRevision,
                maximumLifetime: .milliseconds(maximumLifetime)
            ) else {
                _ = await attempt.unlockAndRevoke(key: exactKey, cleanup: .milliseconds(cleanupMillis))
                pendingAttempts.removeValue(forKey: attempt.identity)
                throw DilockError.acquisitionOutcomeUnknown("lease became invalid before ownership could be returned")
            }
            guard !closed else {
                pendingAttempts.removeValue(forKey: attempt.identity)
                throw DilockError.invalidConfiguration("provider closed during acquisition")
            }
            pendingAttempts.removeValue(forKey: attempt.identity)
            activeLeases[attempt.identity] = attempt
            return attempt
        } catch let error as DilockError { throw error }
        catch {
            let cleanupOutcome = await attempt.unlockAndRevoke(
                key: exactKey,
                cleanup: .milliseconds(cleanupMillis)
            )
            pendingAttempts.removeValue(forKey: attempt.identity)
            if Task.isCancelled {
                if Self.cancellationCleanupWasConfirmed(cleanupOutcome) {
                    throw CancellationError()
                }
                throw DilockError.acquisitionOutcomeUnknown(
                    "etcd ownership verification was cancelled and cleanup is unconfirmed"
                )
            }
            let remainingWait = policy.wait - (ContinuousClock.now - acquisitionStarted)
            if let failure = Self.waitDeadlineFailure(
                remaining: remainingWait,
                cleanupOutcome: cleanupOutcome
            ) {
                throw failure
            }
            throw DilockError.acquisitionOutcomeUnknown("etcd lock ownership verification failed: \(error)")
        }
    }

    fileprivate func unlockAndRevoke(leaseID: Int64, key: Data, cleanup: Duration) async -> ReleaseOutcome {
        let cleanupStarted = ContinuousClock.now
        let endpoint = options.endpoint
        let ownership = await observeOwnership(key: key, leaseID: leaseID, timeout: cleanup)
        var unlockConfirmed = false
        let remainingForUnlock = cleanup - (ContinuousClock.now - cleanupStarted)
        if ownership == .owned, remainingForUnlock > .zero {
            do {
                try await Self.withTimeout(remainingForUnlock) {
                    try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                        let lock = V3lockpb_Lock.Client(wrapping: client)
                        var request = V3lockpb_UnlockRequest()
                        request.key = key
                        _ = try await lock.unlock(request)
                    }
                }
                unlockConfirmed = true
            } catch { /* Revoke this attempt lease below; never touch a prefix. */ }
        }

        let remainingForRevoke = cleanup - (ContinuousClock.now - cleanupStarted)
        let revokeResult: EtcdRevokeObservation = remainingForRevoke > .zero
            ? await revokeObservation(leaseID: leaseID, cleanup: remainingForRevoke)
            : .unknown
        return Self.classifyReleaseOutcome(
            ownership: ownership,
            unlockAcknowledged: unlockConfirmed,
            revokeObservation: revokeResult
        ) ?? .unknown
    }

    static func classifyReleaseOutcome(
        ownership: EtcdReleaseOwnership,
        unlockAcknowledged: Bool,
        revokeObservation: EtcdRevokeObservation
    ) -> ReleaseOutcome? {
        // Unlock addresses a key, not a lease, and can acknowledge a no-op if
        // the lease expires after the ownership read. Require both cleanup ACKs.
        if revokeObservation == .notFound { return .alreadyNotOwned }
        if ownership == .notOwned { return .alreadyNotOwned }
        if ownership == .owned, unlockAcknowledged, revokeObservation == .acknowledged { return .released }
        if revokeObservation == .acknowledged { return .alreadyNotOwned }
        return nil
    }

    private func observeOwnership(key: Data, leaseID: Int64, timeout: Duration) async -> EtcdReleaseOwnership {
        let endpoint = options.endpoint
        do {
            let response = try await Self.withTimeout(timeout) {
                try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                    let kv = Etcdserverpb_KV.Client(wrapping: client)
                    var request = Etcdserverpb_RangeRequest()
                    request.key = key
                    return try await kv.range(request)
                }
            }
            guard response.kvs.count == 1,
                  response.kvs[0].key == key,
                  response.kvs[0].lease == leaseID else { return .notOwned }
            return .owned
        } catch { return .unknown }
    }

    func revokeObservation(leaseID: Int64, cleanup: Duration) async -> EtcdRevokeObservation {
        let endpoint = options.endpoint
        do {
            try await Self.withTimeout(cleanup) {
                try await withGRPCClient(transport: try Self.transport(endpoint)) { client in
                    let lease = Etcdserverpb_Lease.Client(wrapping: client)
                    var request = Etcdserverpb_LeaseRevokeRequest()
                    request.id = leaseID
                    _ = try await lease.leaseRevoke(request)
                }
            }
            return .acknowledged
        } catch let error as RPCError where error.code == .notFound {
            return .notFound
        } catch { return .unknown }
    }

    fileprivate func revoke(leaseID: Int64, cleanup: Duration) async -> Bool {
        let observation = await revokeObservation(leaseID: leaseID, cleanup: cleanup)
        return observation != .unknown
    }

    fileprivate func forgetLease(_ id: UUID) { activeLeases.removeValue(forKey: id) }

    fileprivate static func transport(_ endpoint: URL) throws -> HTTP2ClientTransport.Posix {
        try .http2NIOPosix(
            target: .dns(host: endpoint.host!, port: endpoint.port ?? 2379),
            transportSecurity: .plaintext
        )
    }

    private static func revokeLease(endpoint: URL, leaseID: Int64, timeout: Duration) async -> Bool {
        do {
            try await withTimeout(timeout) {
                try await withGRPCClient(transport: try transport(endpoint)) { client in
                    let lease = Etcdserverpb_Lease.Client(wrapping: client)
                    var request = Etcdserverpb_LeaseRevokeRequest()
                    request.id = leaseID
                    _ = try await lease.leaseRevoke(request)
                }
            }
            return true
        } catch let error as RPCError where error.code == .notFound { return true }
        catch { return false }
    }

    private static func withTimeout<T: Sendable>(
        _ timeout: Duration,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask { try await Task.sleep(for: timeout); throw TimeoutError() }
            guard let value = try await group.next() else { throw TimeoutError() }
            group.cancelAll()
            return value
        }
    }

    static func waitDeadlineFailure(
        remaining: Duration,
        cleanupOutcome: ReleaseOutcome
    ) -> DilockError? {
        guard remaining <= .zero else { return nil }
        switch cleanupOutcome {
        case .released, .alreadyNotOwned: return .timedOut
        case .unknown:
            return .acquisitionOutcomeUnknown(
                "etcd ownership verification completed after the wait budget and cleanup was unconfirmed"
            )
        }
    }

    static func cancellationCleanupWasConfirmed(_ cleanupOutcome: ReleaseOutcome) -> Bool {
        cleanupOutcome != .unknown
    }

    static func invalidGrantFailure(
        hasLeaseID: Bool,
        revokeAcknowledged: Bool,
        detail: String
    ) -> DilockError {
        if hasLeaseID && revokeAcknowledged { return .protocolMismatch(detail) }
        return .acquisitionOutcomeUnknown("\(detail); granted lease cleanup is unconfirmed")
    }

    static func renewedLeaseDeadline(
        previousDeadline: ContinuousClock.Instant,
        sentAt: ContinuousClock.Instant,
        ttlSeconds: Int64,
        now: ContinuousClock.Instant
    ) -> ContinuousClock.Instant? {
        guard ttlSeconds > 0, now < previousDeadline else { return nil }
        let ttl = Duration.seconds(ttlSeconds)
        let margin = max(.milliseconds(100), ttl / 10)
        let candidate = sentAt + ttl - margin
        return now < candidate ? candidate : nil
    }

    private static func milliseconds(_ duration: Duration, maximum: Int64, allowZero: Bool = false) throws -> Int64 {
        let parts = duration.components
        let seconds = parts.seconds.multipliedReportingOverflow(by: 1000)
        guard !seconds.overflow else { throw DilockError.invalidConfiguration("duration overflows milliseconds") }
        let sum = seconds.partialValue.addingReportingOverflow(parts.attoseconds / 1_000_000_000_000_000)
        guard !sum.overflow else { throw DilockError.invalidConfiguration("duration overflows milliseconds") }
        let ms = sum.partialValue
        guard (allowZero ? ms >= 0 : ms > 0), ms <= maximum else {
            throw DilockError.invalidConfiguration("duration is outside the supported finite range")
        }
        return ms
    }

    private struct Metadata: Decodable {
        let protocolMajor: Int
        let identityScheme: String
        let profile: String
        let coordinationEpoch: String
        let fixedLeaseMs: Int?
        enum CodingKeys: String, CodingKey {
            case protocolMajor = "protocol_major", identityScheme = "identity_scheme"
            case profile, coordinationEpoch = "coordination_epoch", fixedLeaseMs = "fixed_lease_ms"
        }
    }
}

private struct TimeoutError: Error {}

public struct EtcdLock: Sendable {
    fileprivate let provider: EtcdProvider
    public let resource: String
    private let defaults: AcquireOptions

    fileprivate init(provider: EtcdProvider, resource: String, defaults: AcquireOptions) {
        self.provider = provider; self.resource = resource; self.defaults = defaults
    }

    /// etcd's native Lock RPC queues, so a one-shot try-acquire is unsupported.
    public func tryAcquire() async throws -> EtcdLease? {
        throw DilockError.unsupportedCapability("etcd native Lock has no non-queuing try-acquire operation")
    }

    public func acquire(_ policy: AcquireOptions? = nil) async throws -> EtcdLease {
        try await provider.acquire(resource: resource, policy: policy ?? defaults)
    }

    /// Acquires this resource, invokes `operation` once, and awaits cleanup
    /// before returning. The policy's wait budget includes the native Lock RPC;
    /// handle defaults apply when the policy is omitted. Acquisition and
    /// operation errors propagate. If the operation throws, its error remains
    /// primary even if cleanup is uncertain. An acknowledged revoke is accepted
    /// as terminal cleanup; unknown cleanup after success throws
    /// `DilockError.releaseUnknown`.
    ///
    /// Cancellation is cooperative: the operation can observe task cancellation,
    /// and cleanup is still awaited before return. Concurrent calls make
    /// independent acquisitions. The lease is valid only within its ownership
    /// period; do not retain it beyond this closure. This helper does not stop
    /// work after lease loss; enforce the exposed fence at the protected resource.
    /// The async operation and its result must be `Sendable` so values can cross
    /// executor boundaries safely.
    public func withLease<Value: Sendable>(
        _ policy: AcquireOptions? = nil,
        operation: @Sendable (EtcdLease) async throws -> Value
    ) async throws -> Value {
        let lease = try await acquire(policy)
        return try await withLeaseScope(lease, cleanup: { lease in
            await lease.releaseForScope()
        }, operation: operation)
    }
}

public struct EtcdFence: Sendable, Equatable {
    public let epoch: String
    /// Decimal text preserves the full unsigned 64-bit conformance/wire value.
    public let createRevision: String
}

public actor EtcdLease: Lease {
    public enum State: Sendable, Equatable { case waiting, held, releasing, released, lost, releasedUnknown }
    fileprivate let identity = UUID()
    private let provider: EtcdProvider
    private let leaseID: Int64
    private let effectiveLease: Duration
    private let endpoint: URL
    private let epoch: String
    private let cleanupTimeout: Duration
    private let rpcTimeout: Duration
    private var opaqueKey = Data()
    private var revision: Int64 = 0
    private var expiry: ContinuousClock.Instant
    private var maximumExpiry: ContinuousClock.Instant?
    private var currentState: State = .waiting
    private var keeper: Task<Void, Never>?
    private var expiryMonitor: Task<Void, Never>?
    private var lossCleanup: Task<Bool, Never>?
    private let releaseOperation = EtcdReleaseOperation()
    private var keepAliveSendTimes: [ContinuousClock.Instant] = []
    private var hasConfirmedKeepAlive = false

    fileprivate init(
        provider: EtcdProvider,
        leaseID: Int64,
        effectiveLease: Duration,
        deadline: ContinuousClock.Instant,
        endpoint: URL,
        epoch: String,
        cleanupTimeout: Duration,
        rpcTimeout: Duration
    ) {
        self.provider = provider; self.leaseID = leaseID; self.effectiveLease = effectiveLease
        self.expiry = deadline
        self.endpoint = endpoint
        self.epoch = epoch
        self.cleanupTimeout = cleanupTimeout
        self.rpcTimeout = rpcTimeout
    }

    public var state: State { currentState }
    /// Server-granted lease TTL in milliseconds, rounded by etcd to whole seconds.
    public var effectiveLeaseMilliseconds: Int64 {
        let parts = effectiveLease.components
        return parts.seconds * 1_000 + parts.attoseconds / 1_000_000_000_000_000
    }
    public var fence: EtcdFence? {
        guard revision > 0 else { return nil }
        return EtcdFence(epoch: epoch, createRevision: String(revision))
    }
    fileprivate var isValid: Bool { (currentState == .waiting || currentState == .held) && ContinuousClock.now < expiry }
    fileprivate var remainingValidity: Duration { max(.zero, expiry - ContinuousClock.now) }

    fileprivate func startKeepAlive(rpcTimeout: Duration) async throws -> Bool {
        guard keeper == nil else { return true }
        self.currentState = .waiting
        let id = leaseID
        let endpoint = self.endpoint
        let components = effectiveLease.components
        let effectiveMillis = max(1, components.seconds * 1_000 + components.attoseconds / 1_000_000_000_000_000)
        let interval = max(200, effectiveMillis / 3)
        let transport = try EtcdProvider.transport(endpoint)
        let task: Task<Void, Never> = Task { [weak self] in
            do {
                try await withGRPCClient(transport: transport) {
                    (client: GRPCClient<HTTP2ClientTransport.Posix>) async throws -> Void in
                    let lease = Etcdserverpb_Lease.Client(wrapping: client)
                    try await lease.leaseKeepAlive(
                        requestProducer: { [weak self] writer in
                            while await self?.maySendKeepAlive() == true {
                                var message = Etcdserverpb_LeaseKeepAliveRequest()
                                message.id = id
                                await self?.recordKeepAliveSend(at: ContinuousClock.now)
                                try await writer.write(message)
                                try await Task.sleep(for: .milliseconds(interval))
                            }
                        },
                        onResponse: { [weak self] response in
                            for try await response in response.messages {
                                guard let self else { return }
                                guard let sentAt = await self.takeKeepAliveSendTime() else {
                                    await self.markLost()
                                    return
                                }
                                await self.observeKeepAlive(ttlSeconds: response.ttl, sentAt: sentAt)
                            }
                        }
                    )
                }
                await self?.markLost()
            } catch { await self?.markLost() }
        }
        keeper = task
        expiryMonitor = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: .milliseconds(50)) } catch { return }
                await self?.expireIfNeeded()
            }
        }
        let readinessDeadline = ContinuousClock.now + rpcTimeout
        while ContinuousClock.now < readinessDeadline {
            try Task.checkCancellation()
            if hasKeepAliveAcknowledgement() { return true }
            if isTerminal() { return false }
            try await Task.sleep(for: .milliseconds(10))
        }
        markLost()
        return false
    }

    fileprivate func markAcquired(key: Data, revision: Int64, maximumLifetime: Duration) -> Bool {
        guard currentState == .waiting, revision > 0, ContinuousClock.now < expiry else { return false }
        opaqueKey = key
        self.revision = revision
        maximumExpiry = ContinuousClock.now + maximumLifetime
        currentState = .held
        return true
    }

    private func recordKeepAliveSend(at instant: ContinuousClock.Instant) {
        keepAliveSendTimes.append(instant)
    }

    private func takeKeepAliveSendTime() -> ContinuousClock.Instant? {
        guard !keepAliveSendTimes.isEmpty else { return nil }
        return keepAliveSendTimes.removeFirst()
    }

    private func observeKeepAlive(ttlSeconds: Int64, sentAt: ContinuousClock.Instant) {
        guard currentState == .waiting || currentState == .held else { return }
        guard let renewedExpiry = EtcdProvider.renewedLeaseDeadline(
            previousDeadline: expiry,
            sentAt: sentAt,
            ttlSeconds: ttlSeconds,
            now: ContinuousClock.now
        ) else { markLost(); return }
        expiry = renewedExpiry
        hasConfirmedKeepAlive = true
    }

    private func markLost() {
        guard currentState == .waiting || currentState == .held else { return }
        currentState = .lost
        keeper?.cancel()
        expiryMonitor?.cancel(); expiryMonitor = nil
        let provider = provider
        let leaseID = leaseID
        let cleanup = cleanupTimeout
        lossCleanup = Task { await provider.revoke(leaseID: leaseID, cleanup: cleanup) }
    }

    private func hasKeepAliveAcknowledgement() -> Bool { hasConfirmedKeepAlive && currentState != .lost }
    private func isTerminal() -> Bool { currentState == .lost || currentState == .released || currentState == .releasedUnknown }
    private func maySendKeepAlive() -> Bool { currentState == .waiting || currentState == .held }
    private func expireIfNeeded() {
        let now = ContinuousClock.now
        if let oldestUnacknowledged = keepAliveSendTimes.first,
           now - oldestUnacknowledged >= rpcTimeout {
            markLost()
            return
        }
        if (currentState == .waiting || currentState == .held), now >= expiry { markLost(); return }
        if currentState == .held, let maximumExpiry, now >= maximumExpiry { markLost() }
    }

    fileprivate func revokeBounded(cleanup: Duration) async -> Bool {
        let stream = stopKeepalive()
        await stream?.value
        if let lossCleanup {
            let confirmed = await lossCleanup.value
            self.lossCleanup = nil
            if confirmed { return true }
        }
        return await provider.revoke(leaseID: leaseID, cleanup: cleanup)
    }

    fileprivate func unlockAndRevoke(key: Data, cleanup: Duration) async -> ReleaseOutcome {
        let stream = stopKeepalive()
        await stream?.value
        return await provider.unlockAndRevoke(leaseID: leaseID, key: key, cleanup: cleanup)
    }

    public func release() async -> ReleaseOutcome {
        if currentState == .releasing {
            return await releaseOperation.perform { [weak self] in
                await self?.performRelease() ?? .unknown
            }
        }
        guard currentState == .held || currentState == .lost else {
            return currentState == .released ? .alreadyNotOwned : .unknown
        }
        currentState = .releasing
        return await releaseOperation.perform { [weak self] in
            await self?.performRelease() ?? .unknown
        }
    }

    /// Captures local ownership validity before release can enter another actor
    /// turn. A revoke acknowledgement is accepted as scope success only when the
    /// lease was still held and unexpired at this boundary.
    fileprivate func releaseForScope() async -> (
        outcome: ReleaseOutcome,
        ownershipWasValid: Bool
    ) {
        let wasValid = currentState == .held && ContinuousClock.now < expiry
        if currentState == .held && !wasValid { markLost() }
        let outcome = await release()
        return (outcome, wasValid)
    }

    private func performRelease() async -> ReleaseOutcome {
        let result: ReleaseOutcome
        let stream = stopKeepalive()
        await stream?.value
        if let lossCleanup {
            let confirmed = await lossCleanup.value
            self.lossCleanup = nil
            if confirmed {
                // The loss cleanup confirms only this lease was revoked; it
                // does not establish that Unlock released an owned lock key.
                result = .alreadyNotOwned
            } else {
                result = await provider.unlockAndRevoke(
                    leaseID: leaseID,
                    key: opaqueKey,
                    cleanup: cleanupTimeout
                )
            }
        } else {
            result = await provider.unlockAndRevoke(
                leaseID: leaseID,
                key: opaqueKey,
                cleanup: cleanupTimeout
            )
        }
        switch result {
        case .released, .alreadyNotOwned: currentState = .released
        case .unknown: currentState = .releasedUnknown
        }
        await provider.forgetLease(identity)
        return result
    }

    private func stopKeepalive() -> Task<Void, Never>? {
        let stream = keeper
        stream?.cancel(); keeper = nil
        expiryMonitor?.cancel(); expiryMonitor = nil
        return stream
    }
}
