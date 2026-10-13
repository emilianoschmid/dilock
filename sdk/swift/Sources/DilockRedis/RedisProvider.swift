import DilockCore
import Foundation
import NIOCore
@preconcurrency import RediStack

actor RedisReleaseOperation {
    private var task: Task<ReleaseOutcome, Never>?

    func perform(_ operation: @escaping @Sendable () async -> ReleaseOutcome) async -> ReleaseOutcome {
        if let task { return await task.value }
        let task = Task { await operation() }
        self.task = task
        return await task.value
    }
}

/// Serializes owner-mutating commands for one lease generation. Actor methods
/// may reenter while awaiting I/O, so the lease actor alone is not a mutation
/// ordering guarantee.
actor RedisLeaseOperation {
    private var predecessor: Task<Void, Never>?

    func perform<Value: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        let previous = predecessor
        let task = Task.detached {
            await previous?.value
            return try await operation()
        }
        predecessor = Task.detached { _ = await task.result }
        return try await task.value
    }
}

struct RedisCommandTimedOut: Error, Sendable {}

actor RedisCommandRace<Value: Sendable> {
    private var continuation: CheckedContinuation<Value, Error>?
    private var completed: Result<Value, Error>?
    private var timeoutTask: Task<Void, Never>?

    func run(
        timeout: Duration,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                if let completed {
                    continuation.resume(with: completed)
                    return
                }
                self.continuation = continuation
                Task {
                    do { finish(.success(try await operation())) }
                    catch { finish(.failure(error)) }
                }
                timeoutTask = Task {
                    try? await Task.sleep(for: timeout)
                    finish(.failure(RedisCommandTimedOut()))
                }
            }
        } onCancel: {
            Task { await self.finish(.failure(CancellationError())) }
        }
    }

    private func finish(_ result: Result<Value, Error>) {
        guard completed == nil else { return }
        completed = result
        timeoutTask?.cancel()
        timeoutTask = nil
        continuation?.resume(with: result)
        continuation = nil
    }
}

/// A reusable provider around a caller-configured RediStack connection.
/// The caller owns connection creation and shutdown; sharing this provider shares that
/// connection. This provider bounds each command by `rpcTimeout`; callers should also
/// configure bounded connection establishment and disable client retries. A local
/// command timeout does not cancel a command already queued to Redis.
public actor RedisProvider {
    private let connection: RedisConnection
    private let cleanupConnection: RedisConnection
    private let namespace: String
    private let epoch: String
    private let defaults: AcquireOptions
    private let rpcTimeout: Duration
    private let cleanupTimeout: Duration
    private var isClosed = false
    private var ownsConnections = false
    private var connectionCloseTask: Task<Void, Error>?

    /// Validates pre-provisioned namespace metadata. This initializer never writes Redis.
    /// Each command is locally bounded by `rpcTimeout`; cleanup uses its separate
    /// `cleanupTimeout`. A local timeout cannot cancel a command already queued to Redis.
    /// - Parameters:
    ///   - rpcTimeout: Maximum local wait for a Redis command, defaulting to two seconds.
    ///   - cleanupTimeout: Maximum local wait for owner-specific cleanup, defaulting to two seconds.
    public init(
        connection: RedisConnection,
        cleanupConnection: RedisConnection,
        namespace: String = "default",
        epoch: String,
        defaults: AcquireOptions = try! AcquireOptions(),
        rpcTimeout: Duration = .seconds(2),
        cleanupTimeout: Duration = .seconds(2)
    ) async throws {
        _ = try Identity.resourceID(namespace: namespace, resource: "validation")
        guard !epoch.isEmpty, epoch.utf8.count <= 1024 else {
            throw DilockError.invalidConfiguration("epoch must contain 1..1024 UTF-8 bytes")
        }
        guard rpcTimeout > .zero, cleanupTimeout > .zero else {
            throw DilockError.invalidConfiguration("Redis RPC and cleanup timeouts must be positive")
        }
        _ = try Self.milliseconds(rpcTimeout, maximum: 86_400_000)
        _ = try Self.milliseconds(cleanupTimeout, maximum: 86_400_000)
        self.connection = connection
        self.cleanupConnection = cleanupConnection
        self.namespace = namespace
        self.epoch = epoch
        self.defaults = defaults
        self.rpcTimeout = rpcTimeout
        self.cleanupTimeout = cleanupTimeout
        let raw = try await send(connection, "GET", ["dilock:namespace:\(namespace):meta"], timeout: rpcTimeout)
        guard let text = raw, text.utf8.count <= 8192, let data = text.data(using: .utf8),
              let metadata = try? JSONDecoder().decode(Metadata.self, from: data),
              metadata.protocolMajor == 1,
              metadata.identityScheme == "sha256-framed-utf8-v1",
              metadata.profile == "redis-single-authority-v1",
              metadata.coordinationEpoch == epoch,
              metadata.fixedLeaseMs == nil else {
            throw DilockError.protocolMismatch("Redis namespace metadata is absent or incompatible")
        }
    }

    /// Opens two independent RediStack connections and validates namespace metadata.
    /// `rpcTimeout` bounds command waits and must leave a safety margin within each
    /// requested lease; `cleanupTimeout` bounds owner-specific cleanup. A timed-out
    /// command may still complete remotely, so a dispatched acquisition reports an
    /// unknown outcome after attempting cleanup.
    ///
    /// The provider owns and closes both connections when `close()` is awaited. The
    /// caller continues to own `eventLoopGroup` and must shut it down after closing
    /// the provider. Connection establishment and metadata requests are not
    /// interrupted mid-flight by task cancellation; cancellation is checked
    /// between stages and any opened connections are closed before returning the
    /// cancellation error. Connection establishment uses RediStack's default TCP
    /// bootstrap and is outside the acquisition wait budget; this factory does not
    /// expose a custom bootstrap. Commands are locally bounded by `rpcTimeout`, but
    /// a timed-out command may still complete remotely. RediStack's `Configuration` supports `redis://`
    /// endpoints; TLS or custom transport policy requires caller-built connections
    /// and the initializer above.
    public static func connect(
        configuration: RedisConnection.Configuration,
        on eventLoopGroup: any EventLoopGroup,
        namespace: String = "default",
        epoch: String,
        defaults: AcquireOptions = try! AcquireOptions(),
        rpcTimeout: Duration = .seconds(2),
        cleanupTimeout: Duration = .seconds(2)
    ) async throws -> RedisProvider {
        try Task.checkCancellation()
        let connection = try await RedisConnection.make(
            configuration: configuration,
            boundEventLoop: eventLoopGroup.next()
        ).get()
        do {
            try Task.checkCancellation()
            let cleanupConnection = try await RedisConnection.make(
                configuration: configuration,
                boundEventLoop: eventLoopGroup.next()
            ).get()
            do {
                try Task.checkCancellation()
                let provider = try await RedisProvider(
                    connection: connection,
                    cleanupConnection: cleanupConnection,
                    namespace: namespace,
                    epoch: epoch,
                    defaults: defaults,
                    rpcTimeout: rpcTimeout,
                    cleanupTimeout: cleanupTimeout
                )
                try Task.checkCancellation()
                await provider.takeConnectionOwnership()
                return provider
            } catch {
                try? await cleanupConnection.close().get()
                throw error
            }
        } catch {
            try? await connection.close().get()
            throw error
        }
    }

    private func takeConnectionOwnership() { ownsConnections = true }

    public func lock(_ resource: String) throws -> RedisLock {
        guard !isClosed else { throw DilockError.invalidConfiguration("provider is closed") }
        _ = try Identity.resourceID(namespace: namespace, resource: resource)
        return RedisLock(provider: self, resource: resource, defaults: defaults)
    }

    /// Stops future work and awaits shutdown of both connections created by
    /// `connect`. Caller-provided connections remain owned by the caller. The
    /// event-loop group is never shut down by this provider. This preserves the
    /// existing non-throwing close API; use `closeAndReport()` to observe shutdown
    /// errors. Calling `close()` more than once is safe.
    public func close() async { try? await closeAndReport() }

    /// Stops future work and reports owned-connection shutdown failures.
    /// Caller-provided connections are never closed. The event-loop group remains
    /// caller-owned and must be shut down after this method returns.
    public func closeAndReport() async throws {
        if isClosed {
            if let connectionCloseTask { try await connectionCloseTask.value }
            return
        }
        isClosed = true
        guard ownsConnections else { return }

        let connection = self.connection
        let cleanupConnection = self.cleanupConnection
        let closeTask = Task {
            let connectionClose = Task { try await connection.close().get() }
            let cleanupClose = Task { try await cleanupConnection.close().get() }
            var closeFailed = false
            do { try await connectionClose.value } catch { closeFailed = true }
            do { try await cleanupClose.value } catch { closeFailed = true }
            if closeFailed {
                throw DilockError.backendUnavailable("one or more owned Redis connections failed to close")
            }
        }
        connectionCloseTask = closeTask
        try await closeTask.value
    }

    fileprivate func tryAcquire(resource: String, lease: Duration, commandBudget: Duration? = nil) async throws -> RedisLease? {
        guard !isClosed else { throw DilockError.invalidConfiguration("provider is closed") }
        let ttl = try Self.milliseconds(lease, maximum: 86_400_000)
        let rpcMillis = try Self.milliseconds(rpcTimeout, maximum: 86_400_000)
        guard rpcMillis + max(10, ttl / 100) < ttl else {
            throw DilockError.invalidConfiguration("Redis RPC timeout plus lease safety margin must be less than the lease")
        }
        let id = try Identity.resourceID(namespace: namespace, resource: resource)
        let key = Identity.redisKey(resourceID: id)
        try Task.checkCancellation()
        let requestBudget = min(rpcTimeout, commandBudget ?? rpcTimeout)
        guard requestBudget > .zero else { throw DilockError.timedOut }
        let owner = Self.newOwner()
        let started = ContinuousClock.now
        let requestDeadline = started + requestBudget
        let result: String?
        do { result = try await send(connection, "SET", [key, owner, "NX", "PX", String(ttl)], timeout: requestBudget) }
        catch is CancellationError {
            await cleanup(key: key, owner: owner)
            throw DilockError.acquisitionOutcomeUnknown("acquisition was cancelled after command dispatch; owner-specific cleanup was attempted")
        } catch {
            await cleanup(key: key, owner: owner)
            throw DilockError.acquisitionOutcomeUnknown("Redis did not acknowledge SET; owner-specific cleanup was attempted")
        }
        if Task.isCancelled {
            await cleanup(key: key, owner: owner)
            throw DilockError.acquisitionOutcomeUnknown("acquisition was cancelled after SET; owner-specific cleanup was attempted")
        }
        if try Self.isContendedSetReply(result) {
            let typeBudget = min(rpcTimeout, requestDeadline - ContinuousClock.now)
            guard typeBudget > .zero else { throw DilockError.timedOut }
            let type: String?
            do {
                type = try await send(connection, "TYPE", [key], timeout: typeBudget)
            } catch is CancellationError {
                throw CancellationError()
            } catch is RedisCommandTimedOut {
                throw Self.typeTimeoutFailure(waitDeadlineReached: ContinuousClock.now >= requestDeadline)
            } catch let error as DilockError {
                throw error
            } catch {
                throw DilockError.backendUnavailable("Redis TYPE failed while classifying a contended lock key")
            }
            try Self.validateContentionType(type)
            return nil
        }
        let margin = max(10, ttl / 100)
        let remaining = ttl - margin
        guard remaining > 0, ContinuousClock.now - started < .milliseconds(remaining) else {
            await cleanup(key: key, owner: owner)
            throw DilockError.acquisitionOutcomeUnknown("Redis confirmed SET after the conservative lease budget")
        }
        return RedisLease(provider: self, key: key, owner: owner, deadline: started + .milliseconds(remaining))
    }

    fileprivate func release(key: String, owner: String) async -> ReleaseOutcome {
        do {
            let result = try await send(cleanupConnection, "EVAL", [Self.releaseScript, "1", key, owner], timeout: cleanupTimeout)
            return result == "1" ? .released : .alreadyNotOwned
        } catch { return .unknown }
    }

    fileprivate func renew(key: String, owner: String, lease: Duration, remainingLease: Duration) async throws -> ContinuousClock.Instant {
        let ttl = try Self.milliseconds(lease, maximum: 86_400_000)
        let rpcMillis = try Self.milliseconds(rpcTimeout, maximum: 86_400_000)
        guard rpcMillis + max(10, ttl / 100) < ttl else {
            throw DilockError.invalidConfiguration("Redis RPC timeout plus lease safety margin must be less than the lease")
        }
        let requestBudget = min(rpcTimeout, remainingLease)
        guard requestBudget > .zero else { throw DilockError.leaseLost }
        let started = ContinuousClock.now
        do {
            let result = try await send(connection, "EVAL", [Self.renewScript, "1", key, owner, String(ttl)], timeout: requestBudget)
            guard result == "1" else { throw DilockError.leaseLost }
        } catch let error as DilockError { throw error }
        catch { throw DilockError.backendUnavailable("Redis renewal was not acknowledged") }
        let remaining = ttl - max(10, ttl / 100)
        guard remaining > 0, ContinuousClock.now - started < .milliseconds(remaining) else {
            throw DilockError.leaseLost
        }
        return started + .milliseconds(remaining)
    }

    private func cleanup(key: String, owner: String) async {
        // Cleanup must retain its own bounded budget after the acquisition task is
        // cancelled; an unstructured task does not inherit that cancellation.
        let cleanupTask = Task {
            _ = try? await send(cleanupConnection, "EVAL", [Self.releaseScript, "1", key, owner], timeout: cleanupTimeout)
        }
        await cleanupTask.value
    }

    private func send(_ connection: RedisConnection, _ command: String, _ values: [String], timeout: Duration) async throws -> String? {
        let allocator = ByteBufferAllocator()
        let args = values.map { value in RESPValue.bulkString(allocator.buffer(string: value)) }
        let future = connection.send(command: command, with: args)
        let race = RedisCommandRace<String?>()
        return try await race.run(timeout: timeout) {
            let response = try await future.get()
            switch response {
            case .simpleString, .bulkString:
                guard let string = response.string else {
                    throw DilockError.protocolMismatch("Redis returned an invalid string response")
                }
                return string
            case .integer(let number): return String(number)
            case .null: return nil
            case .error(let error): throw DilockError.backendUnavailable(error.message)
            case .array: throw DilockError.protocolMismatch("unexpected Redis array response")
            }
        }
    }

    static func isContendedSetReply(_ result: String?) throws -> Bool {
        if result == nil { return true }
        guard result == "OK" else {
            throw DilockError.protocolMismatch("Redis returned an unexpected SET acknowledgement")
        }
        return false
    }

    static func validateContentionType(_ type: String?) throws {
        guard type == "string" || type == "none" else {
            throw DilockError.protocolMismatch("lock key has Redis type \(type ?? "unknown")")
        }
    }

    static func typeTimeoutFailure(waitDeadlineReached: Bool) -> DilockError {
        waitDeadlineReached ? .timedOut : .backendUnavailable("Redis TYPE exceeded its RPC timeout")
    }

    private static func milliseconds(_ duration: Duration, maximum: Int64) throws -> Int64 {
        let components = duration.components
        let ns = components.seconds.multipliedReportingOverflow(by: 1_000_000_000)
        guard !ns.overflow else { throw DilockError.invalidConfiguration("duration overflows") }
        let total = ns.partialValue.addingReportingOverflow(components.attoseconds / 1_000_000_000)
        guard !total.overflow, total.partialValue > 0 else { throw DilockError.invalidConfiguration("duration must be positive") }
        let rounded = total.partialValue.addingReportingOverflow(999_999)
        guard !rounded.overflow else { throw DilockError.invalidConfiguration("duration overflows") }
        let ms = rounded.partialValue / 1_000_000
        guard ms <= maximum else { throw DilockError.invalidConfiguration("duration exceeds 86400000 ms") }
        return ms
    }

    private static func newOwner() -> String {
        var rng = SystemRandomNumberGenerator()
        let bytes = (0..<32).map { _ in UInt8.random(in: .min ... .max, using: &rng) }
        return "dilock/1/" + bytes.map { String(format: "%02x", $0) }.joined()
    }

    private static let releaseScript = String(decoding: try! Data(contentsOf: Bundle.module.url(forResource: "redis-release", withExtension: "lua")!), as: UTF8.self)
    private static let renewScript = String(decoding: try! Data(contentsOf: Bundle.module.url(forResource: "redis-renew", withExtension: "lua")!), as: UTF8.self)

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

public struct RedisLock: Sendable {
    fileprivate let provider: RedisProvider
    public let resource: String
    private let defaults: AcquireOptions

    fileprivate init(provider: RedisProvider, resource: String, defaults: AcquireOptions) {
        self.provider = provider; self.resource = resource; self.defaults = defaults
    }

    /// Attempts one acquisition using `options.lease`; the wait budget is ignored.
    public func tryAcquire(_ options: AcquireOptions? = nil) async throws -> RedisLease? {
        try await provider.tryAcquire(resource: resource, lease: (options ?? defaults).lease)
    }

    public func acquire(_ options: AcquireOptions? = nil) async throws -> RedisLease {
        let policy = options ?? defaults
        let clock = ContinuousClock()
        let deadline = clock.now + policy.wait
        while true {
            try Task.checkCancellation()
            let remaining = deadline - clock.now
            guard remaining > .zero else { throw DilockError.timedOut }
            if let lease = try await provider.tryAcquire(resource: resource, lease: policy.lease, commandBudget: remaining) {
                return try await Self.acceptAcquiredLease(
                    lease,
                    remaining: deadline - clock.now
                ) { await $0.release() }
            }
            let remainingAfterAttempt = deadline - clock.now
            guard remainingAfterAttempt > .zero else { throw DilockError.timedOut }
            let sample = Int64.random(in: 15...50)
            try await Task.sleep(for: Self.contentionDelay(remaining: remainingAfterAttempt, sampleMilliseconds: sample))
        }
    }

    /// Acquires this resource, invokes `operation` once, and awaits release
    /// before returning. `options.wait` bounds acquisition; the handle defaults
    /// apply when options are omitted. Acquisition and operation errors propagate.
    /// If the operation throws, its error remains primary even if cleanup is
    /// uncertain. After success, loss of the Redis owner token throws
    /// `DilockError.leaseLost`, and unknown cleanup throws
    /// `DilockError.releaseUnknown`.
    ///
    /// Cancellation is cooperative: the operation can observe task cancellation,
    /// and release is still awaited before return. Concurrent calls make
    /// independent acquisitions. The lease is valid only within its ownership
    /// period; do not retain it beyond this closure. This helper does not renew
    /// the lease or stop work after lease loss.
    /// The async operation and its result must be `Sendable` so values can cross
    /// executor boundaries safely.
    public func withLease<Value: Sendable>(
        _ options: AcquireOptions? = nil,
        operation: @Sendable (RedisLease) async throws -> Value
    ) async throws -> Value {
        let lease = try await acquire(options)
        return try await withLeaseScope(lease, cleanup: { lease in
            await lease.releaseForScope()
        }, operation: operation)
    }

    static func acceptAcquiredLease<Value>(
        _ lease: Value,
        remaining: Duration,
        release: (Value) async -> ReleaseOutcome
    ) async throws -> Value {
        guard remaining > .zero else {
            switch await release(lease) {
            case .unknown:
                throw DilockError.acquisitionOutcomeUnknown(
                    "Redis acquired after the wait deadline and owner cleanup was not acknowledged"
                )
            case .released, .alreadyNotOwned:
                throw DilockError.timedOut
            }
        }
        return lease
    }

    static func contentionDelay(remaining: Duration, sampleMilliseconds: Int64) -> Duration {
        min(.milliseconds(min(50, max(15, sampleMilliseconds))), remaining)
    }
}

public actor RedisLease: Lease {
    public enum State: Sendable, Equatable { case held, releasing, released, lost, releasedUnknown }
    private let provider: RedisProvider
    private let key: String
    private let owner: String
    private let releaseOperation = RedisReleaseOperation()
    private let lifecycleOperation = RedisLeaseOperation()
    private var expiresAt: ContinuousClock.Instant
    private var currentState: State = .held

    fileprivate init(provider: RedisProvider, key: String, owner: String, deadline: ContinuousClock.Instant) {
        self.provider = provider; self.key = key; self.owner = owner; self.expiresAt = deadline
    }

    public var state: State { currentState }

    /// Extends this acquisition when Redis confirms the owner token.
    /// Renewal and release commands are serialized for this lease. If release
    /// begins while renewal is in flight, renewal does not restore the held state.
    public func renew(for duration: Duration) async throws {
        guard currentState == .held, ContinuousClock.now < expiresAt else {
            currentState = .lost
            throw DilockError.leaseLost
        }
        do {
            let newDeadline = try await lifecycleOperation.perform { [self, provider, key, owner] in
                guard let remaining = await renewalBudget() else {
                    throw DilockError.leaseLost
                }
                return try await provider.renew(
                    key: key,
                    owner: owner,
                    lease: duration,
                    remainingLease: remaining
                )
            }
            guard currentState == .held else { throw DilockError.leaseLost }
            expiresAt = newDeadline
        } catch {
            // Preserve ownership for local argument validation failures. Once a
            // renewal may have reached Redis, fail closed and retain explicit
            // owner-token-checked cleanup through release().
            currentState = Self.stateAfterRenewalFailure(currentState, error: error)
            throw error
        }
    }

    private func renewalBudget() -> Duration? {
        guard currentState == .held, ContinuousClock.now < expiresAt else { return nil }
        return expiresAt - ContinuousClock.now
    }

    static func stateAfterRenewalFailure(_ state: State, error: Error) -> State {
        guard state == .held else { return state }
        if let error = error as? DilockError,
           case .invalidConfiguration = error {
            // Provider validation runs before dispatching EVAL, so the previous
            // lease and its locally tracked expiry remain valid.
            return .held
        }
        return .lost
    }

    /// Releases this owner with a bounded cleanup command. Concurrent lifecycle
    /// mutations are ordered; repeated release calls share one cleanup result.
    public func release() async -> ReleaseOutcome {
        if currentState == .releasing {
            return await releaseOperation.perform { [weak self, provider, key, owner] in
                await self?.completeRelease(provider: provider, key: key, owner: owner) ?? .unknown
            }
        }
        guard currentState == .held || currentState == .lost else {
            return currentState == .released ? .alreadyNotOwned : .unknown
        }
        currentState = .releasing
        let lease = self
        let outcome = await releaseOperation.perform { [lease, provider, key, owner, lifecycleOperation] in
            (try? await lifecycleOperation.perform {
                await lease.completeRelease(provider: provider, key: key, owner: owner)
            }) ?? .unknown
        }
        return outcome
    }

    /// Captures the locally tracked lease validity before release can suspend.
    /// A successful owner-token cleanup does not make work performed after the
    /// conservative local deadline valid again.
    fileprivate func releaseForScope() async -> (
        outcome: ReleaseOutcome,
        ownershipWasValid: Bool
    ) {
        let wasValid = currentState == .held && ContinuousClock.now < expiresAt
        if currentState == .held && !wasValid { currentState = .lost }
        let outcome = await release()
        return (outcome, wasValid)
    }

    private func completeRelease(provider: RedisProvider, key: String, owner: String) async -> ReleaseOutcome {
        let outcome = await provider.release(key: key, owner: owner)
        switch outcome {
        case .released, .alreadyNotOwned: currentState = .released
        case .unknown: currentState = .releasedUnknown
        }
        return outcome
    }
}
