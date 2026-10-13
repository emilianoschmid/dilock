/// A typed failure. Contention is represented only by `nil` from `tryAcquire`.
public enum DilockError: Error, Sendable, Equatable {
    case invalidConfiguration(String)
    case backendUnavailable(String)
    case protocolMismatch(String)
    case timedOut
    case acquisitionOutcomeUnknown(String)
    case unsupportedCapability(String)
    case leaseLost
    case releaseUnknown
}

/// Finite, validated budgets shared by backend adapters.
public struct AcquireOptions: Sendable, Equatable {
    public let wait: Duration
    public let lease: Duration

    public init(wait: Duration = .seconds(5), lease: Duration = .seconds(30)) throws {
        guard wait > .zero, lease > .zero else {
            throw DilockError.invalidConfiguration("wait and lease durations must be positive")
        }
        self.wait = wait
        self.lease = lease
    }
}

/// Result of explicit, awaited cleanup.
public enum ReleaseOutcome: Sendable, Equatable {
    case released
    case alreadyNotOwned
    case unknown
}

/// A lease never performs asynchronous work during deinitialization.
public protocol Lease: Sendable {
    func release() async -> ReleaseOutcome
}

/// Runs an async operation while an acquired lease is in scope, then awaits
/// owner-checked cleanup before returning. If the operation throws, that error
/// remains primary even when cleanup is uncertain. After a successful operation,
/// unknown cleanup is reported as `DilockError.releaseUnknown`. By default,
/// `.alreadyNotOwned` is reported as `DilockError.leaseLost`, even when the
/// backend confirms terminal cleanup. Cleanup acknowledgement cannot prove
/// that the protected work remained owned throughout the operation. An adapter
/// may supply a cleanup closure that reports local ownership validity at exit.
///
/// Cancellation is cooperative: the operation may observe task cancellation,
/// but cleanup is still explicitly awaited before this function returns. The
/// helper does not renew a lease or stop work when ownership is lost; callers
/// must keep protected work within the lease's validity and enforce fencing
/// where the backend provides it.
/// The operation and its result must be `Sendable` so values can cross executor
/// boundaries safely.
public func withLeaseScope<L: Lease, Value: Sendable>(
    _ lease: L,
    cleanup: (@Sendable (L) async -> (
        outcome: ReleaseOutcome,
        ownershipWasValid: Bool
    ))? = nil,
    operation: @Sendable (L) async throws -> Value
) async throws -> Value {
    func release() async -> (
        outcome: ReleaseOutcome,
        ownershipWasValid: Bool
    ) {
        if let cleanup { return await cleanup(lease) }
        return (await lease.release(), true)
    }

    let value: Value
    do {
        value = try await operation(lease)
    } catch {
        _ = await release()
        throw error
    }

    let result = await release()
    switch result.outcome {
    case .released:
        guard result.ownershipWasValid else { throw DilockError.leaseLost }
        return value
    case .alreadyNotOwned:
        throw DilockError.leaseLost
    case .unknown:
        throw DilockError.releaseUnknown
    }
}
