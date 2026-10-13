import Foundation
import Testing
import DilockCore
@testable import DilockEtcd

struct EtcdProviderOptionsTests {
    @Test func concurrentProviderCloseWaitsForSingleCleanup() async throws {
        let operation = EtcdCloseOperation()
        let gate = EtcdCloseGate()
        let first = Task { await operation.run { await gate.cleanup() }.value }
        while await gate.cleanupCount == 0 { try await Task.sleep(for: .milliseconds(1)) }
        first.cancel()

        let second = Task {
            await operation.run { await gate.unexpectedSecondCleanup() }.value
            await gate.markSecondReturned()
        }
        try await Task.sleep(for: .milliseconds(20))
        #expect(await gate.secondReturned == false)

        await gate.finish()
        await first.value
        await second.value
        #expect(await gate.cleanupCount == 1)
        #expect(await gate.secondReturned)
    }

    @Test func concurrentReleaseWaitsForTheSingleCleanupResult() async throws {
        let operation = EtcdReleaseOperation()
        let gate = EtcdReleaseGate()
        let first = Task { await operation.perform { await gate.release() } }
        while await gate.dispatchCount == 0 { try await Task.sleep(for: .milliseconds(1)) }
        let second = Task { await operation.perform { await gate.release() } }
        try await Task.sleep(for: .milliseconds(20))
        await gate.finish(.released)

        let firstResult = await first.value
        let secondResult = await second.value
        #expect(firstResult == .released)
        #expect(secondResult == .released)
        #expect(await gate.dispatchCount == 1)
        #expect(await gate.state == "released")
    }

    @Test func acceptsPlaintextAuthorityAndStableEpoch() throws {
        let options = try EtcdProviderOptions(
            endpoint: URL(string: "http://127.0.0.1:2379")!,
            epoch: "cluster-generation-4"
        )
        #expect(options.namespace == "default")
        #expect(options.epoch == "cluster-generation-4")
        #expect(options.rpcTimeout == .seconds(2))
    }

    @Test func rejectsEndpointCredentialsAndPaths() {
        #expect(throws: DilockError.self) {
            try EtcdProviderOptions(endpoint: URL(string: "http://user:pass@localhost:2379")!, epoch: "e1")
        }
        #expect(throws: DilockError.self) {
            try EtcdProviderOptions(endpoint: URL(string: "http://localhost:2379/v3")!, epoch: "e1")
        }
        #expect(throws: DilockError.self) {
            try EtcdProviderOptions(endpoint: URL(string: "http://localhost:2379")!, epoch: "")
        }
    }

    @Test func verificationReturningAfterWaitDeadlineCleansBeforeTimeout() {
        #expect(EtcdProvider.waitDeadlineFailure(
            remaining: .zero,
            cleanupOutcome: .released
        ) == .timedOut)
        #expect(EtcdProvider.waitDeadlineFailure(
            remaining: .milliseconds(-1),
            cleanupOutcome: .alreadyNotOwned
        ) == .timedOut)
        guard case .acquisitionOutcomeUnknown? = EtcdProvider.waitDeadlineFailure(
            remaining: .zero,
            cleanupOutcome: .unknown
        ) else {
            Issue.record("late verification with unconfirmed cleanup must remain unknown")
            return
        }
        #expect(EtcdProvider.waitDeadlineFailure(
            remaining: .milliseconds(1),
            cleanupOutcome: .unknown
        ) == nil)
    }

    @Test func cancelledOwnershipVerificationIsKnownOnlyAfterAcknowledgedCleanup() {
        #expect(EtcdProvider.cancellationCleanupWasConfirmed(.released))
        #expect(EtcdProvider.cancellationCleanupWasConfirmed(.alreadyNotOwned))
        #expect(!EtcdProvider.cancellationCleanupWasConfirmed(.unknown))
    }

    @Test func invalidGrantedLeaseIsRevokedWhenItsIdentifierIsUsable() {
        #expect(EtcdProvider.invalidGrantFailure(
            hasLeaseID: true,
            revokeAcknowledged: true,
            detail: "bad ttl"
        ) == .protocolMismatch("bad ttl"))
        guard case .acquisitionOutcomeUnknown = EtcdProvider.invalidGrantFailure(
            hasLeaseID: true,
            revokeAcknowledged: false,
            detail: "bad ttl"
        ) else {
            Issue.record("unconfirmed invalid-lease cleanup must remain unknown")
            return
        }
        guard case .acquisitionOutcomeUnknown = EtcdProvider.invalidGrantFailure(
            hasLeaseID: false,
            revokeAcknowledged: false,
            detail: "missing id"
        ) else {
            Issue.record("a missing lease ID cannot be revoked")
            return
        }
    }

    @Test func releasedRequiresExactOwnershipAndBothCleanupAcknowledgements() {
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .owned,
            unlockAcknowledged: true,
            revokeObservation: .acknowledged
        ) == .released)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .owned,
            unlockAcknowledged: true,
            revokeObservation: .unknown
        ) == nil)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .unknown,
            unlockAcknowledged: true,
            revokeObservation: .unknown
        ) == nil)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .notOwned,
            unlockAcknowledged: true,
            revokeObservation: .unknown
        ) == .alreadyNotOwned)
    }

    @Test func revokeAcknowledgementConfirmsTerminalCleanupWithoutOwnershipClaim() {
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .unknown,
            unlockAcknowledged: false,
            revokeObservation: .acknowledged
        ) == .alreadyNotOwned)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .owned,
            unlockAcknowledged: false,
            revokeObservation: .acknowledged
        ) == .alreadyNotOwned)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .notOwned,
            unlockAcknowledged: false,
            revokeObservation: .acknowledged
        ) == .alreadyNotOwned)
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .owned,
            unlockAcknowledged: false,
            revokeObservation: .unknown
        ) == nil)
    }

    @Test func revokeNotFoundWinsOverUnlockAcknowledgementAfterExpiryRace() {
        #expect(EtcdProvider.classifyReleaseOutcome(
            ownership: .owned,
            unlockAcknowledged: true,
            revokeObservation: .notFound
        ) == .alreadyNotOwned)
    }

    @Test func delayedKeepAliveCannotExtendAnAlreadyExpiredLocalLease() {
        let now = ContinuousClock.now
        let sentAt = now - .seconds(1)
        #expect(EtcdProvider.renewedLeaseDeadline(
            previousDeadline: now - .milliseconds(1),
            sentAt: sentAt,
            ttlSeconds: 10,
            now: now
        ) == nil)

        let renewed = EtcdProvider.renewedLeaseDeadline(
            previousDeadline: now + .seconds(2),
            sentAt: sentAt,
            ttlSeconds: 10,
            now: now
        )
        #expect(renewed.map { $0 > now } == true)
    }
}

private actor EtcdReleaseGate {
    private(set) var dispatchCount = 0
    private(set) var state = "releasing"
    private var continuation: CheckedContinuation<ReleaseOutcome, Never>?

    func release() async -> ReleaseOutcome {
        dispatchCount += 1
        let outcome = await withCheckedContinuation { continuation = $0 }
        state = "released"
        return outcome
    }

    func finish(_ outcome: ReleaseOutcome) {
        continuation?.resume(returning: outcome)
        continuation = nil
    }
}

private actor EtcdCloseGate {
    private(set) var cleanupCount = 0
    private(set) var secondReturned = false
    private var continuation: CheckedContinuation<Void, Never>?

    func cleanup() async {
        cleanupCount += 1
        await withCheckedContinuation { continuation = $0 }
    }

    func unexpectedSecondCleanup() async {
        cleanupCount += 1
    }

    func markSecondReturned() { secondReturned = true }

    func finish() {
        continuation?.resume()
        continuation = nil
    }
}
