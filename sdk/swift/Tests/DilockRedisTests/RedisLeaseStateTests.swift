import DilockCore
import Testing
@testable import DilockRedis

struct RedisLeaseStateTests {
    @Test func redisCommandDeadlineBoundsWaitWithoutClaimingRemoteCancellation() async throws {
        let race = RedisCommandRace<String>()
        do {
            _ = try await race.run(timeout: .milliseconds(10)) {
                try await Task.sleep(for: .seconds(1))
                return "late response"
            }
            Issue.record("a command response after its budget must time out")
        } catch is RedisCommandTimedOut {
            // The remote command may still complete; acquisition handles this as unknown.
        }
    }

    @Test func redisCommandDeadlineReturnsPromptResponse() async throws {
        let race = RedisCommandRace<String>()
        let response = try await race.run(timeout: .seconds(1)) { "OK" }
        #expect(response == "OK")
    }

    @Test func redisContentionVerificationTimeoutPreservesWaitVsRPCClassification() {
        #expect(RedisProvider.typeTimeoutFailure(waitDeadlineReached: true) == .timedOut)
        #expect(RedisProvider.typeTimeoutFailure(waitDeadlineReached: false) == .backendUnavailable("Redis TYPE exceeded its RPC timeout"))
    }

    @Test func contentionBackoffNeverSleepsPastWaitDeadline() {
        #expect(RedisLock.contentionDelay(remaining: .milliseconds(100), sampleMilliseconds: 15) == .milliseconds(15))
        #expect(RedisLock.contentionDelay(remaining: .milliseconds(100), sampleMilliseconds: 50) == .milliseconds(50))
        #expect(RedisLock.contentionDelay(remaining: .milliseconds(7), sampleMilliseconds: 50) == .milliseconds(7))
        #expect(RedisLock.contentionDelay(remaining: .nanoseconds(1), sampleMilliseconds: 15) == .nanoseconds(1))
    }

    @Test func acquisitionReturningAfterWaitDeadlineIsCleanedAndNeverReturned() async throws {
        let calls = ReleaseCallCounter()
        do {
            _ = try await RedisLock.acceptAcquiredLease(
                "late-lease",
                remaining: .zero
            ) { lease in
                #expect(lease == "late-lease")
                return await calls.release(.released)
            }
            Issue.record("late lease must not be returned")
        } catch let error as DilockError {
            #expect(error == .timedOut)
        }
        #expect(await calls.count == 1)
    }

    @Test func lateAcquisitionWithUnconfirmedCleanupIsUnknown() async throws {
        let calls = ReleaseCallCounter()
        do {
            _ = try await RedisLock.acceptAcquiredLease(
                "late-lease",
                remaining: .nanoseconds(-1)
            ) { _ in await calls.release(.unknown) }
            Issue.record("late lease with unknown cleanup must not be returned")
        } catch let error as DilockError {
            #expect(error == .acquisitionOutcomeUnknown(
                "Redis acquired after the wait deadline and owner cleanup was not acknowledged"
            ))
        }
        #expect(await calls.count == 1)
    }

    @Test func concurrentReleaseWaitsForTheSingleCleanupResult() async throws {
        let operation = RedisReleaseOperation()
        let gate = ReleaseGate()
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

    @Test func renewalAndReleaseMutationsAreSerialized() async throws {
        let lifecycle = RedisLeaseOperation()
        let sequence = LifecycleSequence()
        let renewal = Task {
            try await lifecycle.perform { await sequence.renew() }
        }
        while await sequence.events.isEmpty { await Task.yield() }

        let release = Task {
            try await lifecycle.perform { await sequence.release() }
        }
        try await Task.sleep(for: .milliseconds(20))
        #expect(await sequence.events == ["renew-start"])

        await sequence.finishRenewal()
        _ = try await renewal.value
        _ = try await release.value
        #expect(await sequence.events == ["renew-start", "renew-finish", "release"])
    }

    @Test func RedisSetReplyAndKeyTypeDistinguishContentionFromProtocolErrors() throws {
        #expect(try RedisProvider.isContendedSetReply(nil))
        #expect(try !RedisProvider.isContendedSetReply("OK"))
        #expect(throws: DilockError.self) {
            try RedisProvider.isContendedSetReply("unexpected")
        }
        try RedisProvider.validateContentionType("string")
        try RedisProvider.validateContentionType("none")
        #expect(throws: DilockError.self) {
            try RedisProvider.validateContentionType("hash")
        }
        #expect(throws: DilockError.self) {
            try RedisProvider.validateContentionType(nil)
        }
    }

    @Test func localConfigurationFailurePreservesHeldLease() {
        let state = RedisLease.stateAfterRenewalFailure(
            .held,
            error: DilockError.invalidConfiguration("lease must be positive")
        )
        #expect(state == .held)
    }

    @Test func ownershipLossAndUncertainRenewalLoseLease() {
        #expect(RedisLease.stateAfterRenewalFailure(.held, error: DilockError.leaseLost) == .lost)
        #expect(
            RedisLease.stateAfterRenewalFailure(
                .held,
                error: DilockError.backendUnavailable("Redis did not acknowledge renewal")
            ) == .lost
        )
    }

    @Test func terminalLeaseStateDoesNotReopenAfterRenewalFailure() {
        #expect(
            RedisLease.stateAfterRenewalFailure(.released, error: DilockError.invalidConfiguration("bad ttl"))
                == .released
        )
    }
}

private actor ReleaseCallCounter {
    private(set) var count = 0

    func release(_ outcome: ReleaseOutcome) -> ReleaseOutcome {
        count += 1
        return outcome
    }
}

private actor ReleaseGate {
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

private actor LifecycleSequence {
    private(set) var events: [String] = []
    private var renewalContinuation: CheckedContinuation<Void, Never>?

    func renew() async {
        events.append("renew-start")
        await withCheckedContinuation { renewalContinuation = $0 }
        events.append("renew-finish")
    }

    func release() { events.append("release") }

    func finishRenewal() {
        renewalContinuation?.resume()
        renewalContinuation = nil
    }
}
