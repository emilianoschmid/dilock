import Testing
@testable import DilockCore

struct WithLeaseTests {
    private enum OperationError: Error, Equatable { case failed, leaseLost }

    @Test func successfulOperationAwaitsReleaseOnce() async throws {
        let lease = TestLease(outcome: .released)
        let result = try await withLeaseScope(lease) { _ in "done" }
        #expect(result == "done")
        #expect(await lease.releaseCount == 1)
    }

    @Test func operationErrorRemainsPrimaryWhenCleanupIsUnknown() async {
        let lease = TestLease(outcome: .unknown)
        do {
            _ = try await withLeaseScope(lease) { _ in throw OperationError.failed }
            Issue.record("Expected operation to fail")
        } catch let error as OperationError {
            #expect(error == .failed)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func leaseLossRemainsPrimaryWhenCleanupIsUnknown() async {
        let lease = TestLease(outcome: .unknown)
        do {
            _ = try await withLeaseScope(lease) { _ in throw OperationError.leaseLost }
            Issue.record("Expected lease loss to escape the operation")
        } catch let error as OperationError {
            #expect(error == .leaseLost)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func unknownCleanupAfterSuccessIsReported() async {
        let lease = TestLease(outcome: .unknown)
        do {
            _ = try await withLeaseScope(lease) { _ in 42 }
            Issue.record("Expected unknown cleanup to fail the scoped call")
        } catch let error as DilockError {
            #expect(error == .releaseUnknown)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func alreadyLostLeaseAfterSuccessIsReported() async {
        let lease = TestLease(outcome: .alreadyNotOwned)
        do {
            _ = try await withLeaseScope(lease, cleanup: { lease in
                (await lease.release(), false)
            }) { _ in 42 }
            Issue.record("Expected loss of ownership to fail the scoped call")
        } catch let error as DilockError {
            #expect(error == .leaseLost)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func successfulRedisCleanupDoesNotHideExpiredOrLostScope() async {
        let lease = TestLease(outcome: .released)
        do {
            _ = try await withLeaseScope(lease, cleanup: { lease in
                (await lease.release(), false)
            }) { _ in 42 }
            Issue.record("Expected an invalid lease scope to fail")
        } catch let error as DilockError {
            #expect(error == .leaseLost)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func validRedisScopeMayReturnAfterConfirmedRelease() async throws {
        let lease = TestLease(outcome: .released)
        let result = try await withLeaseScope(lease, cleanup: { lease in
            (await lease.release(), true)
        }) { _ in "released" }
        #expect(result == "released")
        #expect(await lease.releaseCount == 1)
    }

    @Test func acknowledgedAlreadyNotOwnedCleanupStillReportsLeaseLoss() async {
        let lease = TestLease(outcome: .alreadyNotOwned)
        do {
            _ = try await withLeaseScope(lease, cleanup: { lease in
                (await lease.release(), true)
            }) { _ in "work completed" }
            Issue.record("Expected confirmed cleanup with lost ownership to fail")
        } catch let error as DilockError {
            #expect(error == .leaseLost)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    @Test func workFailureRemainsPrimaryWhenAlreadyNotOwnedCleanupIsAcknowledged() async {
        let lease = TestLease(outcome: .alreadyNotOwned)
        do {
            _ = try await withLeaseScope(lease) { _ in throw OperationError.failed }
            Issue.record("Expected work failure to escape the scope")
        } catch let error as OperationError {
            #expect(error == .failed)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        #expect(await lease.releaseCount == 1)
    }

    private actor TestLease: Lease {
        private let outcome: ReleaseOutcome
        private(set) var releaseCount = 0

        init(outcome: ReleaseOutcome) { self.outcome = outcome }

        func release() async -> ReleaseOutcome {
            releaseCount += 1
            return outcome
        }
    }
}
