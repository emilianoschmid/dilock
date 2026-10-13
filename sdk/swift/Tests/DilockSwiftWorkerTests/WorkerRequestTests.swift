import Foundation
import Testing
import DilockSwiftWorkerSupport

struct WorkerRequestTests {
    @Test func decodesValidAcquireAndPreservesDefaultsAsAbsent() throws {
        let request = try WorkerRequest.decode(line: #"{"id":"a1","op":"acquire","resource":"jobs","wait_ms":1200,"lease_ms":5000}"#)
        #expect(request.id == "a1")
        #expect(request.operation == .acquire)
        #expect(request.resource == "jobs")
        #expect(request.waitMilliseconds == 1200)
        #expect(request.leaseMilliseconds == 5000)
        #expect(request.autoRenew == nil)
    }

    @Test func rejectsUnknownFieldsAndWrongScalarTypes() {
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"hello","secret":"must-not-echo"}"#)
        }
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"acquire","resource":"r","wait_ms":true}"#)
        }
    }

    @Test func rejectsMissingOperationFieldsAndOversizedLines() {
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"release"}"#)
        }
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"acquire","resource":"r"}"#)
        }
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: "{\"id\":\"a1\",\"op\":\"hello\"}" + String(repeating: " ", count: WorkerRequest.maximumLineBytes))
        }
    }

    @Test func rejectsInvalidIdsAndNegativeDurations() throws {
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"","op":"hello"}"#)
        }
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"try_acquire","resource":"r","lease_ms":0}"#)
        }
        #expect(throws: (any Error).self) {
            try WorkerRequest.decode(line: #"{"id":"a1","op":"acquire","resource":"r","wait_ms":0,"lease_ms":1}"#)
        }
        #expect(try WorkerRequest.decode(line: #"{"id":"a2","op":"try_acquire","resource":"r","wait_ms":0,"lease_ms":1}"#).operation == .try_acquire)
    }
}
