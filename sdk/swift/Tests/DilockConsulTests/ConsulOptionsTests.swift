import DilockCore
import Foundation
import Testing
@testable import DilockConsul

struct ConsulOptionsTests {
    @Test func validatesEndpointEpochAndACLToken() throws {
        #expect(throws: DilockError.self) {
            try ConsulProviderOptions(endpoint: URL(string: "http://user:pass@localhost:8500")!, epoch: "e")
        }
        #expect(throws: DilockError.self) {
            try ConsulProviderOptions(endpoint: URL(string: "http://localhost:8500/consul")!, epoch: "e")
        }
        #expect(throws: DilockError.self) {
            try ConsulProviderOptions(endpoint: URL(string: "http://localhost:8500")!, epoch: "bad\0epoch")
        }
        #expect(throws: DilockError.self) {
            try ConsulProviderOptions(endpoint: URL(string: "http://localhost:8500")!, epoch: "e", token: "bad token")
        }
        let options = try ConsulProviderOptions(endpoint: URL(string: "https://consul.example:8501")!, namespace: "billing", epoch: "incarnation-5", token: "secret-token")
        #expect(options.namespace == "billing")
        #expect(options.epoch == "incarnation-5")
    }

    @Test func rejectsInvalidNamespacesAndNonPositiveBudgets() throws {
        #expect(throws: DilockError.self) {
            try ConsulProviderOptions(endpoint: URL(string: "http://localhost:8500")!, namespace: "Bad Namespace", epoch: "e")
        }
        #expect(throws: DilockError.self) {
            try AcquireOptions(wait: .zero, lease: .seconds(1))
        }
    }

    @Test func malformedOrWrongKeyKVReadsAreNotTreatedAsMissing() throws {
        let missingOwner = try ConsulProvider.decodeKVRow(
            Data("[{\"Key\":\"dilock/locks/resource\",\"Session\":null,\"LockIndex\":0}]".utf8),
            expectedKey: "dilock/locks/resource"
        )
        #expect(missingOwner.session == nil)
        #expect(throws: DilockError.self) {
            try ConsulProvider.decodeKVRow(Data("not-json".utf8), expectedKey: "dilock/locks/resource")
        }
        #expect(throws: DilockError.self) {
            try ConsulProvider.decodeKVRow(
                Data("[{\"Key\":\"dilock/locks/other\",\"Session\":null,\"LockIndex\":0}]".utf8),
                expectedKey: "dilock/locks/resource"
            )
        }
    }
}
