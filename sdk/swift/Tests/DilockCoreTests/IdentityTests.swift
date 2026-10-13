import Foundation
import Testing
@testable import DilockCore

struct IdentityTests {
    private struct Vectors: Decodable {
        struct Vector: Decodable {
            let id: String
            let namespace: String
            let resource: String
            let sha256: String
            let redis_key: String
            let etcd_lock_name: String
        }
        let valid: [Vector]
    }

    @Test func matchesSharedIdentityVectors() throws {
        let url = Bundle.module.url(forResource: "identity", withExtension: "json")!
        let vectors = try JSONDecoder().decode(Vectors.self, from: Data(contentsOf: url))
        for vector in vectors.valid {
            let id = try Identity.resourceID(namespace: vector.namespace, resource: vector.resource)
            #expect(id == vector.sha256, "identity vector \(vector.id)")
            #expect(Identity.redisKey(resourceID: id) == vector.redis_key)
            #expect(Identity.etcdLockName(resourceID: id) == vector.etcd_lock_name)
        }
    }

    @Test func rejectsInvalidNamespaceAndResource() {
        #expect(throws: DilockError.self) { try Identity.resourceID(namespace: "Upper", resource: "r") }
        #expect(throws: DilockError.self) { try Identity.resourceID(namespace: "ok", resource: "") }
        #expect(throws: DilockError.self) { try Identity.resourceID(namespace: "ok", resource: "a\0b") }
    }

    @Test func acceptsMaximumUnicodeScalarInFourByteUtf8() throws {
        let resource = String(Unicode.Scalar(0x10FFFF)!)
        #expect(try Identity.resourceID(namespace: "default", resource: resource).count == 64)
    }

    @Test func requiresPositiveAcquisitionWaitAndLeaseDurations() {
        #expect(throws: DilockError.self) {
            try AcquireOptions(wait: .zero, lease: .seconds(1))
        }
        #expect(throws: DilockError.self) {
            try AcquireOptions(wait: .seconds(1), lease: .zero)
        }
        #expect(throws: DilockError.self) {
            try AcquireOptions(wait: .seconds(-1), lease: .seconds(1))
        }
    }
}
