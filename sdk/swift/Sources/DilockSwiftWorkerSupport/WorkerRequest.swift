import CoreFoundation
import Foundation

public enum WorkerOperation: String, Sendable {
    case hello, acquire, try_acquire, renew, release, inspect, cancel_request, shutdown
}

/// Strict parser for the bounded JSONL test protocol. Unknown properties are rejected.
public struct WorkerRequest: Sendable, Equatable {
    public let id: String
    public let operation: WorkerOperation
    public let resource: String?
    public let leaseID: String?
    public let targetID: String?
    public let waitMilliseconds: Int64?
    public let leaseMilliseconds: Int64?
    public let autoRenew: Bool?

    public static let maximumLineBytes = 65_536

    public static func decode(line: String) throws -> WorkerRequest {
        guard line.utf8.count <= maximumLineBytes,
              let data = line.data(using: .utf8),
              let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { throw ParseError.invalid }
        let allowed: Set<String> = ["id", "op", "resource", "lease_id", "target_id", "wait_ms", "lease_ms", "auto_renew"]
        guard Set(object.keys).isSubset(of: allowed),
              let id = object["id"] as? String, !id.isEmpty, id.utf8.count <= 128,
              let operationText = object["op"] as? String,
              let operation = WorkerOperation(rawValue: operationText)
        else { throw ParseError.invalid }

        func string(_ key: String) throws -> String? {
            guard let value = object[key] else { return nil }
            guard let text = value as? String else { throw ParseError.invalid }
            return text
        }
        func integer(_ key: String) throws -> Int64? {
            guard let value = object[key] else { return nil }
            guard let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID(),
                  number.doubleValue.rounded(.towardZero) == number.doubleValue,
                  number.doubleValue >= Double(Int64.min), number.doubleValue <= Double(Int64.max)
            else { throw ParseError.invalid }
            return number.int64Value
        }
        let resource = try string("resource")
        let leaseID = try string("lease_id")
        let targetID = try string("target_id")
        let wait = try integer("wait_ms")
        let lease = try integer("lease_ms")
        let autoRenew: Bool?
        if let value = object["auto_renew"] {
            guard let number = value as? NSNumber, CFGetTypeID(number) == CFBooleanGetTypeID() else {
                throw ParseError.invalid
            }
            autoRenew = number.boolValue
        } else { autoRenew = nil }

        switch operation {
        case .acquire:
            guard let resource, !resource.isEmpty else { throw ParseError.invalid }
            guard let wait, wait > 0 else { throw ParseError.invalid }
            if let lease, lease < 1 { throw ParseError.invalid }
        case .try_acquire:
            guard let resource, !resource.isEmpty else { throw ParseError.invalid }
            if let wait, wait < 0 { throw ParseError.invalid }
            if let lease, lease < 1 { throw ParseError.invalid }
        case .renew, .release, .inspect:
            guard let leaseID, !leaseID.isEmpty else { throw ParseError.invalid }
            if operation == .renew, let lease, lease < 1 { throw ParseError.invalid }
        case .cancel_request:
            guard let targetID, !targetID.isEmpty else { throw ParseError.invalid }
        case .hello, .shutdown: break
        }
        return WorkerRequest(
            id: id, operation: operation, resource: resource, leaseID: leaseID,
            targetID: targetID, waitMilliseconds: wait, leaseMilliseconds: lease,
            autoRenew: autoRenew
        )
    }

    public enum ParseError: Error { case invalid }
}
