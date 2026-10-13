import Crypto
import Foundation

/// Draft 1 cross-language resource identity encoding.
public enum Identity {
    public static func resourceID(namespace: String, resource: String) throws -> String {
        let namespaceBytes = Array(namespace.utf8)
        guard namespaceBytes.count >= 1, namespaceBytes.count <= 64,
              namespaceBytes.allSatisfy({
                  (0x61...0x7a).contains($0) || (0x30...0x39).contains($0)
                      || [0x2e, 0x5f, 0x2d].contains($0)
              }), (0x61...0x7a).contains(namespaceBytes[0])
                  || (0x30...0x39).contains(namespaceBytes[0]) else {
            throw DilockError.invalidConfiguration("namespace must match [a-z0-9][a-z0-9_.-]{0,63}")
        }

        let resourceBytes = Array(resource.utf8)
        guard !resourceBytes.isEmpty, resourceBytes.count <= 1024,
              !resourceBytes.contains(0), resource.unicodeScalars.count > 0 else {
            throw DilockError.invalidConfiguration("resource must contain 1..1024 UTF-8 bytes and no U+0000")
        }

        var framed = Array("dilock.lock.identity".utf8) + [0]
        appendLength(namespaceBytes.count, to: &framed)
        framed.append(contentsOf: namespaceBytes)
        appendLength(resourceBytes.count, to: &framed)
        framed.append(contentsOf: resourceBytes)
        return SHA256.hash(data: Data(framed)).map { String(format: "%02x", $0) }.joined()
    }

    public static func redisKey(resourceID: String) -> String {
        "dilock:{\(resourceID)}:lease"
    }

    public static func etcdLockName(resourceID: String) -> String {
        "/dilock/locks/\(resourceID)"
    }

    private static func appendLength(_ length: Int, to bytes: inout [UInt8]) {
        let value = UInt32(length)
        bytes.append(UInt8((value >> 24) & 0xff))
        bytes.append(UInt8((value >> 16) & 0xff))
        bytes.append(UInt8((value >> 8) & 0xff))
        bytes.append(UInt8(value & 0xff))
    }
}
