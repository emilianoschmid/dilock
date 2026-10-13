import DilockCore
import DilockEtcd
import DilockRedis
import DilockSwiftWorkerSupport
import Foundation
import NIOCore
import NIOPosix
@preconcurrency import RediStack
#if os(Linux)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

private struct Config: Decodable {
    let backend: String
    let endpoint: String
    let namespace: String
    let epoch: String
}

private final class RedisConnectionOwner: @unchecked Sendable {
    let group: MultiThreadedEventLoopGroup
    let command: RedisConnection
    let cleanup: RedisConnection

    init(group: MultiThreadedEventLoopGroup, command: RedisConnection, cleanup: RedisConnection) {
        self.group = group
        self.command = command
        self.cleanup = cleanup
    }

    func close() async {
        try? await command.close().get()
        try? await cleanup.close().get()
        try? await group.shutdownGracefully()
    }
}

private enum Provider: Sendable {
    case redis(RedisProvider)
    case etcd(EtcdProvider)
}

private let maximumActiveRequests = 64
private let maximumHeldLeases = 64

private enum LeaseHandle: Sendable {
    case redis(RedisLease)
    case etcd(EtcdLease)

    func release() async -> ReleaseOutcome {
        switch self {
        case .redis(let lease): await lease.release()
        case .etcd(let lease): await lease.release()
        }
    }

    func stateName() async -> String {
        switch self {
        case .redis(let lease):
            switch await lease.state {
            case .held: "held"
            case .releasing: "releasing"
            case .released: "released"
            case .lost: "lost"
            case .releasedUnknown: "released_unknown"
            }
        case .etcd(let lease):
            switch await lease.state {
            case .waiting: "waiting"
            case .held: "held"
            case .releasing: "releasing"
            case .released: "released"
            case .lost: "lost"
            case .releasedUnknown: "released_unknown"
            }
        }
    }

    func renew(milliseconds: Int64) async throws {
        switch self {
        case .redis(let lease): try await lease.renew(for: .milliseconds(milliseconds))
        case .etcd: throw DilockError.unsupportedCapability("etcd renewals are managed automatically and cannot be manually requested")
        }
    }

    func fence() async -> [String: String]? {
        guard case .etcd(let lease) = self, let fence = await lease.fence else { return nil }
        return ["epoch": fence.epoch, "value": fence.createRevision]
    }

    func effectiveLeaseMilliseconds(requested: Int64) async -> Int64 {
        if case .etcd(let lease) = self { return await lease.effectiveLeaseMilliseconds }
        return requested
    }
}

private final class JSONLWriter: @unchecked Sendable {
    private let lock = NSLock()

    func send(_ object: [String: Any]) {
        guard let data = try? JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]),
        let line = String(data: data, encoding: .utf8) else { return }
        lock.lock()
        FileHandle.standardOutput.write(Data((line + "\n").utf8))
        lock.unlock()
    }
}

private actor Worker {
    private let provider: Provider
    private let backend: String
    private let sdkVersion: String
    private let writer: JSONLWriter
    private var leases: [String: LeaseHandle] = [:]
    private var pending: [String: Task<Void, Never>] = [:]
    private var stopped = false

    init(provider: Provider, backend: String, writer: JSONLWriter) {
        self.provider = provider
        self.backend = backend
        self.sdkVersion = "0.1.0-dev.1"
        self.writer = writer
    }

    func submit(_ line: String) async -> Bool {
        guard let request = try? WorkerRequest.decode(line: line) else {
            writer.send(["id": "", "status": "invalid_argument"])
            return false
        }
        guard !stopped, pending[request.id] == nil else {
            writer.send(["id": request.id, "status": "invalid_argument"])
            return false
        }
        if request.operation == .shutdown {
            await shutdown(responseID: request.id)
            return true
        }
        if request.operation == .cancel_request {
            pending[request.targetID ?? ""]?.cancel()
            writer.send(["id": request.id, "status": "ok"])
            return false
        }
        guard pending.count < maximumActiveRequests else {
            writer.send(["id": request.id, "status": "invalid_argument"])
            return false
        }
        let task = Task { await self.execute(request) }
        pending[request.id] = task
        return false
    }

    func finishInput() async {
        if !stopped { await shutdown(responseID: nil) }
    }

    private func execute(_ request: WorkerRequest) async {
        defer { pending.removeValue(forKey: request.id) }
        do {
            switch request.operation {
            case .hello:
                writer.send([
                    "id": request.id,
                    "status": "ok",
                    "sdk_version": sdkVersion,
                    "protocol": 1,
                    "backend": backend,
                    "server_version": "unknown",
                    "client_version": "unknown",
                    "capabilities": [
                        "renewal": backend == "redis",
                        "auto_renew": backend == "etcd",
                        "auto_renew_mandatory": backend == "etcd",
                        "fencing": backend == "etcd",
                        "try_acquire": backend == "redis"
                    ]
                ])
            case .try_acquire, .acquire:
                try await acquire(request)
            case .renew:
                guard let lease = leases[request.leaseID ?? ""] else { throw WorkerArgumentError.invalid }
                guard let milliseconds = request.leaseMilliseconds else {
                    throw DilockError.invalidConfiguration("renew requires lease_ms")
                }
                try await lease.renew(milliseconds: milliseconds)
                writer.send(["id": request.id, "status": "renewed"])
            case .release:
                guard let lease = leases[request.leaseID ?? ""] else { throw WorkerArgumentError.invalid }
                let result = await lease.release()
                let status: String
                switch result {
                case .released: status = "released"
                case .alreadyNotOwned: status = "already_not_owned"
                case .unknown: status = "release_unknown"
                }
                writer.send(["id": request.id, "status": status])
            case .inspect:
                guard let lease = leases[request.leaseID ?? ""] else { throw WorkerArgumentError.invalid }
                writer.send(["id": request.id, "status": "ok", "state": await lease.stateName()])
            case .cancel_request, .shutdown:
                break
            }
        } catch is CancellationError {
            writer.send(["id": request.id, "status": "cancelled", "remote_outcome_unknown": false])
        } catch let error as DilockError {
            let status: String
            var response: [String: Any] = ["id": request.id]
            switch error {
            case .invalidConfiguration: status = "invalid_argument"
            case .backendUnavailable: status = "backend_unavailable"
            case .protocolMismatch: status = "protocol_mismatch"
            case .timedOut: status = "timeout"
            case .acquisitionOutcomeUnknown:
                status = "acquisition_unknown"
                response["remote_outcome_unknown"] = true
            case .unsupportedCapability: status = "unsupported_capability"
            case .leaseLost: status = "internal_error"
            case .releaseUnknown: status = "release_unknown"
            }
            response["status"] = status
            writer.send(response)
        } catch {
            writer.send(["id": request.id, "status": "invalid_argument"])
        }
    }

    private func acquire(_ request: WorkerRequest) async throws {
        guard let resource = request.resource else { throw WorkerArgumentError.invalid }
        guard leases.count < maximumHeldLeases else { throw WorkerArgumentError.invalid }
        let ttl = request.leaseMilliseconds ?? 30_000
        let wait = request.waitMilliseconds ?? 5_000
        guard ttl > 0, wait >= 0 else { throw WorkerArgumentError.invalid }
        let lease: LeaseHandle?
        switch provider {
        case .redis(let redis):
            guard request.autoRenew != true else {
                throw DilockError.unsupportedCapability("Redis worker does not implement background auto-renewal")
            }
            let options = try AcquireOptions(wait: .milliseconds(wait), lease: .milliseconds(ttl))
            let lock = try await redis.lock(resource)
            if request.operation == .try_acquire {
                lease = try await lock.tryAcquire(options).map(LeaseHandle.redis)
            } else {
                lease = .redis(try await lock.acquire(options))
            }
        case .etcd(let etcd):
            guard request.autoRenew != false else {
                throw DilockError.unsupportedCapability("Swift etcd leases always use the native keepalive lifecycle")
            }
            guard request.operation != .try_acquire else {
                throw DilockError.unsupportedCapability("etcd native Lock queues and has no non-queuing try-acquire")
            }
            let options = try AcquireOptions(wait: .milliseconds(wait), lease: .milliseconds(ttl))
            let lock = try await etcd.lock(resource)
            lease = .etcd(try await lock.acquire(options))
        }
        guard let lease else {
            writer.send(["id": request.id, "status": "busy"])
            return
        }
        let localID = UUID().uuidString.lowercased()
        leases[localID] = lease
        var response: [String: Any] = ["id": request.id, "status": "acquired", "lease_id": localID,
                                       "effective_lease_ms": await lease.effectiveLeaseMilliseconds(requested: ttl)]
        if let fence = await lease.fence() { response["fence"] = fence }
        writer.send(response)
    }

    private func shutdown(responseID: String?) async {
        stopped = true
        let tasks = Array(pending.values)
        for task in tasks { task.cancel() }
        for task in tasks { await task.value }
        var cleanupFailures = 0
        for lease in leases.values {
            if await lease.release() == .unknown { cleanupFailures += 1 }
        }
        leases.removeAll()
        switch provider {
        case .redis(let redis): await redis.close()
        case .etcd(let etcd): await etcd.close()
        }
        if let responseID {
            writer.send(["id": responseID, "status": "ok", "outstanding_cleanup": cleanupFailures])
        }
    }
}

private enum WorkerArgumentError: Error { case invalid }

private func diagnostic(_ stage: String) {
    guard ProcessInfo.processInfo.environment["DILOCK_WORKER_DIAGNOSTICS"] == "1" else { return }
    FileHandle.standardError.write(Data(("dilock-swift-worker: " + stage + "\n").utf8))
}

private func boundedInputLines() -> AsyncStream<String> {
    AsyncStream { continuation in
        Task.detached {
            var bytes: [UInt8] = []
            var oversized = false
            var buffer = [UInt8](repeating: 0, count: 4_096)
            while true {
                let count = buffer.withUnsafeMutableBytes { rawBuffer -> Int in
                    #if os(Linux)
                    Glibc.read(STDIN_FILENO, rawBuffer.baseAddress, rawBuffer.count)
                    #elseif canImport(Darwin)
                    Darwin.read(STDIN_FILENO, rawBuffer.baseAddress, rawBuffer.count)
                    #else
                    -1
                    #endif
                }
                if count < 0 {
                    if errno == EINTR { continue }
                    diagnostic("jsonl.input.read.failed errno=\(errno)")
                    break
                }
                if count == 0 { break }
                for byte in buffer.prefix(count) {
                    if byte == 0x0A {
                        if oversized { continuation.yield(String(repeating: " ", count: WorkerRequest.maximumLineBytes + 1)) }
                        else { continuation.yield(String(bytes: bytes, encoding: .utf8) ?? " ") }
                        bytes.removeAll(keepingCapacity: true)
                        oversized = false
                    } else if !oversized {
                        if bytes.count < WorkerRequest.maximumLineBytes { bytes.append(byte) }
                        else { bytes.removeAll(keepingCapacity: false); oversized = true }
                    }
                }
            }
            if oversized { continuation.yield(String(repeating: " ", count: WorkerRequest.maximumLineBytes + 1)) }
            else if !bytes.isEmpty { continuation.yield(String(bytes: bytes, encoding: .utf8) ?? " ") }
            continuation.finish()
        }
    }
}

@main
private enum DilockSwiftWorker {
    static func main() async throws {
        diagnostic("config.read.start")
        guard CommandLine.arguments.count == 2 else {
            FileHandle.standardError.write(Data("usage: dilock-swift-worker CONFIG.json\n".utf8))
            throw WorkerArgumentError.invalid
        }
        let config = try JSONDecoder().decode(Config.self, from: Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[1])))
        diagnostic("config.read.complete backend=\(config.backend)")
        guard !config.namespace.isEmpty, !config.epoch.isEmpty else { throw WorkerArgumentError.invalid }
        let provider: Provider
        let redisOwner: RedisConnectionOwner?
        switch config.backend {
        case "redis":
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            do {
                diagnostic("redis.command.connect.start")
                let command = try await RedisConnection.make(configuration: .init(url: config.endpoint), boundEventLoop: group.next()).get()
                diagnostic("redis.command.connect.complete")
                diagnostic("redis.cleanup.connect.start")
                let cleanup = try await RedisConnection.make(configuration: .init(url: config.endpoint), boundEventLoop: group.next()).get()
                diagnostic("redis.cleanup.connect.complete")
                let owner = RedisConnectionOwner(group: group, command: command, cleanup: cleanup)
                diagnostic("redis.metadata.validate.start")
                let redis = try await RedisProvider(connection: owner.command, cleanupConnection: owner.cleanup,
                                                    namespace: config.namespace, epoch: config.epoch)
                diagnostic("redis.metadata.validate.complete")
                provider = .redis(redis)
                redisOwner = owner
            } catch {
                try? await group.shutdownGracefully()
                throw error
            }
        case "etcd":
            guard let endpoint = URL(string: config.endpoint) else { throw WorkerArgumentError.invalid }
            let options = try EtcdProviderOptions(endpoint: endpoint, namespace: config.namespace, epoch: config.epoch)
            provider = .etcd(try await EtcdProvider(options: options))
            redisOwner = nil
        default:
            throw WorkerArgumentError.invalid
        }

        let worker = Worker(provider: provider, backend: config.backend, writer: JSONLWriter())
        diagnostic("jsonl.input.start")
        for await line in boundedInputLines() {
            diagnostic("jsonl.line.received bytes=\(line.utf8.count)")
            if await worker.submit(line) { break }
        }
        await worker.finishInput()
        await redisOwner?.close()
    }
}
