import DilockCore
import Foundation

/// Validated connection identity for the native etcd v3 Lock profile.
/// This value configures the transport adapter; it does not provision namespace metadata.
public struct EtcdProviderOptions: Sendable, Equatable {
    public let endpoint: URL
    public let namespace: String
    public let epoch: String
    public let rpcTimeout: Duration
    public let cleanupTimeout: Duration
    /// Maximum active ownership time before the SDK stops renewal and revokes its lease.
    public let maximumLeaseLifetime: Duration
    public let defaults: AcquireOptions

    public init(
        endpoint: URL,
        namespace: String = "default",
        epoch: String,
        rpcTimeout: Duration = .seconds(2),
        cleanupTimeout: Duration = .seconds(2),
        maximumLeaseLifetime: Duration = .seconds(300),
        defaults: AcquireOptions = try! AcquireOptions()
    ) throws {
        guard endpoint.scheme == "http" || endpoint.scheme == "https",
              endpoint.host != nil, endpoint.user == nil, endpoint.password == nil,
              endpoint.path.isEmpty || endpoint.path == "/", endpoint.query == nil,
              endpoint.fragment == nil else {
            throw DilockError.invalidConfiguration("etcd endpoint must be an HTTP(S) authority without credentials, query, or path")
        }
        _ = try Identity.resourceID(namespace: namespace, resource: "validation")
        guard !epoch.isEmpty, epoch.utf8.count <= 1024 else {
            throw DilockError.invalidConfiguration("epoch must contain 1..1024 UTF-8 bytes")
        }
        guard rpcTimeout > .zero, cleanupTimeout > .zero, maximumLeaseLifetime > .zero else {
            throw DilockError.invalidConfiguration("RPC, cleanup, and maximum lifetime durations must be finite and positive")
        }
        self.endpoint = endpoint
        self.namespace = namespace
        self.epoch = epoch
        self.rpcTimeout = rpcTimeout
        self.cleanupTimeout = cleanupTimeout
        self.maximumLeaseLifetime = maximumLeaseLifetime
        self.defaults = defaults
    }
}
