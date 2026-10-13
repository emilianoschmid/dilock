# Dilock for Swift

Swift Package Manager products are split into `DilockCore`, `DilockRedis`, and
`DilockEtcd`. The repository-root `Package.swift` exposes those same products
from `sdk/swift/Sources`, so a tagged Git checkout can be consumed from the
repository root instead of relying on an unsupported subdirectory URL. A minimal root-package consumer fixture is maintained in the repository for local validation.
The implementation follows Dilock's shared draft-1 identity and
lock contracts. The isolated Swift style sample is only an API-design
reference; the Redis and etcd adapters in this package are the actual backend
implementations.

## Conformance worker

The `dilock-swift-worker` SwiftPM executable implements the repository's JSONL
conformance test protocol. Start it with one JSON config path containing
`backend` (`redis` or `etcd`), `endpoint`, `namespace`, and `epoch`. Redis
supports one-shot acquisition, explicit renewal, and awaited release; etcd
supports queued acquisition, automatic keepalive, fencing, and awaited
release. The worker does not support Redis background auto-renewal, etcd
non-queuing `try_acquire`, or disabling etcd keepalive. It writes protocol
messages to stdout and leaves service configuration and provisioning to the
test harness.

The Redis source uses RediStack commands and bundles the canonical owner-check
Lua scripts. The etcd source uses the native Lock, Lease and Range RPCs through
grpc-swift 2 generated clients; its keepalive task runs before and during lock
waiting, verifies the exact opaque key, and exposes the epoch plus
`create_revision` as a fence. The package includes generated SwiftProtobuf and gRPC bindings for the focused
etcd v3.7.2 definitions, with upstream license notices retained under
`Sources/EtcdProtos/`. Consumers do not need `protoc` or SwiftPM generator
plugins to build. The checked-in `.proto` definitions and generation details
remain there for maintainers updating the wire schema.

The etcd acquisition wait budget starts before LeaseGrant and includes lease
grant, keepalive readiness, Lock, and exact-key Range verification. Each RPC is
capped by the remaining wait. If ownership verification completes after the
wait deadline, the SDK unlocks and revokes the attempt lease before returning;
it reports a timeout only when cleanup is acknowledged, and otherwise reports
an unknown acquisition outcome. Cancellation after dispatch follows the same
cleanup rule.

## etcd quickstart with defaults

The etcd provider creates its gRPC transport from an endpoint and uses a
5-second wait, 30-second lease, and 2-second RPC and cleanup budgets by default.
Namespace metadata must already be provisioned for the same epoch. This adapter
currently accepts plaintext HTTP/2 only:

`withLease` awaits owner cleanup before returning. If its operation throws, that
error remains primary even if cleanup is unknown. After a successful operation,
unknown cleanup is reported; both adapters report `leaseLost` for
`alreadyNotOwned`, including etcd's acknowledged revoke-only cleanup. Terminal
cleanup does not prove that ownership held throughout the callback. The helper
does not renew the lease or stop a callback that continues after lease loss.

```swift
import DilockCore
import DilockEtcd
import Foundation

let endpoint = URL(string: "http://127.0.0.1:2379")!
let options = try EtcdProviderOptions(endpoint: endpoint, epoch: "local-etcd-epoch")
let provider = try await EtcdProvider(options: options)
do {
    let lock = try await provider.lock("reports/daily")
    try await lock.withLease { lease in
        if let fence = await lease.fence {
            // Atomically enforce (fence.epoch, fence.createRevision) at the resource.
        }
        print("Run one short, idempotent report operation here")
    }
    await provider.close()
} catch {
    await provider.close()
    throw error
}
```

## Fully explicit etcd policy

Provider connection and acquisition budgets are configured independently. A
single provider can be reused for multiple handles; each handle represents a
different resource under that shared channel:

```swift
let acquisition = try AcquireOptions(wait: .seconds(3), lease: .seconds(20))
let options = try EtcdProviderOptions(
    endpoint: URL(string: "http://127.0.0.1:2379")!,
    namespace: "billing",
    epoch: "billing-etcd-incarnation-7",
    rpcTimeout: .seconds(1),
    cleanupTimeout: .seconds(1),
    maximumLeaseLifetime: .seconds(120),
    defaults: acquisition
)
let provider = try await EtcdProvider(options: options)
do {
    try await provider.lock("invoice/8391").withLease(acquisition) { lease in
        guard let fence = await lease.fence else { throw DilockError.leaseLost }
        // The protected resource must atomically reject stale epoch/revision values.
        print("Enforce fence \(fence.epoch)/\(fence.createRevision) at the resource")
    }
    await provider.close()
} catch {
    await provider.close()
    throw error
}
```

## Redis quickstart with defaults

Provision Redis namespace metadata for the same epoch before connecting. The
factory uses RediStack's `redis://` configuration, opens separate command and
cleanup connections, validates metadata, and owns both connections. The caller
owns the event-loop group and shuts it down after the provider:

```swift
import DilockCore
import DilockRedis
import NIOPosix
import RediStack

let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
do {
    let provider = try await RedisProvider.connect(
        configuration: .init(url: "redis://127.0.0.1:6379"),
        on: group,
        epoch: "local-redis-epoch"
    )
    do {
        try await provider.lock("reports/daily").withLease { _ in
            print("Run one short, idempotent report operation here")
        }
    } catch {
        await provider.close()
        throw error
    }
    await provider.close()
    try await group.shutdownGracefully()
} catch {
    try? await group.shutdownGracefully()
    throw error
}
```

`close()` is awaited and non-throwing for compatibility; `closeAndReport()` is
available when connection-shutdown errors must be observed. Task cancellation is
checked between connection and metadata stages; an in-flight RediStack future is
not interrupted, and any opened connections are closed before cancellation is
returned. Connection establishment uses RediStack's default TCP bootstrap and is
outside the acquisition wait budget. Each Redis command is locally bounded by
`rpcTimeout` (two seconds by default), while owner-specific cleanup uses its own
`cleanupTimeout` (also two seconds by default). A local timeout cannot cancel a
command already queued to Redis; a dispatched `SET` that times out or is cancelled
returns acquisition-outcome-unknown after one bounded cleanup attempt, and a late
Redis completion may leave an orphan until its TTL expires. Disable client retries.
The factory does not configure TLS or custom NIO bootstraps. For those transport
choices, construct two independently configured
RediStack connections and pass them to `RedisProvider(connection:cleanupConnection:namespace:epoch:defaults:)`;
the caller then owns their shutdown. The factory never shuts down the supplied
event-loop group. Redis metadata must be provisioned separately in either mode.

For etcd, concurrent calls to `EtcdProvider.close()` now share and await one
bounded shutdown operation. Its cleanup task is independent of cancellation on
the first caller, so another caller cannot observe `close()` as complete while
tracked lease cleanup is still running.

Run `swift test` from this SDK directory to build and execute its native test
suite. The etcd adapter supports plaintext HTTP/2 endpoints and rejects HTTPS.
Its opt-in integration test requires a pre-provisioned namespace record via
`DILOCK_TEST_ETCD_ENDPOINT`, `DILOCK_TEST_ETCD_NAMESPACE`, and
`DILOCK_TEST_ETCD_EPOCH`; it does not provision metadata. The SDK remains
experimental, and package consumption, platform coverage, and distributed
fault behavior must be verified for each release profile.
