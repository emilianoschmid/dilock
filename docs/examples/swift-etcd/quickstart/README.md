# Swift + etcd quickstart

This standalone SwiftPM consumer uses Dilock's Swift etcd adapter over grpc-swift 2 and native etcd v3 Lock/Lease services. It runs two sequential resource scopes on one provider/channel, first with defaults and then with a 3-second wait / 20-second lease policy. Both print the epoch/create-revision fence. The SDK is experimental; downstream writes must atomically enforce the fence.

Prerequisites: Swift 6.1 toolchain, SwiftPM dependencies, a reachable etcd v3 endpoint, and metadata already provisioned at `/dilock/namespaces/default/meta` for the configured epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The SDK validates but never creates metadata.

The example folder is a consumer package pointing at `sdk/swift`. From the repository root:

```sh
swift run --package-path docs/examples/swift-etcd/quickstart
```

Set the endpoint and epoch in `main.swift` to match the existing metadata. The current adapter accepts plaintext HTTP/2 only; use a trusted endpoint as described in the Swift SDK guide.
