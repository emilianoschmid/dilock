# Swift + Redis quickstart

This standalone SwiftPM consumer uses Dilock's Swift Redis adapter and RediStack. It runs two sequential resource scopes through one provider: defaults first, then a 3-second wait / 20-second lease policy. Both handles reuse the provider's connections. Redis commands use the owner-checked Lua bundled by the SDK. The Swift SDK remains experimental; this Redis profile does not expose a strong fencing token.

Prerequisites: Swift 6.1 toolchain, SwiftPM dependencies, a reachable Redis server, and metadata pre-provisioned for namespace `default` and the epoch configured in the source. Redis namespace details are in [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The SDK only validates metadata.

The example folder is a small consumer package whose dependency points to `sdk/swift`. From the repository root:

```sh
swift run --package-path docs/examples/swift-redis/quickstart
```

Change the endpoint and epoch in `main.swift` to match the pre-provisioned namespace. The caller owns the NIO event-loop group; the example closes the provider before shutting it down.
