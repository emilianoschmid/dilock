# Dart + Redis quickstart

This is a consumer example for Dilock's Dart Redis adapter (`sdk/dart/redis`), which uses Redis commands and bundled owner-checked Lua scripts. The SDK and this example are experimental; Redis is a single-authority lease profile and does not provide a strong fencing token. The first resource uses the default policy; the second uses an explicit 3-second wait and 20-second lease. Both handles share one provider and its connections, and the scopes run sequentially.

Prerequisites: Dart 3.5+, a reachable Redis server, and Redis namespace metadata already provisioned for namespace `default` with epoch `local-redis-epoch`. Follow [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md); the application client only validates existing metadata.

From the repository root, resolve the local package dependencies and run:

```sh
cd sdk/dart/redis
dart pub get
dart run ../../../docs/examples/dart-redis/quickstart/main.dart
```

Set the endpoint and epoch in `main.dart` to match your provisioned environment before use. No live execution is implied by this source example.
