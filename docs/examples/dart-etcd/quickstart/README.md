# Dart + etcd quickstart

This is a consumer example for Dilock's Dart etcd adapter (`sdk/dart/etcd`), which calls etcd v3 native Lock and Lease RPCs over gRPC and exposes an epoch-scoped create-revision fence. The SDK and example are experimental; the protected resource must atomically enforce the fence. The first resource uses defaults; the second explicitly requests a 3-second wait and 20-second lease. Both handles share one provider/channel and run sequentially.

Prerequisites: Dart 3.5+, a reachable etcd v3 endpoint, and metadata already provisioned at `/dilock/namespaces/default/meta` for epoch `local-etcd-incarnation-1`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Dilock does not create or repair metadata.

From the repository root:

```sh
cd sdk/dart/etcd
dart pub get
dart run ../../../docs/examples/dart-etcd/quickstart/main.dart
```

Set the endpoint and epoch in `main.dart` to match the provisioned cluster. This is a source quickstart, not evidence of a live etcd run.
