# PHP + etcd quickstart

This consumer uses Dilock's PHP etcd adapter over etcd v3 gRPC. It runs a default-policy scope and then an explicit 3-second wait / 20-second lease scope through distinct handles on the same provider/channel. Both print the epoch/revision token. This SDK adapter is experimental; the protected data store must atomically reject stale fences.

Prerequisites: PHP 8.1+, Composer, `ext-grpc`, `grpc/grpc`, `google/protobuf`, a reachable etcd v3 endpoint, and pre-provisioned metadata at `/dilock/namespaces/<namespace>/meta` matching `DILOCK_EPOCH`. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Metadata is never created by the runtime SDK.

From the repository root:

```sh
composer install --working-dir=sdk/php
php docs/examples/php-etcd/quickstart/main.php
```

Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to the provisioned cluster values. The sample will fail if metadata is absent or incompatible.
