# TypeScript etcd public API consumer

This standalone consumer uses the public TypeScript API with two independently owned etcd providers. They acquire one unique resource in sequence, print their epoch-scoped etcd `create_revision` fences, and release explicitly. Native etcd `Lock` has no non-blocking try-lock API; the example uses its queued acquisition API and never treats a timeout as ordinary contention.

An operator must provision `/dilock/namespaces/<namespace>/meta` before the example runs. Its UTF-8 JSON value must contain `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The SDK validates the existing record and never provisions or edits it. See [`spec/NAMESPACE-METADATA.md`](../../../spec/NAMESPACE-METADATA.md). Use a disposable non-production etcd authority for this short demonstration.

## Build and run offline

Node.js 22 or newer is required. Build and pack the core and etcd workspaces locally, install both tarballs without lifecycle scripts, then type-check using the SDK's locally installed TypeScript compiler and Node declarations:

```powershell
npm --prefix sdk/typescript run build
New-Item -ItemType Directory -Force .tools/m6-typescript-etcd-consumer | Out-Null
New-Item -ItemType Directory -Force .tools/npm-cache | Out-Null
$env:NPM_CONFIG_CACHE = Join-Path (Get-Location).Path '.tools/npm-cache'
Push-Location sdk/typescript
npm pack --workspace=@dilock/core --pack-destination ..\..\.tools\m6-typescript-etcd-consumer
npm pack --workspace=@dilock/etcd --pack-destination ..\..\.tools\m6-typescript-etcd-consumer
Pop-Location
$core = (Resolve-Path .tools/m6-typescript-etcd-consumer/dilock-core-0.1.0.tgz).Path
$etcd = (Resolve-Path .tools/m6-typescript-etcd-consumer/dilock-etcd-0.1.0.tgz).Path
npm --prefix docs/examples/typescript-etcd install --no-save --package-lock=false --ignore-scripts --offline --cache $env:NPM_CONFIG_CACHE $core $etcd
npm --prefix docs/examples/typescript-etcd run check
```

`--offline` prohibits registry access, and the tarball is built from this checkout. Set the endpoint and exact existing metadata identity before running:

```powershell
$env:DILOCK_ETCD_ENDPOINT = 'http://127.0.0.1:2379'
$env:DILOCK_NAMESPACE = 'example'
$env:DILOCK_EPOCH = 'operator-approved-cluster-incarnation'
npm --prefix docs/examples/typescript-etcd start
```

The endpoint is one `http://` or `https://` etcd authority without credentials or path. The current SDK configures TLS with default root trust when using HTTPS; it does not accept inline credentials.

## Lifetime, cancellation, fencing, and limits

Each provider owns its gRPC clients and active leases and must be closed; the example awaits `close()` in `finally`. Each lease is one acquisition generation and its `release()` is explicitly awaited. Pass an `AbortSignal` to `acquire` to cancel. Cancellation or loss of a response after dispatch can leave a remote outcome unknown and is reported as an error, never “busy.” A bounded `waitMs` expiry is an acquisition timeout. `tryAcquire` reports `UnsupportedCapabilityError` because the native etcd Lock RPC cannot safely distinguish immediate contention from backend delay.

The fence is `(epoch, create_revision)`. The protected resource must atomically validate the approved epoch and reject stale revisions before relying on the token. Coordinator restore or replacement requires a coordinated epoch migration. Lease expiry and `Lost` cannot stop stale application work; downstream enforcement is required. This example does not prove external fencing, quorum behavior, or failover safety.
