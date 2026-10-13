# TypeScript Redis consumer

This executable consumer uses the public `@dilock/redis` package and two independently owned Redis providers. It demonstrates acknowledged contention, explicit release, and a successor acquisition against Redis. It does not emulate a lock.

## Prerequisites

- Node.js 22 or newer (for built-in TypeScript type stripping).
- The private local `@dilock/core` and `@dilock/redis` workspaces built and packed locally.
- A Redis authority reachable from this machine, with an operator-provisioned namespace using profile `redis-single-authority-v1`, identity scheme `sha256-framed-utf8-v1`, the selected `coordination_epoch`, and `fixed_lease_ms: null`.

From the repository root, build both workspaces and install their local tarballs into this consumer. The install is offline and does not contact a registry:

```powershell
npm --prefix sdk/typescript run build
New-Item -ItemType Directory -Force .tools/m6-typescript-redis-consumer | Out-Null
Push-Location sdk/typescript
npm pack --workspace=@dilock/core --pack-destination ..\..\.tools\m6-typescript-redis-consumer
npm pack --workspace=@dilock/redis --pack-destination ..\..\.tools\m6-typescript-redis-consumer
Pop-Location
$core = (Resolve-Path .tools/m6-typescript-redis-consumer/dilock-core-0.1.0.tgz).Path
$redis = (Resolve-Path .tools/m6-typescript-redis-consumer/dilock-redis-0.1.0.tgz).Path
npm --prefix docs/examples/typescript-redis install --offline --no-save --package-lock=false --ignore-scripts $core $redis
npm --prefix docs/examples/typescript-redis run check
```

Set the endpoint and the already-provisioned namespace identity, then run:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
npm --prefix docs/examples/typescript-redis start
```

Expected output:

```text
contender: acknowledged busy
contender: acquired after owner release
```

The resource name is unique for each run. Credentials may be included in `DILOCK_REDIS_URL`; do not commit credentials or provision metadata implicitly from the example. The SDK validates metadata on connect and fails if the namespace is missing or incompatible.

Both providers own their Redis clients and are closed in `finally`. Release is awaited so the caller observes whether Redis confirmed ownership. `tryAcquire` returns `undefined` only for ordinary contention; connection/protocol errors throw. For cancellation, pass an `AbortSignal` in the operation options: cancellation before dispatch rejects as cancellation, while an uncertain in-flight command raises `AcquisitionUnknownError` and may have acquired. Do not treat unknown outcome as busy or retry blindly. The provider and lease may be used across Node.js tasks; use a distinct signal per operation. `waitMs` bounds waiting for contention, while `rpcTimeoutMs` bounds an individual Redis command.

Redis TTL provides eventual cleanup after a crash; it cannot stop stale application work. This profile does not provide strong fencing or safety across state loss and unsafe failover. A downstream resource that needs stale-writer protection must enforce fencing or idempotency itself.
