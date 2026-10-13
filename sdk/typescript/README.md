# Dilock TypeScript workspaces

This directory is a private npm workspace containing three independent package candidates: `@dilock/core`, `@dilock/redis` and `@dilock/etcd`. Core has no Redis or gRPC runtime dependency. The Redis adapter installs node-redis; the etcd adapter installs grpc-js and its loader. Each candidate manifest stays `private: true` until npm scope ownership is verified. Nothing is published by the workspace scripts.

Use Node.js 22 or newer. Runtime dependencies are declared by their adapter packages; workspace tooling is pinned in the root manifest and lockfile. Run these local checks from this directory:

```powershell
$env:NPM_CONFIG_CACHE = 'C:\codex\dilock\.tools\npm-cache'
npm install --offline --ignore-scripts --no-audit --no-fund
npm test
npm run pack:check
npm run test:packages
```

The package consumer gate packs and installs the exact three local tarballs offline. It verifies that core runs without either backend client and that each adapter contains only its own backend's runtime dependency closure. See [`consumers/README.md`](consumers/README.md).

## Redis

Namespace metadata must be provisioned explicitly before normal clients connect. The worker orchestrator provisions disposable namespaces; the exported `provisionNamespace` helper is an administrative utility.

```ts
import { RedisProvider } from "@dilock/redis";

const provider = await RedisProvider.connect({ endpoint: process.env.REDIS_URL!, namespace: "billing", epoch: "prod-redis-2026-10" });
try {
  await provider.lock("invoice/42/reconcile").withLease(async lease => {
    lease.assertValid();
    // Protect the actual resource with transactions, idempotency, or resource-side fencing.
  });
} finally { await provider.close(); }
```

`withLease` acquires with the provider's standard 5 s wait, 30 s lease and 2 s RPC timeout, then awaits bounded release. To supply a complete per-call policy, pass all three values:

```ts
await provider.lock("invoice/42/reconcile").withLease(
  async lease => { lease.assertValid(); },
  { waitMs: 8_000, leaseMs: 45_000, rpcTimeoutMs: 1_000 },
);
```

The provider owns its Redis client and closes it when `close` is awaited. `tryAcquire` returns `undefined` only for ordinary contention; `acquire` throws on wait timeout. Abort before send cancels cleanly; cancellation or transport failure after dispatch is an unknown outcome, with an owner-specific cleanup attempt. Renewal is explicit. Redis fencing is rejected as unsupported.

Calling `acquire()` uses the provider's complete default acquisition policy. When overriding it per call, pass a complete policy including `waitMs`, `leaseMs` and `rpcTimeoutMs`.

## etcd native Lock profile

Connect one `EtcdProvider` to one etcd authority with a pre-provisioned namespace and stable operator-approved epoch. Runtime connections validate metadata with linearizable Range and never provision it. Each acquisition has its own server lease; the adapter keeps it alive while waiting for native `Lock`. After acquisition, `autoRenew` defaults to false; set it to true to keep the lease renewed while held. Cancellation aborts the gRPC call and attempts bounded lease revocation. `tryAcquire` fails with `UnsupportedCapabilityError`: native Lock has no try-lock RPC, and a deadline cannot distinguish contention from backend delay. Endpoints may be plaintext `http://` or TLS `https://`; this package currently creates default gRPC TLS credentials for HTTPS and does not expose custom trust roots or client certificates.

```ts
import { EtcdProvider } from "@dilock/etcd";

const provider = await EtcdProvider.connect({ endpoint: "http://127.0.0.1:2379", namespace: "billing", epoch: "prod-etcd-2026-10" });
try {
  await provider.lock("invoice/42/reconcile").withLease(async lease => {
    lease.assertValid();
    const fence = lease.fence; // { epoch, value: create_revision }
    // The protected resource must atomically compare/register this fence.
  });
} finally { await provider.close(); }
```

On a successful native Lock, the SDK retains the returned opaque key and verifies that exact key's lease and `create_revision` with a linearizable Range before returning. It never edits a native Lock key. Revisions need an operator-controlled epoch change after restore/reset; the fence does not provide exactly-once effects. `release()` awaits native Unlock of the opaque key and revokes only that attempt lease. `close()` cancels in-flight acquisitions, attempts bounded cleanup and closes its owned gRPC clients. Provider/lease state is safe across Node.js tasks; an `AbortSignal` is per operation. Managed keepalive is mandatory while queued; after acquisition it follows `autoRenew`.

Calling `acquire()` uses the provider's complete default policy, including `autoRenew: false`. Per-call overrides must specify the complete acquisition policy, including `waitMs`, `leaseMs`, `rpcTimeoutMs` and `autoRenew`.

For a longer operation, enable renewal explicitly in the scoped policy:

```ts
await provider.lock("invoice/42/reconcile").withLease(
  async lease => { lease.assertValid(); },
  { waitMs: 8_000, leaseMs: 45_000, rpcTimeoutMs: 1_000, autoRenew: true },
);
```

The JSONL worker is `npm run worker -- <config.json>` and keeps replies on stdout, diagnostics on stderr. Real interoperability requires an explicitly provisioned namespace and the shared conformance harness.
