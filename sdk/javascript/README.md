# Dilock JavaScript for Node.js

JavaScript uses the TypeScript SDK's compiled Node.js runtime. This directory
contains only JavaScript examples and a consumer test; it has no second lock
implementation. The local consumer installs the exact workspace package sources
from `../typescript` and remains private. It does not represent a separately
published npm package or confirm Deno/Bun compatibility.

Use Node.js 22 or newer. From this directory, run `npm install --offline
--ignore-scripts --no-audit --no-fund`, then `npm test` and
`npm run check:examples`. The test imports the public package entry points and
checks shared identity behavior without starting a coordinator. The examples
below require real, pre-provisioned Redis, etcd, or Consul namespace metadata.

## Redis quick start

The common path uses the default namespace and the SDK's five-second wait and
thirty-second lease. The endpoint and deployment-approved epoch are required;
the epoch must match metadata provisioned ahead of time. `withLease` awaits
release before returning. A release failure rejects successful callback work;
if the callback already failed, its error remains primary and the provider
cleanup observer reports the unknown release. The outer `finally` closes the
provider. A non-function callback fails with `InvalidArgumentError` before
acquisition:

```js
import { RedisProvider } from "@dilock/redis";

const provider = await RedisProvider.connect({
  endpoint: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  namespace: "default",
  epoch: required("DILOCK_COORDINATION_EPOCH"),
});
try {
  await provider.lock("reports/daily").withLease(async (lease) => {
    lease.assertValid();
    await rebuildDailyReport();
  });
} finally {
  await provider.close();
}

async function rebuildDailyReport() {}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must match the pre-provisioned namespace epoch`);
  return value;
}
```

## Explicit policy

Set connection budgets and the provider's default acquisition policy when the
workload needs them. A call to `acquire()` with no argument uses that immutable
provider policy; pass options to `acquire` only when one attempt needs a
different wait or lease duration:

```js
import { RedisProvider } from "@dilock/redis";

const provider = await RedisProvider.connect({
  endpoint: required("REDIS_URL"),
  namespace: "billing",
  epoch: required("DILOCK_COORDINATION_EPOCH"),
  rpcTimeoutMs: 2_000,
  cleanupTimeoutMs: 3_000,
  defaultAcquisition: {
    waitMs: 12_000,
    leaseMs: 45_000,
    rpcTimeoutMs: 2_000,
  },
});
try {
  const lease = await provider.lock("invoice/42/reconcile").acquire();
  try {
    lease.assertValid();
    await rebuildInvoice();
  } finally {
    await lease.release();
  }
} finally {
  await provider.close();
}

async function rebuildInvoice() {}
```

The JavaScript package shares the TypeScript packages' contracts and limits:
await release/close explicitly, treat cancellation or uncertain acquisition as
an error, and never assume lease expiry stops stale work. Redis does not offer
strong fencing across failover. etcd uses its native Lock/Lease API and returns
an epoch-scoped creation-revision fence; the protected resource must enforce it
atomically. See [`../typescript/README.md`](../typescript/README.md) for the
backend profiles and package-consumer gates. The same quick-start/explicit
policy distinction applies to etcd; `EtcdProvider.connect` accepts endpoint,
namespace, epoch, RPC and cleanup budgets, while `EtcdLock.acquire` accepts
wait/lease/RPC budgets and optional `autoRenew` and `AbortSignal` settings.
