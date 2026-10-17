# Dilock Consul adapter for TypeScript

`@dilock/consul` is the optional Node.js adapter for the `consul-kv-session-v1` profile. It uses Node's built-in `fetch`; it adds no Consul SDK dependency. Create namespace metadata explicitly before connecting. Lock entries must be retained so Consul's per-key `LockIndex` cannot reset.

```ts
import { ConsulProvider } from "@dilock/consul";

const provider = await ConsulProvider.connect({
  endpoint: process.env.CONSUL_HTTP_ADDR ?? "http://127.0.0.1:8500",
  namespace: "billing",
  epoch: process.env.DILOCK_COORDINATION_EPOCH!,
  token: process.env.CONSUL_HTTP_TOKEN,
});
try {
  await provider.lock("invoice/42/reconcile").withLease(async lease => {
    lease.assertValid();
    const fence = lease.fence; // { epoch, lockIndex }
    // The protected resource must atomically reject stale fences.
    console.log(fence);
  }, { waitMs: 5_000, leaseMs: 30_000 });
} finally {
  await provider.close();
}
```

Sessions use `Behavior=release`, zero lock delay, and renewal while queued and held. Acquisitions are confirmed with a consistent KV read. Fencing depends on the operator retaining lock entries; no automatic failover, quorum, or resource-side effect guarantee is implied. See [`spec/backends/CONSUL.md`](../../../../spec/backends/CONSUL.md).
