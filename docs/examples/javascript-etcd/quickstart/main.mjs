import { EtcdProvider } from "@dilock/etcd";

const provider = await EtcdProvider.connect({
  endpoint: process.env.ETCD_URL ?? "http://127.0.0.1:2379",
  namespace: process.env.DILOCK_NAMESPACE ?? "default",
  epoch: required("DILOCK_COORDINATION_EPOCH"),
});

try {
  await provider.lock("reports/daily").withLease(async (lease) => {
    lease.assertValid();
    const fence = lease.fence;
    await rebuildDailyReport(fence);
  });
  // Reuses the provider channel; this is another, independent acquisition.
  await provider.lock("reports/monthly").withLease(async (lease) => {
    lease.assertValid();
    await rebuildMonthlyReport(lease.fence);
  }, { waitMs: 12_000, leaseMs: 45_000, rpcTimeoutMs: 2_000, autoRenew: true });
} finally {
  await provider.close();
}

async function rebuildDailyReport(fence) {
  // Atomically enforce the epoch/revision fence at the protected resource.
  void fence;
}

async function rebuildMonthlyReport(fence) { void fence; }

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must match provisioned namespace metadata`);
  return value;
}
