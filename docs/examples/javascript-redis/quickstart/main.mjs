import { RedisProvider } from "@dilock/redis";

const provider = await RedisProvider.connect({
  endpoint: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  namespace: process.env.DILOCK_NAMESPACE ?? "default",
  epoch: required("DILOCK_COORDINATION_EPOCH"),
});

try {
  await provider.lock("reports/daily").withLease(async (lease) => {
    lease.assertValid();
    await rebuildDailyReport();
  });
  // Same provider/client, distinct resource, explicit wait/lease/RPC budgets.
  await provider.lock("reports/monthly").withLease(async (lease) => {
    lease.assertValid();
    await rebuildMonthlyReport();
  }, { waitMs: 12_000, leaseMs: 45_000, rpcTimeoutMs: 2_000 });
} finally {
  await provider.close();
}

async function rebuildDailyReport() {
  // Replace with one bounded, idempotent unit of work.
}

async function rebuildMonthlyReport() {}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must match provisioned namespace metadata`);
  return value;
}
