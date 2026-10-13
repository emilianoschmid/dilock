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
} finally {
  await provider.close();
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must identify the pre-provisioned namespace epoch`);
  return value;
}

async function rebuildDailyReport() {
  // Replace with one bounded, idempotent unit of application work.
}
