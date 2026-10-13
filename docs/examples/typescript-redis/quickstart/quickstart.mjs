import { RedisProvider } from "@dilock/redis";

const env = (name, fallback) => process.env[name] || fallback;
const endpoint = env("DILOCK_REDIS_ENDPOINT", "redis://127.0.0.1:6379");
const epoch = env("DILOCK_EPOCH", "dev-1");
const configured = process.argv.includes("--configured");
const sharedProvider = process.argv.includes("--shared-provider");
const policy = { waitMs: 10_000, leaseMs: 60_000, rpcTimeoutMs: 2_000 };
const provider = await RedisProvider.connect(configured ? {
  endpoint, namespace: env("DILOCK_NAMESPACE", "reports"), epoch,
  rpcTimeoutMs: 2_000, cleanupTimeoutMs: 2_000,
  defaultAcquisition: policy,
  cleanupObserver: observation => console.error("cleanup", observation.outcome, observation.reason),
} : { endpoint, epoch });

try {
  const daily = provider.lock(configured ? "daily" : "reports/daily");
  if (sharedProvider) {
    const weekly = provider.lock("reports/weekly");
    await Promise.all([
      daily.withLease(async lease => { lease.assertValid(); console.log("daily work completed"); }),
      weekly.withLease(async lease => { lease.assertValid(); console.log("weekly work completed"); }),
    ]);
  } else if (configured) {
    await daily.withLease(async lease => { lease.assertValid(); await new Promise(resolve => setTimeout(resolve, 50)); console.log("configured protected Redis operation completed"); }, policy);
  } else {
    await daily.withLease(async lease => { lease.assertValid(); await new Promise(resolve => setTimeout(resolve, 50)); console.log("short protected Redis operation completed"); });
  }
} finally { await provider.close(); }
