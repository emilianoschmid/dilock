import { EtcdProvider } from "@dilock/etcd";

const env = (name, fallback) => process.env[name] || fallback;
const endpoint = env("DILOCK_ETCD_ENDPOINT", "http://127.0.0.1:2379");
const epoch = env("DILOCK_EPOCH", "dev-1");
const configured = process.argv.includes("--configured");
const sharedProvider = process.argv.includes("--shared-provider");
const policy = { waitMs: 10_000, leaseMs: 60_000, rpcTimeoutMs: 2_000, autoRenew: false };
const provider = await EtcdProvider.connect(configured ? {
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
      daily.withLease(async lease => { lease.assertValid(); console.log("daily create_revision", lease.createRevision); }),
      weekly.withLease(async lease => { lease.assertValid(); console.log("weekly create_revision", lease.createRevision); }),
    ]);
  } else if (configured) {
    await daily.withLease(async lease => { lease.assertValid(); console.log("configured etcd create_revision", lease.createRevision); }, policy);
  } else {
    await daily.withLease(async lease => { lease.assertValid(); console.log("short protected etcd operation at revision", lease.createRevision); });
  }
} finally { await provider.close(); }
