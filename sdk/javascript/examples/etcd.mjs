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
} finally {
  await provider.close();
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must identify the pre-provisioned namespace epoch`);
  return value;
}

async function rebuildDailyReport(fence) {
  // Any write protected by this lock must atomically enforce the fence.
  void fence;
}
