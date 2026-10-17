import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { ConsulProvider } from "@dilock/consul";

const endpoint = process.env.DILOCK_CONSUL_ENDPOINT;
test("Consul session lifecycle returns and releases a positive LockIndex fence", { skip: !endpoint }, async () => {
  if (!endpoint) return;
  const namespace = process.env.DILOCK_CONSUL_NAMESPACE;
  const epoch = process.env.DILOCK_CONSUL_EPOCH;
  if (!namespace || !epoch) throw new Error("Consul live test requires explicitly provisioned namespace and epoch");
  const token = process.env.CONSUL_HTTP_TOKEN;
  const provider = await ConsulProvider.connect({ endpoint, namespace, epoch, ...(token ? { token } : {}) });
  try {
    const lease = await provider.lock(`typescript/live/${randomUUID()}`).acquire({ waitMs: 5_000, leaseMs: 12_000 });
    try {
      lease.assertValid();
      assert.equal(lease.fence.epoch, epoch);
      assert.ok(lease.fence.lockIndex > 0);
    } finally {
      assert.equal(await lease.release(), "released");
    }
  } finally { await provider.close(); }
});
