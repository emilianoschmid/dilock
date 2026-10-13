import assert from "node:assert/strict";
import { EtcdProvider } from "@dilock/etcd";
import { AcquisitionTimeoutError, ReleaseUnknownError } from "@dilock/core";

const { DILOCK_ETCD_ENDPOINT: endpoint, DILOCK_ETCD_NAMESPACE: namespace,
  DILOCK_ETCD_EPOCH: epoch, DILOCK_PROXY_STATS_URL: statsUrl } = process.env;
assert(endpoint && namespace && epoch && statsUrl, "fault fixture environment is incomplete");
const dropRevokeAck = process.env.DILOCK_DROP_REVOKE_ACK === "1";
const expectedRelease = dropRevokeAck ? "release_unknown" : "already-not-owned";

const stats = async () => {
  const response = await fetch(statsUrl);
  assert.equal(response.status, 200);
  return response.json();
};

const provider = await EtcdProvider.connect({ endpoint, namespace, epoch });
try {
  const lock = provider.lock("typescript-applied-unlock-lost-ack");
  const owner = await lock.acquire({ waitMs: 5000, leaseMs: 15000, rpcTimeoutMs: 2000, autoRenew: false });
  const ownerFence = owner.fence;
  assert(ownerFence && BigInt(ownerFence.value) > 0n, "owner fence was not verified");

  if (dropRevokeAck) {
    await assert.rejects(owner.release(), error => error instanceof ReleaseUnknownError);
    assert.equal(owner.state, "release-unknown");
  } else {
    assert.equal(await owner.release(), "already-not-owned");
    assert.equal(owner.state, "lost");
  }
  const afterFirst = await stats();
  assert.equal(afterFirst.unlock_requests, 1);
  assert.equal(afterFirst.revoke_requests, 1);
  assert.equal(afterFirst.unlock_applied, 1);
  assert.equal(afterFirst.revoke_applied, 1);
  assert.equal(afterFirst.unlock_withheld, 1);
  assert.equal(afterFirst.revoke_withheld, dropRevokeAck ? 1 : 0);
  assert.equal(afterFirst.revoke_acknowledged, dropRevokeAck ? 0 : 1);

  if (dropRevokeAck) {
    await assert.rejects(owner.release(), error => error instanceof ReleaseUnknownError);
  } else {
    assert.equal(await owner.release(), "already-not-owned");
  }
  assert.deepEqual(await stats(), afterFirst, "repeat release must not issue another owner cleanup RPC");

  const successor = await lock.acquire({ waitMs: 5000, leaseMs: 15000, rpcTimeoutMs: 2000, autoRenew: false });
  assert(successor.fence && BigInt(successor.fence.value) > BigInt(ownerFence.value), "successor fence did not increase");

  const beforeOldRepeatAfterSuccessor = await stats();
  if (dropRevokeAck) {
    await assert.rejects(owner.release(), error => error instanceof ReleaseUnknownError);
  } else {
    assert.equal(await owner.release(), "already-not-owned");
  }
  assert.deepEqual(await stats(), beforeOldRepeatAfterSuccessor, "old-owner release must not replay cleanup against successor");

  await assert.rejects(
    lock.acquire({ waitMs: 250, leaseMs: 15000, rpcTimeoutMs: 2000, autoRenew: false }),
    error => error instanceof AcquisitionTimeoutError,
    "old-owner cleanup must not release the successor's native key",
  );
  assert.equal(await successor.release(), "released");

  const finalStats = await stats();
  assert.equal(finalStats.unlock_requests, 2, "only owner plus successor Unlock are expected");
  assert.equal(finalStats.revoke_requests, 3, "owner, timed-out contender, and successor Revoke are expected");
  assert.equal(finalStats.unlock_applied, 2);
  assert.equal(finalStats.revoke_applied, 3);
  assert.equal(finalStats.revoke_acknowledged, dropRevokeAck ? 2 : 3);
  assert.equal(finalStats.unlock_withheld, 1);
  assert.equal(finalStats.revoke_withheld, dropRevokeAck ? 1 : 0);
  console.log(JSON.stringify({
    status: "PASS",
    first_release: expectedRelease,
    repeat_release: expectedRelease,
    revoke_acknowledged: !dropRevokeAck,
    owner_state: owner.state,
    successor_acquired: true,
    successor_fence_greater: true,
    successor_preserved_until_release: true,
    successor_release: "released",
    proxy: finalStats,
  }));
} finally {
  await provider.close();
}
