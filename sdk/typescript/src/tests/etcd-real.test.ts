import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { EtcdAcquisitionCancelledError, EtcdProvider } from "@dilock/etcd";
import { AcquisitionTimeoutError, UnsupportedCapabilityError } from "@dilock/core";

const endpoint = process.env.DILOCK_ETCD_ENDPOINT;
const namespace = process.env.DILOCK_ETCD_NAMESPACE;
const epoch = process.env.DILOCK_ETCD_EPOCH;
const configured = Boolean(endpoint && namespace && epoch);

test("etcd native queue keeper, revision fencing and cancellation cleanup", { skip: !configured }, async () => {
  const provider = await EtcdProvider.connect({ endpoint: endpoint!, namespace: namespace!, epoch: epoch! });
  const suffix = randomUUID();
  try {
    const lock = provider.lock(`typescript-smoke/queue/${suffix}`);
    await assert.rejects(lock.tryAcquire(), UnsupportedCapabilityError);
    const first = await lock.acquire({ waitMs: 5000, leaseMs: 3000, rpcTimeoutMs: 2000, autoRenew: true });
    const waiting = lock.acquire({ waitMs: 9000, leaseMs: 3000, rpcTimeoutMs: 2000, autoRenew: false });
    await delay(3800);
    first.assertValid();
    await first.release();
    const successor = await waiting;
    assert.ok(BigInt(successor.fence!.value) > BigInt(first.fence!.value));
    await successor.release();

    const held = await provider.lock(`typescript-smoke/cancel/${suffix}`).acquire({ waitMs: 5000, leaseMs: 3000, rpcTimeoutMs: 2000, autoRenew: false });
    const controller = new AbortController();
    const queued = provider.lock(`typescript-smoke/cancel/${suffix}`).acquire({ waitMs: 9000, leaseMs: 3000, rpcTimeoutMs: 2000, autoRenew: false, signal: controller.signal });
    await delay(250);
    controller.abort();
    await assert.rejects(queued, EtcdAcquisitionCancelledError);
    await held.release();
    const reacquired = await provider.lock(`typescript-smoke/cancel/${suffix}`).acquire({ waitMs: 5000, leaseMs: 3000, rpcTimeoutMs: 2000, autoRenew: false });
    await reacquired.release();
  } finally { await provider.close(); }
});

test("etcd autoRenew=false expires after acquisition without disturbing its successor", { skip: !configured }, async () => {
  const provider = await EtcdProvider.connect({ endpoint: endpoint!, namespace: namespace!, epoch: epoch! });
  try {
    const lock = provider.lock(`typescript-smoke/expiry/${randomUUID()}`);
    const first = await lock.acquire({ waitMs: 5000, leaseMs: 1000, rpcTimeoutMs: 200, autoRenew: false });
    const firstFence = BigInt(first.fence!.value);
    await delay(1800);
    const successor = await lock.acquire({ waitMs: 5000, leaseMs: 3000, rpcTimeoutMs: 200, autoRenew: false });
    assert.ok(BigInt(successor.fence!.value) > firstFence);
    assert.equal(await first.release(), "already-not-owned");
    assert.equal(successor.state, "held");
    await assert.rejects(
      lock.acquire({ waitMs: 300, leaseMs: 1000, rpcTimeoutMs: 200, autoRenew: false }),
      AcquisitionTimeoutError,
      "the expired owner's acknowledged Unlock must not release the successor's opaque key",
    );
    await successor.release();
  } finally { await provider.close(); }
});

