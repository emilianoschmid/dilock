import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { createClient } from "redis";
import { AcquisitionUnknownError, identity, redisKey } from "@dilock/core";
import { provisionNamespace, RedisProvider } from "@dilock/redis";

const endpoint = process.env.DILOCK_TEST_REDIS_URL;

test("Redis uses independent providers for contention, expiry, stale release and explicit renewal", { skip: !endpoint }, async () => {
  const suffix = randomUUID();
  const namespace = `typescript-live-${suffix}`;
  const epoch = `local-test-${suffix}`;
  const metadataKey = `dilock:namespace:${namespace}:meta`;
  const admin = createClient({ url: endpoint!, disableOfflineQueue: true, socket: { reconnectStrategy: false } });
  admin.on("error", () => undefined);
  let firstProvider: RedisProvider | undefined;
  let secondProvider: RedisProvider | undefined;
  try {
    await admin.connect();
    assert.equal(await provisionNamespace(admin, namespace, epoch), "created");
    // Bound abandoned test metadata if the runner or process exits unexpectedly.
    await admin.pExpire(metadataKey, 120_000);
    firstProvider = await RedisProvider.connect({ endpoint: endpoint!, namespace, epoch });
    secondProvider = await RedisProvider.connect({ endpoint: endpoint!, namespace, epoch });

    const resource = `redis-live/${suffix}/handoff`;
    const firstLock = firstProvider.lock(resource);
    const secondLock = secondProvider.lock(resource);
    const first = await firstLock.acquire({ waitMs: 2000, leaseMs: 1600, rpcTimeoutMs: 100 });
    assert.equal(await secondLock.tryAcquire({ leaseMs: 1600, rpcTimeoutMs: 100 }), undefined);
    const waiting = secondLock.acquire({ waitMs: 4000, leaseMs: 1600, rpcTimeoutMs: 100 });
    await delay(100);
    assert.equal(await first.release(), "released");
    const successor = await waiting;
    assert.equal(successor.state, "held");
    assert.equal(await successor.release(), "released");

    const expiryResource = `redis-live/${suffix}/expiry`;
    const expired = await firstProvider.lock(expiryResource).acquire({ waitMs: 2000, leaseMs: 500, rpcTimeoutMs: 50 });
    await delay(800);
    const afterExpiry = await secondProvider.lock(expiryResource).acquire({ waitMs: 3000, leaseMs: 1200, rpcTimeoutMs: 100 });
    assert.equal(expired.state, "lost");
    assert.equal(await expired.release(), "already-not-owned");
    assert.equal(afterExpiry.state, "held");
    assert.equal(await secondProvider.lock(expiryResource).tryAcquire({ leaseMs: 1200, rpcTimeoutMs: 100 }), undefined);
    await afterExpiry.release();

    const renewalResource = `redis-live/${suffix}/renewal`;
    const renewable = await firstProvider.lock(renewalResource).acquire({ waitMs: 2000, leaseMs: 500, rpcTimeoutMs: 50 });
    await delay(200);
    await renewable.renew(1200);
    await delay(550);
    renewable.assertValid();
    assert.equal(await secondProvider.lock(renewalResource).tryAcquire({ leaseMs: 1200, rpcTimeoutMs: 100 }), undefined);
    await renewable.release();
  } finally {
    await Promise.allSettled([firstProvider?.close(), secondProvider?.close()].filter((value): value is Promise<void> => value !== undefined));
    if (admin.isOpen) {
      const raw = await admin.get(metadataKey).catch(() => null);
      if (raw !== null) {
        let matchesEpoch = false;
        try { matchesEpoch = (JSON.parse(raw) as { coordination_epoch?: unknown }).coordination_epoch === epoch; } catch { /* Leave unexpected metadata untouched. */ }
        if (matchesEpoch) {
          await admin.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0", { keys: [metadataKey], arguments: [raw] }).catch(() => undefined);
        }
      }
      await admin.close().catch(() => admin.destroy());
    }
  }
});

const appliedSetProxyEndpoint = process.env.DILOCK_TEST_REDIS_APPLIED_SET_PROXY_URL;
const appliedSetVerifyEndpoint = process.env.DILOCK_TEST_REDIS_APPLIED_SET_VERIFY_URL;
const appliedSetControlEndpoint = process.env.DILOCK_TEST_REDIS_APPLIED_SET_CONTROL_URL;
const appliedSetConfigured = Boolean(appliedSetProxyEndpoint && appliedSetVerifyEndpoint && appliedSetControlEndpoint);

async function appliedSetProxyStats(controlEndpoint: string, command: "RESET" | "STATS"): Promise<Record<string, unknown>> {
  const endpoint = new URL(controlEndpoint);
  assert.equal(endpoint.protocol, "tcp:");
  assert.ok(endpoint.hostname && endpoint.port);
  return await new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: endpoint.hostname, port: Number(endpoint.port) }, () => socket.write(`${command}\n`));
    let response = "";
    socket.setTimeout(2000, () => socket.destroy(new Error("proxy control request timed out")));
    socket.on("data", chunk => {
      response += chunk.toString("utf8");
      const newline = response.indexOf("\n");
      if (newline >= 0) {
        socket.end();
        try { resolve(JSON.parse(response.slice(0, newline)) as Record<string, unknown>); }
        catch (error) { reject(error); }
      }
    });
    socket.on("error", reject);
  });
}

test("Redis cancellation after applied SET NX PX remains unknown and owner cleanup removes the lease", { skip: !appliedSetConfigured }, async () => {
  const suffix = randomUUID();
  const namespace = `typescript-cancel-set-${suffix}`;
  const epoch = `typescript-cancel-epoch-${suffix}`;
  const metadataKey = `dilock:namespace:${namespace}:meta`;
  const resource = `redis-cancel-applied-set/${suffix}`;
  const lockKey = redisKey(identity(namespace, resource));
  const admin = createClient({ url: appliedSetVerifyEndpoint!, disableOfflineQueue: true, socket: { reconnectStrategy: false } });
  admin.on("error", () => undefined);
  let provider: RedisProvider | undefined;
  try {
    await admin.connect();
    assert.equal(await provisionNamespace(admin, namespace, epoch), "created");
    await admin.pExpire(metadataKey, 120_000);
    await appliedSetProxyStats(appliedSetControlEndpoint!, "RESET");
    provider = await RedisProvider.connect({ endpoint: appliedSetProxyEndpoint!, namespace, epoch });

    const controller = new AbortController();
    const attempt = provider.lock(resource).tryAcquire({ leaseMs: 5000, rpcTimeoutMs: 2000, signal: controller.signal });
    const waitDeadline = Date.now() + 3000;
    let stats: Record<string, unknown> = {};
    while (Date.now() < waitDeadline) {
      stats = await appliedSetProxyStats(appliedSetControlEndpoint!, "STATS");
      if (stats.delayed_set_count === 1) break;
      await delay(10);
    }
    assert.equal(stats.delayed_set_count, 1, `proxy did not observe an applied SET NX PX: ${JSON.stringify(stats)}`);

    controller.abort(new DOMException("test cancellation after applied SET", "AbortError"));
    await assert.rejects(attempt, AcquisitionUnknownError);
    await provider.close();
    provider = undefined;

    stats = await appliedSetProxyStats(appliedSetControlEndpoint!, "STATS");
    assert.equal(stats.delayed_set_count, 1, `acquisition was retried: ${JSON.stringify(stats)}`);
    assert.equal(stats.eval_count, 1, `expected one owner-checked cleanup EVAL: ${JSON.stringify(stats)}`);
    assert.equal(stats.dropped_count, 0, `a command reply was unexpectedly dropped: ${JSON.stringify(stats)}`);
    assert.equal(await admin.get(lockKey), null, "owner cleanup left the applied lease key behind");
    console.log("PASS: Redis applied one SET NX PX, cancellation returned acquisition-unknown, one owner cleanup EVAL removed the key");
  } finally {
    await provider?.close();
    if (admin.isOpen) {
      const raw = await admin.get(metadataKey).catch(() => null);
      if (raw !== null) {
        let matchesEpoch = false;
        try { matchesEpoch = (JSON.parse(raw) as { coordination_epoch?: unknown }).coordination_epoch === epoch; } catch { /* Leave unexpected metadata untouched. */ }
        if (matchesEpoch) {
          await admin.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0", { keys: [metadataKey], arguments: [raw] }).catch(() => undefined);
        }
      }
      await admin.close().catch(() => admin.destroy());
    }
  }
});
