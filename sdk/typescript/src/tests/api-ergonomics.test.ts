import assert from "node:assert/strict";
import test from "node:test";
import { RedisLease, RedisLock, RedisProvider, type RedisProvider as RedisProviderType } from "@dilock/redis";
import { InvalidArgumentError, LeaseLostError, ReleaseUnknownError } from "@dilock/core";
import { EtcdLease, EtcdLock, EtcdProvider, type EtcdProvider as EtcdProviderType } from "@dilock/etcd";

test("Redis withLease preserves body errors and reports unknown cleanup", async () => {
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 123, leaseMs: 456, rpcTimeoutMs: 100 }),
    async acquire(_resource: string, leaseMs: number) {
      assert.equal(leaseMs, 456);
      return { async release() { throw new ReleaseUnknownError("uncertain cleanup"); } };
    },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as RedisProviderType;
  const workError = new Error("work failed");
  await assert.rejects(new RedisLock(provider, "job").withLease(() => { throw workError; }), error => error === workError);
  assert.deepEqual(seen, ["with-lease-after-work-error"]);
});

test("etcd withLease preserves work errors and exposes unknown cleanup after success", async () => {
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 111, leaseMs: 6000, rpcTimeoutMs: 100, autoRenew: false }),
    async acquire(_resource: string, options: { waitMs: number; leaseMs: number }) {
      assert.equal(options.waitMs, 111);
      return { async release() { throw new ReleaseUnknownError("uncertain cleanup"); } };
    },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as EtcdProviderType;
  await assert.rejects(new EtcdLock(provider, "job").withLease(() => 42), ReleaseUnknownError);
  assert.deepEqual(seen, ["with-lease"]);
});

test("Redis withLease exposes unknown release after successful work", async () => {
  const releaseFailure = new ReleaseUnknownError("Redis release acknowledgement lost");
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 123, leaseMs: 456, rpcTimeoutMs: 100 }),
    async acquire() { return { async release() { throw releaseFailure; } }; },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as RedisProviderType;
  await assert.rejects(new RedisLock(provider, "job").withLease(() => 42), error => error === releaseFailure);
  assert.deepEqual(seen, ["with-lease"]);
});

test("etcd withLease exposes unknown release after successful work", async () => {
  const releaseFailure = new ReleaseUnknownError("etcd release acknowledgement lost");
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 111, leaseMs: 6000, rpcTimeoutMs: 100, autoRenew: false }),
    async acquire() { return { async release() { throw releaseFailure; } }; },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as EtcdProviderType;
  await assert.rejects(new EtcdLock(provider, "job").withLease(() => 42), error => error === releaseFailure);
  assert.deepEqual(seen, ["with-lease"]);
});

test("Redis withLease rejects already-not-owned release and preserves callback errors", async () => {
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 123, leaseMs: 456, rpcTimeoutMs: 100 }),
    async acquire() { return { async release() { return "already-not-owned" as const; } }; },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as RedisProviderType;
  await assert.rejects(new RedisLock(provider, "job").withLease(() => 42), LeaseLostError);
  assert.deepEqual(seen, ["with-lease"]);

  const workError = new Error("work failed");
  await assert.rejects(new RedisLock(provider, "job").withLease(() => { throw workError; }), error => error === workError);
  assert.deepEqual(seen, ["with-lease", "with-lease-after-work-error"]);
});

test("etcd withLease rejects already-not-owned release and preserves callback errors", async () => {
  const seen: string[] = [];
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 111, leaseMs: 6000, rpcTimeoutMs: 100, autoRenew: false }),
    async acquire() { return { async release() { return "already-not-owned" as const; } }; },
    observeCleanup(reason: string) { seen.push(reason); },
  } as unknown as EtcdProviderType;
  await assert.rejects(new EtcdLock(provider, "job").withLease(() => 42), LeaseLostError);
  assert.deepEqual(seen, ["with-lease"]);

  const workError = new Error("work failed");
  await assert.rejects(new EtcdLock(provider, "job").withLease(() => { throw workError; }), error => error === workError);
  assert.deepEqual(seen, ["with-lease", "with-lease-after-work-error"]);
});

test("withLease rejects non-function callbacks before backend acquisition", async () => {
  let redisAcquisitions = 0;
  const redisProvider = {
    defaultAcquireOptions: () => ({ waitMs: 100, leaseMs: 100, rpcTimeoutMs: 100 }),
    async acquire() { redisAcquisitions++; throw new Error("backend must not be called"); },
  } as unknown as RedisProviderType;
  await assert.rejects(new RedisLock(redisProvider, "job").withLease(null as any), InvalidArgumentError);
  assert.equal(redisAcquisitions, 0);

  let etcdAcquisitions = 0;
  const etcdProvider = {
    defaultAcquireOptions: () => ({ waitMs: 100, leaseMs: 100, rpcTimeoutMs: 100, autoRenew: false }),
    async acquire() { etcdAcquisitions++; throw new Error("backend must not be called"); },
  } as unknown as EtcdProviderType;
  await assert.rejects(new EtcdLock(etcdProvider, "job").withLease(0 as any), InvalidArgumentError);
  assert.equal(etcdAcquisitions, 0);
});

test("providers snapshot immutable defaults and suppress throwing cleanup observers", () => {
  const redisDefaults = { waitMs: 8_000, leaseMs: 45_000, rpcTimeoutMs: 1_000 };
  const redis = new (RedisProvider as any)({}, {
    endpoint: "redis://127.0.0.1:6379", epoch: "test", defaultAcquisition: redisDefaults,
    cleanupObserver() { throw new Error("observer failure"); },
  }) as RedisProviderType;
  redisDefaults.waitMs = 20_000;
  assert.equal(redis.defaultAcquireOptions().waitMs, 8_000);
  assert.throws(() => Object.assign(redis.defaultAcquireOptions(), { waitMs: 1 }));
  assert.doesNotThrow(() => redis.observeCleanup("scope-close"));

  const etcdDefaults = { waitMs: 7_000, leaseMs: 45_000, rpcTimeoutMs: 1_000, autoRenew: false };
  const etcd = new (EtcdProvider as any)({ endpoint: "http://127.0.0.1:2379", epoch: "test", defaultAcquisition: etcdDefaults,
    cleanupObserver() { throw new Error("observer failure"); },
  }) as EtcdProviderType;
  etcdDefaults.waitMs = 20_000;
  assert.equal(etcd.defaultAcquireOptions().waitMs, 7_000);
  assert.throws(() => Object.assign(etcd.defaultAcquireOptions(), { waitMs: 1 }));
  assert.doesNotThrow(() => etcd.observeCleanup("scope-close"));
  void etcd.close();
});

test("providers reject zero default acquisition waits before network use", () => {
  assert.throws(() => new (RedisProvider as any)({}, {
    endpoint: "redis://127.0.0.1:6379", epoch: "test",
    defaultAcquisition: { waitMs: 0, leaseMs: 30_000, rpcTimeoutMs: 2_000 },
  }), InvalidArgumentError);
  assert.throws(() => new (EtcdProvider as any)({
    endpoint: "http://127.0.0.1:2379", epoch: "test",
    defaultAcquisition: { waitMs: 0, leaseMs: 30_000, rpcTimeoutMs: 2_000, autoRenew: false },
  }), InvalidArgumentError);
});

test("locally lost leases still attempt owner-specific backend cleanup", async () => {
  let redisCleanup = 0;
  const redisProvider = { async release() { redisCleanup++; return "released" as const; } } as unknown as RedisProviderType;
  const redisLease = new RedisLease(redisProvider, "resource-key", "owner-token-test", 30_000, 0, 2_000, 2_000);
  assert.equal(await redisLease.release(), "released");
  assert.equal(redisCleanup, 1);

  let etcdCleanup = 0;
  const etcdProvider = { async release() { etcdCleanup++; return "released" as const; } } as unknown as EtcdProviderType;
  const etcdLease = new EtcdLease(etcdProvider, "1", 30_000, Number.POSITIVE_INFINITY);
  etcdLease.activate(Buffer.from("verified-native-key"), "1");
  etcdLease.markLost();
  assert.equal(await etcdLease.release(), "released");
  assert.equal(etcdCleanup, 1);
});


test("explicit TypeScript acquisition policies do not merge provider defaults", async () => {
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 5_000, leaseMs: 30_000, rpcTimeoutMs: 2_000 }),
    async acquire() { throw new Error("backend must not be called for an incomplete policy"); },
  } as unknown as RedisProviderType;
  const lock = new RedisLock(provider, "job");
  await assert.rejects(lock.acquire({ waitMs: 1_000, leaseMs: 1_000 } as any), InvalidArgumentError);
});
