import assert from "node:assert/strict";
import test from "node:test";
import {
  AcquisitionTimeoutError,
  InvalidArgumentError,
  LeaseLostError,
  ReleaseUnknownError,
  identity,
  redisKey,
} from "@dilock/core";
import { RedisLock, RedisProvider } from "@dilock/redis";
import { EtcdLock, EtcdProvider, UnsupportedCapabilityError } from "@dilock/etcd";
import { ConsulLock, ConsulProvider } from "@dilock/consul";

test("public JavaScript imports reuse the TypeScript runtime packages", () => {
  assert.equal(typeof RedisProvider.connect, "function");
  assert.equal(typeof EtcdProvider.connect, "function");
  assert.equal(typeof ConsulProvider.connect, "function");
  assert.equal(typeof ConsulLock.prototype.acquire, "function");
  assert.equal(typeof AcquisitionTimeoutError, "function");
  assert.equal(typeof UnsupportedCapabilityError, "function");
});

test("the shared package preserves the canonical identity and Redis key", () => {
  const id = identity("default", "camera/123/reindex");
  assert.equal(id, "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6");
  assert.equal(redisKey(id), `dilock:{${id}}:lease`);
});

test("the shared runtime rejects invalid resource text", () => {
  assert.throws(() => identity("default", ""), TypeError);
  assert.throws(() => identity("default", "bad\0resource"), TypeError);
  assert.throws(() => identity("default", "\ud800"), TypeError);
});

test("the public JavaScript Redis facade uses configured default lease and RPC budgets", async () => {
  const defaults = { waitMs: 12_000, leaseMs: 45_000, rpcTimeoutMs: 1_500 };
  let received;
  const provider = {
    defaultAcquireOptions: () => defaults,
    async acquire(resource, leaseMs, rpcTimeoutMs) {
      received = { resource, leaseMs, rpcTimeoutMs };
      return { async release() { return "released"; } };
    },
  };

  await new RedisLock(provider, "javascript/default-policy").acquire();

  assert.deepEqual(received, {
    resource: "javascript/default-policy",
    leaseMs: defaults.leaseMs,
    rpcTimeoutMs: defaults.rpcTimeoutMs,
  });
});

test("the public JavaScript Redis facade enforces its configured default wait budget", async () => {
  let attempts = 0;
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 25, leaseMs: 45_000, rpcTimeoutMs: 1_500 }),
    async acquire() {
      attempts++;
      if (attempts === 1) {
        await new Promise(resolve => setTimeout(resolve, 40));
        return undefined;
      }
      return { async release() { return "released"; } };
    },
  };

  await assert.rejects(
    new RedisLock(provider, "javascript/default-wait").acquire(),
    AcquisitionTimeoutError,
  );
  assert.equal(attempts, 1);
});

test("the public JavaScript facade rejects when release is unknown after successful work", async () => {
  const redisFailure = new ReleaseUnknownError("Redis release acknowledgement lost");
  const redisProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000 }),
    async acquire() { return { async release() { throw redisFailure; } }; },
    observeCleanup() {},
  };
  await assert.rejects(new RedisLock(redisProvider, "javascript/scope").withLease(() => "done"), error => error === redisFailure);

  const etcdFailure = new ReleaseUnknownError("etcd release acknowledgement lost");
  const etcdProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000, autoRenew: false }),
    async acquire() { return { async release() { throw etcdFailure; } }; },
    observeCleanup() {},
  };
  await assert.rejects(new EtcdLock(etcdProvider, "javascript/scope").withLease(() => "done"), error => error === etcdFailure);
});

test("the public JavaScript facade preserves work errors and observes unknown release for both backends", async () => {
  const workFailure = new Error("application work failed");
  const redisReleaseFailure = new ReleaseUnknownError("Redis release acknowledgement lost");
  const redisObservations = [];
  const redisProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000 }),
    async acquire() { return { async release() { throw redisReleaseFailure; } }; },
    observeCleanup(reason) { redisObservations.push(reason); },
  };
  await assert.rejects(
    new RedisLock(redisProvider, "javascript/scope").withLease(() => { throw workFailure; }),
    error => error === workFailure,
  );
  assert.deepEqual(redisObservations, ["with-lease-after-work-error"]);

  const etcdReleaseFailure = new ReleaseUnknownError("etcd release acknowledgement lost");
  const etcdObservations = [];
  const etcdProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000, autoRenew: false }),
    async acquire() { return { async release() { throw etcdReleaseFailure; } }; },
    observeCleanup(reason) { etcdObservations.push(reason); },
  };
  await assert.rejects(
    new EtcdLock(etcdProvider, "javascript/scope").withLease(() => { throw workFailure; }),
    error => error === workFailure,
  );
  assert.deepEqual(etcdObservations, ["with-lease-after-work-error"]);
});

test("the public JavaScript facade rejects an already-not-owned scope release", async () => {
  const redisProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000 }),
    async acquire() { return { async release() { return "already-not-owned"; } }; },
    observeCleanup() {},
  };
  await assert.rejects(
    new RedisLock(redisProvider, "javascript/scope").withLease(() => "done"),
    LeaseLostError,
  );

  const etcdProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000, autoRenew: false }),
    async acquire() { return { async release() { return "already-not-owned"; } }; },
    observeCleanup() {},
  };
  await assert.rejects(
    new EtcdLock(etcdProvider, "javascript/scope").withLease(() => "done"),
    LeaseLostError,
  );
});

test("the public JavaScript facade rejects invalid callbacks before backend acquisition", async () => {
  let redisAcquisitions = 0;
  const redisProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000 }),
    async acquire() { redisAcquisitions++; throw new Error("backend must not be called"); },
  };
  await assert.rejects(new RedisLock(redisProvider, "javascript/scope").withLease(null), InvalidArgumentError);
  assert.equal(redisAcquisitions, 0);

  let etcdAcquisitions = 0;
  const etcdProvider = {
    defaultAcquireOptions: () => ({ waitMs: 5000, leaseMs: 30000, rpcTimeoutMs: 2000, autoRenew: false }),
    async acquire() { etcdAcquisitions++; throw new Error("backend must not be called"); },
  };
  await assert.rejects(new EtcdLock(etcdProvider, "javascript/scope").withLease(0), InvalidArgumentError);
  assert.equal(etcdAcquisitions, 0);
});
