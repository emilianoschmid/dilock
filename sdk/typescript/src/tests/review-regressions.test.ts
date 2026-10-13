import assert from "node:assert/strict";
import test from "node:test";
import { AcquisitionTimeoutError, AcquisitionUnknownError, BackendUnavailableError, InvalidArgumentError, LeaseLostError } from "@dilock/core";
import { endpointTransport } from "../../packages/etcd/src/internal/endpoint.js";
import { closeOnce } from "../../packages/etcd/src/internal/close-once.js";
import { EtcdLock, EtcdProvider, type EtcdProvider as EtcdProviderType } from "@dilock/etcd";
import { RedisLease, RedisLock, RedisProvider, type RedisProvider as RedisProviderType } from "@dilock/redis";

test("etcd provider shutdown is single-flight and every caller awaits completion", async () => {
  let finish!: () => void;
  let shutdownCalls = 0;
  let completed = false;
  const shutdown = closeOnce(async () => {
    shutdownCalls++;
    await new Promise<void>(resolve => { finish = resolve; });
    completed = true;
  });

  const first = shutdown();
  const second = shutdown();
  assert.equal(shutdownCalls, 1);
  assert.equal(completed, false);
  finish();
  await Promise.all([first, second]);
  assert.equal(completed, true);
  assert.equal(shutdown(), first);
  assert.equal(shutdownCalls, 1);
});

test("etcd rejects non-boolean autoRenew policy before starting a lease attempt", async () => {
  let acquireCalls = 0;
  const provider = {
    defaultAcquireOptions: () => ({ waitMs: 1000, leaseMs: 3000, rpcTimeoutMs: 100, autoRenew: "false" }),
    async acquire() { acquireCalls++; throw new Error("must not start an RPC"); },
  } as unknown as EtcdProviderType;
  const lock = new EtcdLock(provider, "runtime-validation");

  await assert.rejects(lock.acquire(), InvalidArgumentError);
  await assert.rejects(lock.acquire({ waitMs: 1000, leaseMs: 3000, rpcTimeoutMs: 100, autoRenew: "false" as unknown as boolean }), InvalidArgumentError);
  assert.equal(acquireCalls, 0);

  assert.throws(() => new (EtcdProvider as any)({
    endpoint: "http://127.0.0.1:2379",
    epoch: "test-epoch",
    defaultAcquisition: { waitMs: 1000, leaseMs: 3000, rpcTimeoutMs: 100, autoRenew: "false" },
  }), InvalidArgumentError);
});

test("etcd endpoint scheme and TLS selection come from one normalized URL", () => {
  assert.deepEqual(endpointTransport("https://etcd.example:2379"), {
    target: "etcd.example:2379", secure: true,
  });
  assert.deepEqual(endpointTransport("HTTPS://etcd.example:2379"), {
    target: "etcd.example:2379", secure: true,
  });
  assert.deepEqual(endpointTransport("HtTpS://etcd.example:2379"), {
    target: "etcd.example:2379", secure: true,
  });
  assert.deepEqual(endpointTransport("etcd.example:2379"), {
    target: "etcd.example:2379", secure: false,
  });
});

test("Redis acquire caps attempt RPC budget and cleans up success after total wait", async () => {
  let rpcBudget = 0;
  let cleanupCount = 0;
  const lateLease = {};
  const provider = {
    async acquire(_resource: string, _leaseMs: number, rpcMs: number) {
      rpcBudget = rpcMs;
      await new Promise(resolve => setTimeout(resolve, 30));
      return lateLease;
    },
    async cleanupLateAcquisition(lease: object) {
      assert.equal(lease, lateLease);
      cleanupCount++;
    },
  } as unknown as RedisProvider;
  const lock = new RedisLock(provider, "late-response");

  await assert.rejects(lock.acquire({ waitMs: 10, leaseMs: 1000, rpcTimeoutMs: 500 }), AcquisitionTimeoutError);
  assert.ok(rpcBudget <= 10, `attempt RPC budget was ${rpcBudget} ms`);
  assert.equal(cleanupCount, 1);
});

test("Redis provider close aborts and drains an in-flight acquisition before closing its client", async () => {
  const events: string[] = [];
  let markCleanupStarted!: () => void;
  let finishCleanup!: () => void;
  const cleanupStarted = new Promise<void>(resolve => { markCleanupStarted = resolve; });
  const client = {
    withAbortSignal(signal: AbortSignal) {
      return {
        set: () => new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        }),
        eval: async () => {
          events.push("owner-cleanup");
          markCleanupStarted();
          return await new Promise<number>(resolve => { finishCleanup = () => resolve(1); });
        },
      };
    },
    async close() { events.push("client-close"); },
    destroy() { events.push("client-destroy"); },
  };
  const provider = new (RedisProvider as any)(client, {
    endpoint: "redis://unused",
    epoch: "test-epoch",
    rpcTimeoutMs: 100,
    cleanupTimeoutMs: 100,
  }) as RedisProviderType;

  const acquisition = provider.acquire("in-flight", 1000, 50);
  const closing = provider.close();
  const concurrentClose = provider.close();
  await assert.rejects(acquisition, AcquisitionUnknownError);
  await cleanupStarted;
  assert.deepEqual(events, ["owner-cleanup"], "client must remain open while owner cleanup is pending");
  finishCleanup();
  await Promise.all([closing, concurrentClose]);

  assert.deepEqual(events, ["owner-cleanup", "client-close"]);
  assert.throws(() => provider.acquire("after-close", 1000, 50), InvalidArgumentError);
  await provider.close();
  assert.deepEqual(events, ["owner-cleanup", "client-close"]);
});

test("Redis renewal backend failure permanently loses the local lease generation", async () => {
  let renewalCalls = 0;
  const provider = {
    async renew() {
      renewalCalls++;
      throw new BackendUnavailableError("renewal reply was not confirmed");
    },
  } as unknown as RedisProvider;
  const lease = new RedisLease(provider, "lease-key", "owner-token", 5000,
    performance.now() + 5000, 100, 100);

  await assert.rejects(lease.renew(), BackendUnavailableError);
  assert.equal(lease.state, "lost");
  await assert.rejects(lease.renew(), LeaseLostError);
  assert.equal(renewalCalls, 1, "a terminal lease generation must not retry renewal");
});
