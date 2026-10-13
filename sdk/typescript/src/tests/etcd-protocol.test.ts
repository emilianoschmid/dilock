import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import { resolve } from "node:path";
import test from "node:test";
import { EtcdLease, type EtcdProvider } from "@dilock/etcd";
import { boundedEtcdCleanup, classifyEtcdLockFailure, classifyEtcdRelease, classifyEtcdReleaseOwnership, etcdCleanupConfirmed } from "../../packages/etcd/src/internal/etcd-lock-failure.js";
import { isEtcdWaitBudgetExpired, remainingEtcdWaitBudget } from "../../packages/etcd/src/etcd.js";

const require = createRequire(import.meta.url);
const grpc = require("@grpc/grpc-js") as any;
const protoLoader = require("@grpc/proto-loader") as any;
const protocol = resolve(import.meta.dirname, "../../../packages/etcd/proto/etcd-v3.7.2/v3lock.proto");

test("etcd adapter uses the native v3 Lock and Lease gRPC service paths", () => {
  const definition = protoLoader.loadSync(protocol, { keepCase: true, longs: String, enums: String, defaults: false });
  const api = grpc.loadPackageDefinition(definition);
  assert.equal(api.v3lockpb.Lock.service.Lock.path, "/v3lockpb.Lock/Lock");
  assert.equal(api.v3lockpb.Lock.service.Unlock.path, "/v3lockpb.Lock/Unlock");
  assert.equal(api.etcdserverpb.KV.service.Range.path, "/etcdserverpb.KV/Range");
  assert.equal(api.etcdserverpb.Lease.service.LeaseGrant.path, "/etcdserverpb.Lease/LeaseGrant");
  assert.equal(api.etcdserverpb.Lease.service.LeaseKeepAlive.path, "/etcdserverpb.Lease/LeaseKeepAlive");
  assert.equal(api.etcdserverpb.Lease.service.LeaseRevoke.path, "/etcdserverpb.Lease/LeaseRevoke");
});

test("etcd lock protobuf preserves the upstream opaque-key and lease field numbers", () => {
  const definition = protoLoader.loadSync(protocol, { keepCase: true, longs: String, enums: String, defaults: false });
  const request = definition["v3lockpb.LockRequest"] as any;
  const response = definition["v3lockpb.LockResponse"] as any;
  const unlock = definition["v3lockpb.UnlockRequest"] as any;
  const fieldNumber = (descriptor: any, name: string) => descriptor.type.field.find((field: any) => field.name === name)?.number;
  assert.equal(fieldNumber(request, "name"), 1);
  assert.equal(fieldNumber(request, "lease"), 2);
  assert.equal(fieldNumber(response, "key"), 2);
  assert.equal(fieldNumber(unlock, "key"), 1);
});

test("etcd Lock cancellation is a timeout only at the local deadline after confirmed revoke", () => {
  const cancelled = { code: 1, details: "Call cancelled" };
  assert.equal(classifyEtcdLockFailure(cancelled, 100, 100, false, true), "timeout");
  assert.equal(classifyEtcdLockFailure(cancelled, 99, 100, false, true), "backend-unavailable");
  assert.equal(classifyEtcdLockFailure(cancelled, 100, 100, true, true), "cancelled");
  assert.equal(classifyEtcdLockFailure(cancelled, 100, 100, false, false), "unknown");
  assert.equal(classifyEtcdLockFailure({ code: 4 }, 20, 100, false, true), "timeout");
});

test("etcd acquisition cleanup is confirmed by either acknowledged Unlock or Revoke", () => {
  assert.equal(etcdCleanupConfirmed(true, true), true);
  assert.equal(etcdCleanupConfirmed(true, false), true, "Unlock acknowledgement proves the native lock key was removed");
  assert.equal(etcdCleanupConfirmed(false, true), true, "Revoke acknowledgement removes keys attached to the attempt lease");
  assert.equal(etcdCleanupConfirmed(false, false), false, "no cleanup acknowledgement leaves the remote outcome unknown");
});

test("etcd public release requires exact observed ownership and both cleanup acknowledgements", () => {
  assert.equal(classifyEtcdRelease("owned", true, true), "released");
  assert.equal(classifyEtcdRelease("owned", true, false), "unknown", "Unlock ACK alone does not confirm attempt-lease cleanup");
  assert.equal(classifyEtcdRelease("owned", false, true), "already-not-owned", "Revoke confirms the exact owned key is gone");
  assert.equal(classifyEtcdRelease("owned", false, false), "unknown");
  assert.equal(classifyEtcdRelease("not-owned", false, true), "already-not-owned");
  assert.equal(classifyEtcdRelease("not-owned", false, false), "unknown");
  assert.equal(classifyEtcdRelease("unknown", false, true), "unknown", "cleanup ACK does not erase an uncertain ownership observation");
  assert.equal(classifyEtcdRelease("unknown", true, true), "unknown", "Unlock is never sent without exact ownership proof");
});

test("etcd release ownership requires one exact opaque key attached to the attempt lease", () => {
  const key = Buffer.from("/dilock/locks/resource/opaque-attempt-key");
  assert.equal(classifyEtcdReleaseOwnership([{ key, lease: "17" }], key, "17"), "owned");
  assert.equal(classifyEtcdReleaseOwnership([], key, "17"), "not-owned");
  assert.equal(classifyEtcdReleaseOwnership([{ key: Buffer.from("/different-key"), lease: "17" }], key, "17"), "not-owned");
  assert.equal(classifyEtcdReleaseOwnership([{ key, lease: "18" }], key, "17"), "not-owned");
  assert.equal(classifyEtcdReleaseOwnership([{ key, lease: "17" }, { key, lease: "17" }], key, "17"), "not-owned");
  assert.equal(classifyEtcdReleaseOwnership([{ lease: "17" }], key, "17"), "not-owned");
});

test("repeated etcd release returns its cached acknowledged outcome without new RPCs", async () => {
  for (const outcome of ["released", "already-not-owned"] as const) {
    let calls = 0;
    const provider = {
      epochForLease: () => "test-epoch",
      release: async () => { calls++; return outcome; },
    } as unknown as EtcdProvider;
    const lease = new EtcdLease(provider, "lease-1", 3000, Number.POSITIVE_INFINITY);
    lease.activate(Buffer.from("opaque-key"), "1");

    assert.equal(await lease.release(), outcome);
    assert.equal(await lease.release(), outcome);
    assert.equal(calls, 1, "a terminal cleanup observation must not replay etcd RPCs");
  }
});
test("etcd Unlock and Revoke share one monotonic cleanup budget", async () => {
  let now = 100;
  const budgets: number[] = [];
  const result = await boundedEtcdCleanup(20, () => now, async timeoutMs => {
    budgets.push(timeoutMs);
    now += 13;
    throw new Error("Unlock acknowledgement lost");
  }, async timeoutMs => {
    budgets.push(timeoutMs);
    now += timeoutMs;
  });
  assert.deepEqual(budgets, [20, 7]);
  assert.equal(result.unlockAcknowledged, false);
  assert.equal(result.revokeAcknowledged, true);
});

test("etcd skips Revoke when Unlock exhausts the total cleanup budget", async () => {
  let now = 50;
  let revokeCalls = 0;
  const result = await boundedEtcdCleanup(10, () => now, async timeoutMs => {
    assert.equal(timeoutMs, 10);
    now += 10;
  }, async () => { revokeCalls++; });
  assert.equal(result.unlockAcknowledged, true);
  assert.equal(result.revokeAcknowledged, false);
  assert.equal(revokeCalls, 0);
});

test("etcd acquisition deadline rejects successful responses at or after the monotonic wait limit", () => {
  const deadline = 150;
  assert.equal(isEtcdWaitBudgetExpired(deadline, 149.999), false);
  assert.equal(isEtcdWaitBudgetExpired(deadline, 150), true);
  assert.equal(isEtcdWaitBudgetExpired(deadline, 150.001), true);
});

test("etcd grant, Lock and verification use the remaining total wait budget", () => {
  assert.equal(remainingEtcdWaitBudget(5000, 100, 100), 5000);
  assert.equal(remainingEtcdWaitBudget(5000, 100, 3100), 2000);
  assert.equal(remainingEtcdWaitBudget(5000, 100, 5100), 0);
});

test("a late etcd keepalive acknowledgement cannot revive an expired local lease", () => {
  const originalNow = Object.getOwnPropertyDescriptor(performance, "now");
  const originalSetInterval = globalThis.setInterval;
  const originalClearInterval = globalThis.clearInterval;
  let now = 100;
  let tick: (() => void) | undefined;
  let acknowledge: ((response: { TTL: number }) => void) | undefined;
  let writes = 0;
  Object.defineProperty(performance, "now", { configurable: true, value: () => now });
  globalThis.setInterval = ((callback: TimerHandler) => {
    tick = callback as () => void;
    return 1 as unknown as ReturnType<typeof setInterval>;
  }) as unknown as typeof setInterval;
  globalThis.clearInterval = (() => undefined) as typeof clearInterval;
  try {
    const provider = {
      epochForLease: () => "test-epoch",
      openKeepAlive: (_id: string, onResponse: (response: { TTL: number }) => void) => {
        acknowledge = onResponse;
        return { write: () => { writes++; }, cancel: () => undefined };
      },
    } as unknown as EtcdProvider;
    const lease = new EtcdLease(provider, "lease-1", 3000, 1000);
    lease.startKeepAlive();
    lease.activate(Buffer.from("opaque-key"), "1");

    now = 500;
    tick!();
    assert.equal(writes, 1);
    now = 1001; // The prior confirmed deadline elapsed before the reply is processed.
    acknowledge!({ TTL: 3 });

    assert.equal(lease.state, "lost");
    assert.equal(lease.isConservativelyValid(), false);
  } finally {
    if (originalNow) Object.defineProperty(performance, "now", originalNow);
    else Reflect.deleteProperty(performance, "now");
    globalThis.setInterval = originalSetInterval;
    globalThis.clearInterval = originalClearInterval;
  }
});
