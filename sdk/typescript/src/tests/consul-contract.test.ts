import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { ConsulProvider } from "@dilock/consul";
import { AcquisitionUnknownError, InvalidArgumentError, ProtocolMismatchError, ReleaseUnknownError } from "@dilock/core";

interface LockRow { session: string | null; lockIndex: number }

function createConsul(epoch = "epoch-1", includeUnknownMetadata = false) {
  const sessions = new Set<string>();
  const locks = new Map<string, LockRow>();
  const tokenHeaders: string[] = [];
  const requestUrls: string[] = [];
  let sessionCreates = 0;
  let renewals = 0;
  let metadataReads = 0;
  let loseAcquireAck = false;
  let loseReleaseAck = false;

  const fetch: typeof globalThis.fetch = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const method = init?.method ?? "GET";
    const headers = new Headers(init?.headers);
    tokenHeaders.push(headers.get("x-consul-token") ?? "");
    requestUrls.push(url.href);
    const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {
      status, headers: { "content-type": "application/json" },
    });

    if (method === "GET" && url.pathname === "/v1/kv/dilock/namespaces/tests/meta") {
      metadataReads++;
      const metadata = {
        protocol_major: 1,
        identity_scheme: "sha256-framed-utf8-v1",
        profile: "consul-kv-session-v1",
        coordination_epoch: epoch,
        fixed_lease_ms: null,
      };
      if (includeUnknownMetadata) Object.assign(metadata, { future_field: "unsupported" });
      return json([{
        Key: "dilock/namespaces/tests/meta",
        Value: Buffer.from(JSON.stringify(metadata)).toString("base64"),
      }]);
    }

    if (method === "PUT" && url.pathname === "/v1/session/create") {
      const session = `session-${++sessionCreates}`;
      const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
      assert.equal(body.Behavior, "release");
      assert.equal(body.LockDelay, "0s");
      assert.equal(body.TTL, "10s");
      assert.deepEqual(body.Checks, []);
      sessions.add(session);
      return json({ ID: session });
    }

    if (method === "PUT" && url.pathname.startsWith("/v1/session/renew/")) {
      const session = decodeURIComponent(url.pathname.slice("/v1/session/renew/".length));
      if (!sessions.has(session)) return json([]);
      renewals++;
      return json([{ ID: session }]);
    }

    if (method === "PUT" && url.pathname.startsWith("/v1/session/destroy/")) {
      const session = decodeURIComponent(url.pathname.slice("/v1/session/destroy/".length));
      sessions.delete(session);
      for (const lock of locks.values()) if (lock.session === session) lock.session = null;
      return json(true);
    }

    if (url.pathname.startsWith("/v1/kv/dilock/locks/")) {
      const key = url.pathname.slice("/v1/kv/".length);
      if (method === "GET") {
        const lock = locks.get(key);
        return lock
          ? json([{ Key: key, Session: lock.session, LockIndex: lock.lockIndex }])
          : json([], 404);
      }
      const acquire = url.searchParams.get("acquire");
      if (method === "PUT" && acquire !== null) {
        const lock = locks.get(key) ?? { session: null, lockIndex: 0 };
        locks.set(key, lock);
        if (!sessions.has(acquire) || (lock.session !== null && lock.session !== acquire)) return json(false);
        if (lock.session === null) {
          lock.session = acquire;
          lock.lockIndex++;
        }
        if (loseAcquireAck) {
          loseAcquireAck = false;
          throw new TypeError("injected lost acquire acknowledgement");
        }
        return json(true);
      }
      const release = url.searchParams.get("release");
      if (method === "PUT" && release !== null) {
        if (loseReleaseAck) {
          loseReleaseAck = false;
          throw new TypeError("injected lost release acknowledgement");
        }
        const lock = locks.get(key);
        const released = lock?.session === release;
        if (released) lock.session = null;
        return json(released);
      }
    }
    return json({ error: "not found" }, 404);
  };

  return {
    sessions, locks, tokenHeaders, requestUrls, fetch,
    get sessionCreates() { return sessionCreates; },
    get renewals() { return renewals; },
    get metadataReads() { return metadataReads; },
    failNextAcquireAcknowledgement() { loseAcquireAck = true; },
    failNextReleaseAcknowledgement() { loseReleaseAck = true; },
    expireSessions() {
      sessions.clear();
      for (const lock of locks.values()) lock.session = null;
    },
  };
}

function useConsulPeer(context: TestContext, peer: ReturnType<typeof createConsul>): void {
  context.mock.method(globalThis, "fetch", peer.fetch);
}

async function waitForRenewal(peer: ReturnType<typeof createConsul>): Promise<void> {
  const deadline = Date.now() + 6000;
  while (peer.renewals === 0 && Date.now() < deadline)
    await new Promise(resolve => setTimeout(resolve, 25));
  assert.ok(peer.renewals > 0, "the attempt session should renew before its TTL expires");
}

test("Consul sessions contend, renew, release, and retain a monotonic LockIndex", async context => {
  const peer = createConsul();
  useConsulPeer(context, peer);
  let provider: ConsulProvider | undefined;
  try {
    provider = await ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "epoch-1", token: "contract-token" });
    const lock = provider.lock("shared");
    const first = await lock.tryAcquire({ leaseMs: 10_000 });
    assert.ok(first);
    assert.deepEqual(first.fence, { epoch: "epoch-1", lockIndex: 1 });
    assert.equal(await lock.tryAcquire({ leaseMs: 10_000 }), undefined);
    await waitForRenewal(peer);
    first.assertValid();
    assert.deepEqual(first.fence, { epoch: "epoch-1", lockIndex: 1 });
    assert.equal(await first.release(), "released");

    const second = await lock.tryAcquire({ leaseMs: 10_000 });
    assert.ok(second);
    assert.equal(second.fence.lockIndex, 2);
    assert.equal(await second.release(), "released");
    assert.equal([...peer.locks.values()][0]?.lockIndex, 2);
    assert.ok(peer.tokenHeaders.every(value => value === "contract-token"));
    assert.ok(peer.requestUrls.every(value => !value.includes("contract-token")));
  } finally {
    await provider?.close();
  }
});

test("a stale Consul session cannot release a successor owner", async context => {
  const peer = createConsul();
  useConsulPeer(context, peer);
  let provider: ConsulProvider | undefined;
  try {
    provider = await ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "epoch-1" });
    const lock = provider.lock("successor");
    const stale = await lock.tryAcquire({ leaseMs: 10_000 });
    assert.ok(stale);
    peer.expireSessions();
    const successor = await lock.tryAcquire({ leaseMs: 10_000 });
    assert.ok(successor);
    assert.equal(successor.fence.lockIndex, 2);
    assert.equal(await stale.release(), "already-not-owned");
    assert.notEqual([...peer.locks.values()][0]?.session, null);
    assert.equal(await successor.release(), "released");
  } finally {
    await provider?.close();
  }
});

test("an unacknowledged Consul acquire stays unknown and attempts owner cleanup", async context => {
  const peer = createConsul();
  useConsulPeer(context, peer);
  let provider: ConsulProvider | undefined;
  try {
    provider = await ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "epoch-1" });
    peer.failNextAcquireAcknowledgement();
    await assert.rejects(
      provider.lock("lost-ack").tryAcquire({ leaseMs: 10_000 }),
      AcquisitionUnknownError,
    );
    assert.equal(peer.sessions.size, 0);
    assert.equal([...peer.locks.values()][0]?.session, null);
  } finally {
    await provider?.close();
  }
});

test("an unacknowledged Consul release still destroys its attempt session", async context => {
  const peer = createConsul();
  useConsulPeer(context, peer);
  let provider: ConsulProvider | undefined;
  try {
    provider = await ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "epoch-1" });
    const lease = await provider.lock("lost-release-ack").tryAcquire({ leaseMs: 10_000 });
    assert.ok(lease);
    peer.failNextReleaseAcknowledgement();
    await assert.rejects(lease.release(), ReleaseUnknownError);
    assert.equal(peer.sessions.size, 0);
    assert.equal([...peer.locks.values()][0]?.session, null);
  } finally {
    await provider?.close();
  }
});

test("Consul metadata mismatch and malformed endpoints fail before session creation", async context => {
  const peer = createConsul("wrong-epoch");
  useConsulPeer(context, peer);
  await assert.rejects(
    ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "expected-epoch" }),
    ProtocolMismatchError,
  );
  assert.equal(peer.sessionCreates, 0);
  await assert.rejects(
    ConsulProvider.connect({ endpoint: "http://consul.test:8500/prefix", namespace: "tests", epoch: "wrong-epoch" }),
    InvalidArgumentError,
  );
  assert.equal(peer.metadataReads, 1);
});

test("Consul metadata rejects fields outside the exact profile shape", async context => {
  const peer = createConsul("epoch-1", true);
  useConsulPeer(context, peer);
  await assert.rejects(
    ConsulProvider.connect({ endpoint: "http://consul.test:8500", namespace: "tests", epoch: "epoch-1" }),
    ProtocolMismatchError,
  );
  assert.equal(peer.sessionCreates, 0);
});
