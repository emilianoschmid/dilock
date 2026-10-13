# Dilock Java SDK

This Java 21 SDK implements the draft 1 Redis single-authority and etcd native-lock profiles. The Maven reactor builds `dilock-core`, `dilock-redis`, and `dilock-etcd` separately. The Redis adapter uses Jedis; the etcd adapter uses Jetcd's official generated protobuf and gRPC stubs. Installing one adapter does not install the other backend client. Neither backend stops a paused process after its lease expires. Redis does not provide strong fencing and cannot guarantee ownership across state loss, asynchronous replica promotion, eviction or manual deletion.

The Maven reactor builds `dilock-core`, `dilock-redis`, and `dilock-etcd` separately. Run `mvn test` from this directory to build and test the modules. Dependency and Maven plugin versions are pinned in the reactor POM. The JSONL conformance worker is local test tooling, enabled only by `-Pconformance-worker`; the Python launcher is `python sdk/java/worker_launcher.py <config.json>`.

## Redis

Namespace metadata must be provisioned explicitly before normal clients connect. `RedisProvider.provisionNamespace` is an administrative helper that borrows a caller-owned `RedisClient`, sets metadata only when absent and rejects a conflict without overwriting it.

```java
try (var provider = RedisProvider.connect(redisUri, "billing", "prod-redis-2026-10")) {
    var lock = provider.lock("invoice/42/reconcile");
    try (var lease = lock.acquire()) {
        lease.assertValid();
        // Protect the actual resource with transactions, idempotency, or resource-side fencing.
    }
}
```

`acquire()` uses the provider's standard 5 s wait and 30 s lease. Pass `AcquireOptions` or explicit durations when a call needs a different policy; the provider connection, namespace and approved epoch remain explicit.

The Redis provider owns a Jedis pooled client until `close`; lock handles are reusable, and leases represent one owner generation. Calls are synchronous and use bounded Redis connection/socket timeouts. Thread interruption is preserved. `tryAcquire` returns `Optional.empty()` only for ordinary contention. `acquire` uses one monotonic wait budget. Explicit renewal and owner-checked release use canonical draft 1 Lua; automatic renewal and strong fencing are unsupported.

Normal try-with-resources exit requires an acknowledged `RELEASED` outcome. If release is `ALREADY_NOT_OWNED`, `close()` throws a typed `LEASE_LOST` exception; an unknown acknowledgement remains a typed release error. Java suppresses a close failure under an exception from the protected body, so the body failure stays primary. If release matters independently of scope exit, call `release()` explicitly and inspect its outcome.

When interruption follows an acknowledged Redis SET, `RedisAcquisitionCancelledException`
preserves Java interruption and exposes `remoteOutcomeUnknown()`. A value of `true`
means owner-specific cleanup was not confirmed; no lease is returned, and the server
TTL remains the fallback bound. This exception remains catchable as `InterruptedException`.

## etcd native Lock profile

Use `EtcdProvider.connect` with one etcd endpoint, a pre-provisioned namespace, and a stable operator-approved coordinator epoch. The current adapter accepts plaintext `http://` gRPC endpoints. It validates metadata with a linearizable Range and never provisions it at runtime. The provider owns its gRPC channel and keepalive scheduler until `close`; lock handles are reusable, while each acquisition receives a distinct server lease. `tryAcquire` fails with `UNSUPPORTED_CAPABILITY`: the native API has no try-lock operation, and an RPC deadline does not prove contention. `acquire` throws `ACQUISITION_TIMEOUT` when its wait budget expires. Thread interruption cancels the native RPC, stops keepalive and attempts a bounded lease revoke.

Each successful native Lock retains its opaque key. Before returning, the client performs a linearizable Range on that exact key to verify its attached attempt lease and capture `create_revision`. `EtcdLease.fence()` returns an `EtcdFence` containing the configured epoch and revision; the resource must atomically compare/register fences itself. It does not guarantee exactly-once effects. Revision reset after restore requires an operational epoch change. Keepalive protects queued attempts automatically; held-lease renewal is opt-in. `release` performs native Unlock on the opaque key and then revokes only that attempt's lease. `close` cancels pending Lock RPCs and attempts bounded cleanup.

As with Redis, normal try-with-resources exit requires acknowledged `RELEASED`; `ALREADY_NOT_OWNED` throws `LEASE_LOST`, and unconfirmed cleanup propagates its typed release error. A body exception remains primary under Java's suppressed-exception rules. Use explicit `release()` to inspect the outcome directly.

The default acquisition stops keepalive once the lock is verified, so the lease expires after its granted TTL unless the caller releases it. Use the explicit overload only when a longer held operation needs renewal:

```java
try (var lease = provider.lock("invoice/42/reconcile").acquire(5_000, 3_000, true)) {
    lease.assertValid();
    // The etcd lease is kept alive while this process remains healthy.
    // Resource-side fencing is still required to reject stale work.
}
```

Renewal is tied to the same etcd lease and stops on release or loss of keepalive certainty. It does not fence or stop a paused process.

```java
try (var provider = EtcdProvider.connect("http://127.0.0.1:2379", "billing", "prod-etcd-2026-10")) {
    var lock = provider.lock("invoice/42/reconcile");
    try (var lease = lock.acquire()) {
        lease.assertValid();
        EtcdFence fence = lease.fence();
        // The protected resource must atomically reject older fences.
    }
}
```

This no-argument `acquire()` uses the provider's immutable default policy (5 s wait, 30 s lease, and no held-lease auto-renewal). The preceding overload example shows how to opt into renewal for a longer operation.

The identity-vector helper is `python sdk/java/tools/identity_vectors.py`. Redis and etcd interoperability is established only by running their worker against appropriately provisioned authorities with the shared real-process harness.

The JSONL conformance worker advertises `auto_renew` for both backends. It renews Redis leases with the SDK's owner-token-checked Lua operation and requests held etcd keepalive only when `auto_renew: true` is present. This local harness capability does not stop stale application work after lease loss.
