import type { RedisLease, RedisLock } from "@dilock/redis";
import type { EtcdLease, EtcdLock } from "@dilock/etcd";

// Keep the README quickstart forms checked by the workspace's strict tsc build.
export function redisDefaultQuickstart(lock: RedisLock): Promise<RedisLease> {
  return lock.acquire();
}

export function redisDefaultScopedQuickstart(lock: RedisLock): Promise<void> {
  return lock.withLease(async lease => { lease.assertValid(); });
}

export function redisConfiguredQuickstart(lock: RedisLock): Promise<RedisLease> {
  return lock.acquire({ waitMs: 5_000, leaseMs: 30_000, rpcTimeoutMs: 2_000 });
}

export function redisConfiguredScopedQuickstart(lock: RedisLock): Promise<void> {
  return lock.withLease(async lease => { lease.assertValid(); }, {
    waitMs: 8_000,
    leaseMs: 45_000,
    rpcTimeoutMs: 1_000,
  });
}

export function etcdDefaultQuickstart(lock: EtcdLock): Promise<EtcdLease> {
  return lock.acquire();
}

export function etcdDefaultScopedQuickstart(lock: EtcdLock): Promise<void> {
  return lock.withLease(async lease => { lease.assertValid(); });
}

export function etcdConfiguredScopedQuickstart(lock: EtcdLock): Promise<void> {
  return lock.withLease(async lease => { lease.assertValid(); }, {
    waitMs: 8_000,
    leaseMs: 45_000,
    rpcTimeoutMs: 1_000,
    autoRenew: true,
  });
}

export function etcdConfiguredQuickstart(lock: EtcdLock): Promise<EtcdLease> {
  return lock.acquire({
    waitMs: 5_000,
    leaseMs: 30_000,
    rpcTimeoutMs: 2_000,
    autoRenew: false,
  });
}
