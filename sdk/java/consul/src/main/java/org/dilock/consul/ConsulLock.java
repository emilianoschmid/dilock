package org.dilock.consul;

import com.fasterxml.jackson.databind.JsonNode;
import org.dilock.redis.AcquireOptions;
import org.dilock.redis.DilockException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Reusable Consul KV lock handle; every acquisition owns a fresh session. */
public final class ConsulLock {
    private final ConsulProvider provider;
    private final String key;
    ConsulLock(ConsulProvider provider, String key) { this.provider = provider; this.key = key; }

    /** Attempts one acquisition without waiting. */
    public Optional<ConsulLease> tryAcquire() { return Optional.ofNullable(acquireInternal(provider.defaultAcquisition().leaseMillis(), 0)); }

    /** Waits up to the supplied contention budget. */
    public ConsulLease acquire(AcquireOptions options) {
        ConsulLease result = acquireInternal(options.leaseMillis(), options.waitMillis());
        if (result == null) throw new DilockException(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for Consul KV lock");
        return result;
    }

    public ConsulLease acquire() { return acquire(provider.defaultAcquisition()); }

    private ConsulLease acquireInternal(long leaseMillis, long waitMillis) {
        provider.ensureOpen();
        int ttlSeconds = ConsulProviderOptions.validateTtl(leaseMillis);
        long effectiveMillis = ttlSeconds * 1000L;
        ConsulProvider.CreatedSession created = provider.createSession(ttlSeconds);
        String session = created.id();
        ConsulLease lease = new ConsulLease(provider, key, session, effectiveMillis, created.sentNanos());
        provider.track(lease);
        lease.startRenewal();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        try {
            do {
                if (lease.state() == ConsulLease.State.LOST) throw new DilockException(DilockException.Kind.LEASE_LOST, "Consul session renewal failed while waiting");
                if (provider.acquireKey(key, session)) {
                    JsonNode owner = provider.owner(key);
                    if (owner == null || !owner.isArray() || owner.size() != 1
                            || !session.equals(owner.get(0).path("Session").asText())
                            || !owner.get(0).path("LockIndex").canConvertToLong()
                            || owner.get(0).path("LockIndex").longValue() <= 0)
                        throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Consul KV read did not verify session ownership and LockIndex");
                    lease.activate(owner.get(0).path("LockIndex").longValue());
                    return lease;
                }
                if (waitMillis == 0 || System.nanoTime() >= deadline) break;
                Thread.sleep(Math.min(100, Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))));
            } while (true);
            lease.abandon();
            return null;
        } catch (InterruptedException e) {
            lease.abandon();
            Thread.currentThread().interrupt();
            throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Consul acquisition was interrupted; cleanup was attempted", e);
        } catch (RuntimeException e) {
            lease.abandon();
            throw e;
        }
    }
}
