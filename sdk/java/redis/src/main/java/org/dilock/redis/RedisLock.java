package org.dilock.redis;

import java.util.Optional;

/**
 * Reusable, immutable handle to one named resource. Concurrent calls are permitted; this is a
 * distributed lock handle, never a process-local mutex.
 */
public final class RedisLock {
    private final RedisProvider provider;
    private final String resource;

    RedisLock(RedisProvider provider, String resource) { this.provider = provider; this.resource = resource; }

    /** Acquires using the provider's immutable default policy (5 s wait, 30 s lease).
     * @return the acquired lease; use try-with-resources for bounded cleanup
     * @throws InterruptedException if waiting is interrupted
     */
    public RedisLease acquire() throws InterruptedException {
        AcquireOptions options = provider.defaultAcquisition();
        return acquire(options);
    }

    /** Acquires using a complete per-attempt policy.
     * @param options full attempt policy
     * @return the acquired lease
     * @throws InterruptedException if waiting is interrupted
     */
    public RedisLease acquire(AcquireOptions options) throws InterruptedException {
        return acquire(options.waitMillis(), options.leaseMillis());
    }

    /** Makes one attempt using the provider's default lease duration.
     * @return a lease or empty for ordinary contention
     * @throws InterruptedException if interrupted
     */
    public Optional<RedisLease> tryAcquire() throws InterruptedException {
        return tryAcquire(provider.defaultAcquisition().leaseMillis());
    }

    /**
     * Makes one Redis attempt bounded by the provider's RPC timeout. {@link Optional#empty()} means
     * ordinary contention only. The lease must be long enough to exceed the RPC timeout plus a
     * safety margin. Interruption is preserved; after command send, uncertain remote outcomes are
     * reported as {@link DilockException} and owner-specific cleanup is attempted.
     * After an acknowledged SET, interruption throws {@link RedisAcquisitionCancelledException};
     * inspect its {@code remoteOutcomeUnknown()} before treating cleanup as confirmed.
     *
     * @param leaseMillis requested server lease duration, from 1 through 86,400,000 ms
     * @return a lease if acquired, or empty for ordinary contention
     * @throws InterruptedException if the calling thread is interrupted
     * @throws IllegalArgumentException if duration is invalid or too short for the RPC bound
     * @throws DilockException for backend, protocol, or uncertain-acquisition failures
     */
    public Optional<RedisLease> tryAcquire(long leaseMillis) throws InterruptedException {
        if (leaseMillis <= 0 || leaseMillis > 86_400_000) throw new IllegalArgumentException("leaseMillis must be 1 through 86400000");
        return provider.tryAcquire(resource, leaseMillis);
    }

    /**
     * Waits with bounded jitter until contention clears or the monotonic wait budget expires.
     * {@code waitMillis} bounds waiting between attempts; one final backend RPC can extend return
     * time by the configured RPC timeout. Backend errors and interrupted waits are not contention.
     *
     * @param waitMillis positive maximum contention-wait budget, from 1 through 86,400,000 ms
     * @param leaseMillis requested server lease duration
     * @return the acquired lease
     * @throws InterruptedException if the calling thread is interrupted
     * @throws IllegalArgumentException if either duration is invalid
     * @throws DilockException with {@link DilockException.Kind#ACQUISITION_TIMEOUT} on contention
     *         timeout, or another kind for backend/protocol/uncertain outcomes
     */
    public RedisLease acquire(long waitMillis, long leaseMillis) throws InterruptedException {
        if (waitMillis < 1 || waitMillis > 86_400_000) throw new IllegalArgumentException("waitMillis must be 1 through 86400000");
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(waitMillis);
        for (;;) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("acquisition interrupted while waiting");
            Optional<RedisLease> lease = tryAcquire(leaseMillis);
            if (lease.isPresent()) return lease.get();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new DilockException(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for Redis lease contention to clear");
            long pause = Math.min(remaining, java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(25 + java.util.concurrent.ThreadLocalRandom.current().nextInt(51)));
            java.util.concurrent.TimeUnit.NANOSECONDS.sleep(pause);
        }
    }

    /**
     * Rejects a fencing requirement because the Redis single-authority profile has no strong
     * fencing counter. A lease owner value is not a fencing token.
     *
     * @throws DilockException always, with {@link DilockException.Kind#UNSUPPORTED_CAPABILITY}
     */
    public void requireFencing() {
        throw new DilockException(DilockException.Kind.UNSUPPORTED_CAPABILITY, "Redis single-authority profile does not provide strong fencing");
    }
}
