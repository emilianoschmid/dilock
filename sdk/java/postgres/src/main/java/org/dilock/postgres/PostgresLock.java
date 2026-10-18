package org.dilock.postgres;

import org.dilock.redis.AcquireOptions;
import org.dilock.redis.DilockException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Reusable PostgreSQL advisory-lock handle. */
public final class PostgresLock {
    private final PostgresProvider provider;
    private final String resource;
    PostgresLock(PostgresProvider provider, String resource) { this.provider = provider; this.resource = resource; }

    public Optional<PostgresLease> tryAcquire() { return provider.tryAcquire(resource, provider.defaultAcquisition().leaseMillis()); }

    public PostgresLease acquire(AcquireOptions options) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.waitMillis());
        do {
            Optional<PostgresLease> attempt = provider.tryAcquire(resource, options.leaseMillis());
            if (attempt.isPresent()) return attempt.orElseThrow();
            if (System.nanoTime() >= deadline) break;
            try { Thread.sleep(Math.min(50, Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "PostgreSQL acquisition was interrupted", e); }
        } while (true);
        throw new DilockException(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for PostgreSQL advisory lock");
    }

    public PostgresLease acquire() { return acquire(provider.defaultAcquisition()); }
}
