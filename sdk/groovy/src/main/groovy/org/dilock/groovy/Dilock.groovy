package org.dilock.groovy

import org.dilock.etcd.EtcdLease
import org.dilock.etcd.EtcdProvider
import org.dilock.redis.AcquireOptions
import org.dilock.redis.RedisLease
import org.dilock.redis.RedisProvider

/**
 * Closure-based lifetime helpers over the Java SDK. This facade owns no backend
 * protocol state; the supplied provider is closed only by withRedis/withEtcd,
 * while withRedisLock/withEtcdLock close only the acquired lease.
 */
final class Dilock {
    private Dilock() { }

    static RedisProvider redis(String endpoint, String namespace, String epoch) {
        RedisProvider.connect(endpoint, namespace, epoch)
    }

    static EtcdProvider etcd(String endpoint, String namespace, String epoch) {
        EtcdProvider.connect(endpoint, namespace, epoch)
    }

    static <T extends AutoCloseable, R> R withCloseable(T resource, Closure<R> body) {
        Objects.requireNonNull(resource, 'resource')
        Throwable bodyFailure = null
        try {
            // Validate inside the managed region: an invalid callback must not
            // leak a provider or lease already transferred to this scope.
            Objects.requireNonNull(body, 'body')
            body.call(resource)
        } catch (Throwable error) {
            bodyFailure = error
            throw error
        } finally {
            try {
                resource.close()
            } catch (Throwable closeFailure) {
                if (bodyFailure != null) {
                    // Throwable.addSuppressed rejects self-suppression. Preserve
                    // the body failure even if a resource rethrows that exact
                    // Throwable instance from close().
                    if (!bodyFailure.is(closeFailure)) {
                        bodyFailure.addSuppressed(closeFailure)
                    }
                } else {
                    throw closeFailure
                }
            }
        }
    }

    static <R> R withRedis(String endpoint, String namespace, String epoch,
                           Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        withCloseable(redis(endpoint, namespace, epoch), body)
    }

    static <R> R withEtcd(String endpoint, String namespace, String epoch,
                          Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        withCloseable(etcd(endpoint, namespace, epoch), body)
    }

    static <R> R withLease(RedisLease lease, Closure<R> body) {
        withLeaseScope(lease, body)
    }

    static <R> R withLease(EtcdLease lease, Closure<R> body) {
        withLeaseScope(lease, body)
    }

    static <T, R> R withLeaseScope(T lease, Closure<R> body) {
        Objects.requireNonNull(lease, 'lease')
        Throwable bodyFailure = null
        R result = null
        try {
            Objects.requireNonNull(body, 'body')
            result = body.call(lease)
        } catch (Throwable error) {
            bodyFailure = error
        }

        Throwable releaseFailure = null
        try {
            Object outcome = lease.release()
            if (outcome != RedisLease.ReleaseOutcome.RELEASED &&
                    outcome != EtcdLease.ReleaseOutcome.RELEASED) {
                releaseFailure = new IllegalStateException(
                    "Lease scope did not receive a RELEASED acknowledgement: ${outcome}")
            }
        } catch (Throwable error) {
            releaseFailure = error
        }

        if (bodyFailure != null) {
            if (releaseFailure != null && !bodyFailure.is(releaseFailure)) {
                bodyFailure.addSuppressed(releaseFailure)
            }
            throw bodyFailure
        }
        if (releaseFailure != null) throw releaseFailure
        result
    }

    /** Uses the Java defaults: a 5-second wait and 30-second lease. */
    static <R> R withRedisLock(RedisProvider provider, String resource,
                               Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        withLease(provider.lock(resource).acquire(), body)
    }

    /** Uses the Java defaults: a 5-second wait and 30-second lease. */
    static <R> R withEtcdLock(EtcdProvider provider, String resource,
                              Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        withLease(provider.lock(resource).acquire(), body)
    }

    /** Uses the default wait and lease durations with optional held-lease renewal. */
    static <R> R withEtcdLock(EtcdProvider provider, String resource,
                              boolean autoRenew, Closure<R> body) {
        withEtcdLock(provider, resource, AcquireOptions.DEFAULT, autoRenew, body)
    }

    static <R> R withRedisLock(RedisProvider provider, String resource,
                               AcquireOptions options, Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        withLease(provider.lock(resource).acquire(options), body)
    }

    static <R> R withEtcdLock(EtcdProvider provider, String resource,
                              AcquireOptions options, Closure<R> body) {
        withEtcdLock(provider, resource, options, false, body)
    }

    /** Configures the etcd acquisition and whether its server lease renews while held. */
    static <R> R withEtcdLock(EtcdProvider provider, String resource,
                              AcquireOptions options, boolean autoRenew, Closure<R> body) {
        Objects.requireNonNull(body, 'body')
        Objects.requireNonNull(options, 'options')
        withLease(provider.lock(resource).acquire(
                options.waitMillis(), options.leaseMillis(), autoRenew), body)
    }
}
