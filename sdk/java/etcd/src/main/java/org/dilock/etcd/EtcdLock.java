package org.dilock.etcd;

import org.dilock.redis.DilockException;
import org.dilock.redis.AcquireOptions;

import java.util.Optional;

/** Immutable reusable handle to one etcd-native lock name; safe for concurrent calls. */
public final class EtcdLock {
    private final EtcdProvider provider;
    private final String resource;
    EtcdLock(EtcdProvider provider, String resource) { this.provider = provider; this.resource = resource; }

    /** Acquires using the provider's immutable default policy (5 s wait, 30 s lease).
     * @return the acquired lease; use try-with-resources for bounded cleanup
     * @throws InterruptedException if waiting is interrupted
     */
    public EtcdLease acquire() throws InterruptedException { return acquire(provider.defaultAcquisition()); }

    /** Acquires using a complete per-attempt policy.
     * @param options full attempt policy
     * @return the acquired lease
     * @throws InterruptedException if waiting is interrupted
     */
    public EtcdLease acquire(AcquireOptions options) throws InterruptedException {
        EtcdLease result = provider.acquire(resource, options.waitMillis(), options.leaseMillis());
        if (result == null) throw new DilockException(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for etcd native Lock");
        return result;
    }

    /** Explicitly fails because etcd native Lock has no safe try-acquire operation.
     * @return never returns
     * @throws DilockException with {@link DilockException.Kind#UNSUPPORTED_CAPABILITY}
     */
    public Optional<EtcdLease> tryAcquire() { return tryAcquire(provider.defaultAcquisition().leaseMillis()); }

    /**
     * Reports unsupported capability because native etcd Lock has no try-lock RPC and a deadline
     * cannot distinguish contention from backend delay. No RPC is sent.
     *
     * @param leaseMillis requested lease duration, validated for API consistency
     * @return never returns
     * @throws IllegalArgumentException if the lease duration is outside 1..86,400,000 ms
     * @throws DilockException with {@link DilockException.Kind#UNSUPPORTED_CAPABILITY} otherwise
     */
    public Optional<EtcdLease> tryAcquire(long leaseMillis) {
        if (leaseMillis < 1 || leaseMillis > 86_400_000) throw new IllegalArgumentException("leaseMillis must be 1 through 86400000");
        throw new DilockException(DilockException.Kind.UNSUPPORTED_CAPABILITY, "etcd native Lock does not provide a safe try-acquire operation");
    }

    /**
     * Waits in the native etcd queue for at most the requested contention budget. A backend call
     * may add its bounded RPC time to the return time. Interruption attempts bounded lease cleanup;
     * {@link EtcdAcquisitionCancelledException#remoteOutcomeUnknown()} reports whether cleanup was
     * unconfirmed. A Lock or verification response received after the wait budget is cleaned up;
     * confirmed cleanup yields acquisition timeout, while unconfirmed cleanup yields an unknown
     * acquisition. Backend errors remain typed failures, never ordinary contention.
     *
     * @param waitMillis positive contention-wait budget, from 1 through 86,400,000 ms
     * @param leaseMillis requested attempt/held lease duration
     * @return acquired lease
     * @throws InterruptedException if interrupted; remote cleanup status is available when the
     *         exception is an {@link EtcdAcquisitionCancelledException}
     * @throws IllegalArgumentException if either duration is invalid or too short for RPC bounds
     * @throws DilockException for timeout, backend, protocol, or uncertain-acquisition failures
     */
    public EtcdLease acquire(long waitMillis, long leaseMillis) throws InterruptedException {
        return acquire(waitMillis, leaseMillis, false);
    }

    /**
     * Acquires with optional automatic renewal of this attempt's etcd lease. Renewal is serialized
     * with release and stops when ownership becomes uncertain or cleanup begins. It cannot stop a
     * paused process from using a protected resource after losing its lease; resource-side fencing
     * remains the caller's responsibility.
     *
     * @param waitMillis contention wait budget
     * @param leaseMillis requested etcd lease duration
     * @param autoRenew true to keep the same etcd lease alive while held
     * @return acquired lease
     * @throws InterruptedException if acquisition is interrupted
     * @throws DilockException if acquisition times out or its result is uncertain
     */
    public EtcdLease acquire(long waitMillis, long leaseMillis, boolean autoRenew) throws InterruptedException {
        EtcdLease result = provider.acquire(resource, waitMillis, leaseMillis, autoRenew);
        if (result == null) throw new DilockException(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for etcd native Lock");
        return result;
    }
}
