package org.dilock.redis;

import java.util.concurrent.atomic.AtomicReference;

/** One acquisition generation. It is not a thread-owned mutex or a fencing token. */
public final class RedisLease implements AutoCloseable {
    /** Process-local lifecycle view; {@link #state()} does not query Redis. */
    public enum State {
        /** Locally within the conservative validity deadline. */ HELD,
        /** Owner-checked release is in progress. */ RELEASING,
        /** Release was acknowledged. */ RELEASED,
        /** Release was sent but its acknowledgement was not confirmed. */ RELEASED_UNKNOWN,
        /** Local validity elapsed or Redis no longer recognizes this owner. */ LOST
    }
    /** Result of an acknowledged owner-checked release. */
    public enum ReleaseOutcome {
        /** This call removed the matching owner value. */ RELEASED,
        /** The matching owner value was already absent or belonged to a successor. */ ALREADY_NOT_OWNED
    }
    private final RedisProvider provider;
    private final String key;
    private final String owner;
    private final String id = java.util.UUID.randomUUID().toString();
    private final long originalLeaseMillis;
    private final AtomicReference<State> state = new AtomicReference<>(State.HELD);
    private volatile long deadlineNanos;
    private volatile ReleaseOutcome terminalReleaseOutcome;

    RedisLease(RedisProvider provider, String key, String owner, long originalLeaseMillis, long deadlineNanos) {
        this.provider = provider; this.key = key; this.owner = owner;
        this.originalLeaseMillis = originalLeaseMillis; this.deadlineNanos = deadlineNanos;
    }
    /** Returns this process-local identifier; it is not the Redis owner value.
     * @return process-local identifier
     */
    public String id() { return id; }
    /** Reports local state after applying the monotonic deadline; this is not a Redis query.
     * @return local lifecycle state
     */
    public State state() { expireLocally(); return state.get(); }
    String key() { return key; }
    String owner() { return owner; }

    /**
     * Throws when the conservative monotonic validity budget has elapsed or the lease is terminal.
     * This local check cannot stop a paused process or prevent stale external effects.
     *
     * @throws DilockException with {@link DilockException.Kind#LEASE_LOST} if locally invalid
     */
    public void assertValid() {
        expireLocally();
        if (state.get() != State.HELD) throw new DilockException(DilockException.Kind.LEASE_LOST, "lease is no longer locally valid");
    }
    private void expireLocally() {
        if (state.get() == State.HELD && System.nanoTime() >= deadlineNanos) state.compareAndSet(State.HELD, State.LOST);
    }

    /**
     * Extends this owner's TTL atomically. Calls on this lease are serialized. A false server
     * comparison or an acknowledgement arriving beyond the conservative deadline permanently
     * loses local validity; transport failures are reported as backend failures.
     *
     * @param leaseMillis requested new TTL in milliseconds
     * @throws IllegalArgumentException if duration is outside the supported range or too short
     * @throws DilockException if the lease is lost or renewal outcome cannot be confirmed
     */
    public synchronized void renew(long leaseMillis) {
        assertValid();
        if (leaseMillis <= 0 || leaseMillis > 86_400_000) throw new IllegalArgumentException("leaseMillis must be 1 through 86400000");
        try {
            if (!provider.renew(this, leaseMillis)) {
                state.set(State.LOST);
                throw new DilockException(DilockException.Kind.LEASE_LOST, "Redis no longer recognizes this owner");
            }
        } catch (DilockException error) {
            // A backend failure leaves ownership uncertain. Do not let a later call treat this
            // generation as held and retry renewal after Redis may have accepted the first EVAL.
            if (error.kind() == DilockException.Kind.BACKEND_UNAVAILABLE) state.set(State.LOST);
            throw error;
        }
    }
    synchronized boolean confirmRenewal(long nextDeadlineNanos) {
        expireLocally();
        if (state.get() != State.HELD || System.nanoTime() >= nextDeadlineNanos) {
            state.set(State.LOST); return false;
        }
        deadlineNanos = nextDeadlineNanos;
        return true;
    }

    /**
     * Performs bounded, awaited, owner-checked release. Calls on this lease are serialized;
     * repeated terminal cleanup returns {@link ReleaseOutcome#ALREADY_NOT_OWNED}. If the release
     * acknowledgement is unknown, the lease enters {@link State#RELEASED_UNKNOWN} and a later call
     * throws rather than replaying the release. A locally lost lease still performs the bounded
     * owner check; local expiry alone does not prove that Redis removed its owner value.
     *
     * @return acknowledged release outcome
     * @throws DilockException with {@link DilockException.Kind#RELEASE_UNKNOWN} if acknowledgement
     *         cannot be confirmed
     */
    public synchronized ReleaseOutcome release() {
        expireLocally();
        State current = state.get();
        if (current == State.RELEASED) return ReleaseOutcome.ALREADY_NOT_OWNED;
        if (current == State.RELEASED_UNKNOWN) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "a previous Redis release acknowledgement was unknown");
        if (!state.compareAndSet(current, State.RELEASING)) return ReleaseOutcome.ALREADY_NOT_OWNED;
        try {
            ReleaseOutcome result = provider.release(this);
            terminalReleaseOutcome = result;
            state.set(State.RELEASED);
            return result;
        } catch (RuntimeException error) {
            state.set(State.RELEASED_UNKNOWN);
            throw error;
        }
    }
    /**
     * Performs bounded cleanup for try-with-resources. A normal scope exit requires an
     * acknowledged owner release; an earlier successful explicit release is accepted.
     * If the protected body throws, Java suppresses a close failure under that primary error.
     */
    @Override public void close() {
        if (state.get() == State.RELEASED && terminalReleaseOutcome == ReleaseOutcome.RELEASED) return;
        ReleaseOutcome result;
        try { result = release(); }
        catch (RuntimeException error) {
            provider.observeCleanup("unknown", "scope-close");
            throw error;
        }
        if (result != ReleaseOutcome.RELEASED) {
            provider.observeCleanup("already_not_owned", "scope-close");
            throw new DilockException(DilockException.Kind.LEASE_LOST,
                    "try-with-resources exited without an acknowledged owner release");
        }
    }
}
