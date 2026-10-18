package org.dilock.postgres;

import org.dilock.redis.DilockException;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;

/** One advisory lock held by one private physical PostgreSQL session. */
public final class PostgresLease implements AutoCloseable {
    public enum State { HELD, RELEASED, LOST }
    public enum ReleaseOutcome { RELEASED, ALREADY_NOT_OWNED }
    private final PostgresProvider provider;
    private final Connection connection;
    private final long key;
    private final long leaseMillis;
    private final AtomicReference<State> state = new AtomicReference<>(State.HELD);

    PostgresLease(PostgresProvider provider, Connection connection, long key, long leaseMillis) {
        this.provider = provider; this.connection = connection; this.key = key; this.leaseMillis = leaseMillis;
    }
    Connection connection() { return connection; }
    long key() { return key; }
    void lose() { state.set(State.LOST); }
    public State state() { return state.get(); }
    public long requestedLeaseMillis() { return leaseMillis; }
    public void assertValid() { if (state.get() != State.HELD) throw new DilockException(DilockException.Kind.LEASE_LOST, "PostgreSQL advisory lease is no longer locally valid"); }
    /** Liveness only; PostgreSQL advisory locks have no server TTL to extend. */
    public void renew() { assertValid(); provider.liveness(this); }
    public synchronized ReleaseOutcome release() {
        if (state.get() == State.RELEASED) return ReleaseOutcome.ALREADY_NOT_OWNED;
        if (state.get() == State.LOST) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "PostgreSQL advisory-lock ownership is no longer certain");
        try {
            boolean released = provider.release(this);
            state.set(released ? State.RELEASED : State.LOST);
            return released ? ReleaseOutcome.RELEASED : ReleaseOutcome.ALREADY_NOT_OWNED;
        } catch (RuntimeException e) { state.set(State.LOST); throw e; }
    }
    @Override public void close() { release(); }
}
