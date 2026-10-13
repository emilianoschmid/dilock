package org.dilock.etcd;

import com.google.protobuf.ByteString;
import io.etcd.jetcd.api.LeaseKeepAliveRequest;
import io.grpc.stub.StreamObserver;
import org.dilock.redis.DilockException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

/** One independent attempt lease and native lock generation. */
public final class EtcdLease implements AutoCloseable {
    /** Process-local lease lifecycle; inspecting it does not query etcd. */
    public enum State {
        /** Attempt lease is queued for the native lock. */ WAITING,
        /** Native lock acquisition and exact-key verification completed. */ HELD,
        /** Owner-key cleanup is in progress. */ RELEASING,
        /** Unlock and lease revocation were acknowledged. */ RELEASED,
        /** Release was attempted but acknowledgement was incomplete. */ RELEASED_UNKNOWN,
        /** Local deadline elapsed or keepalive/ownership verification failed. */ LOST
    }
    /** Result of release after native unlock and attempt-lease cleanup. */
    public enum ReleaseOutcome {
        /** The exact key and attempt lease were verified before native Unlock was acknowledged. */ RELEASED,
        /** The exact key was confirmed absent; an unconfirmed attempt lease expires by its server TTL. */ ALREADY_NOT_OWNED
    }
    private final EtcdProvider provider;
    private final long id;
    private final long effectiveLeaseMillis;
    private final AtomicReference<State> state = new AtomicReference<>(State.WAITING);
    private volatile long deadlineNanos;
    private volatile ByteString key;
    private volatile long createRevision;
    private volatile StreamObserver<LeaseKeepAliveRequest> requests;
    private volatile ScheduledFuture<?> task;
    private volatile boolean keepAliveStopping;
    private volatile ReleaseOutcome terminalReleaseOutcome;

    EtcdLease(EtcdProvider provider, long id, long effectiveLeaseMillis, long deadlineNanos) {
        this.provider = provider; this.id = id; this.effectiveLeaseMillis = effectiveLeaseMillis; this.deadlineNanos = deadlineNanos;
    }
    /** Returns the etcd lease ID for diagnostics; it is not an ownership or fencing token.
     * @return etcd lease ID
     */
    public long id() { return id; }
    /** Returns the granted attempt lease duration.
     * @return lease duration in milliseconds
     */
    public long effectiveLeaseMillis() { return effectiveLeaseMillis; }
    /** Returns the configured coordinator incarnation for resource-side comparisons.
     * @return configured epoch
     */
    public String epoch() { return provider.epoch(); }
    /**
     * Returns etcd's create_revision captured by the verifying linearizable Range.
     * @return positive etcd create revision
     * @throws DilockException with {@link DilockException.Kind#LEASE_LOST} if locally invalid
     */
    public long createRevision() { assertValid(); return createRevision; }
    /**
     * Creates the epoch-scoped fencing value from the verified etcd create revision.
     * The resource being protected must atomically compare epoch and revision with its stored value.
     * @return epoch-scoped fence
     * @throws DilockException with {@link DilockException.Kind#LEASE_LOST} if locally invalid
     */
    public EtcdFence fence() { assertValid(); return new EtcdFence(provider.epoch(), createRevision); }
    /** Reports local lifecycle state after applying the monotonic deadline.
     * @return local lifecycle state
     */
    public State state() { expire(); return state.get(); }
    /**
     * Checks only local lease validity; this cannot stop a paused process or guarantee external effects.
     * @throws DilockException with {@link DilockException.Kind#LEASE_LOST} if no longer locally valid
     */
    public void assertValid() { expire(); if (state.get() != State.HELD) throw new DilockException(DilockException.Kind.LEASE_LOST, "etcd lease is no longer locally valid"); }
    boolean isValid() { expire(); return (state.get() == State.HELD || state.get() == State.WAITING) && System.nanoTime() < deadlineNanos; }
    private synchronized void expire() { if ((state.get() == State.WAITING || state.get() == State.HELD) && System.nanoTime() >= deadlineNanos) state.set(State.LOST); }
    void startKeepAlive() { provider.keepAlive(this, System.nanoTime()); }
    void openKeepAlive(StreamObserver<LeaseKeepAliveRequest> requests, ConcurrentLinkedQueue<Long> sentTimes) { this.requests = requests; sendKeepAlive(sentTimes, id -> LeaseKeepAliveRequest.newBuilder().setID(id).build()); }
    void sendKeepAlive(ConcurrentLinkedQueue<Long> sentTimes, LongFunction<LeaseKeepAliveRequest> request) {
        if (keepAliveStopping) return;
        StreamObserver<LeaseKeepAliveRequest> stream = requests;
        if (keepAliveStopping) return;
        if (stream == null || !isValid()) { markLost(); return; }
        try { sentTimes.add(System.nanoTime()); stream.onNext(request.apply(id)); }
        catch (RuntimeException error) { markLost(); }
    }
    synchronized void confirmDeadline(long next) {
        if (!keepAliveStopping && (state.get() == State.WAITING || state.get() == State.HELD)) {
            long now = System.nanoTime();
            // A delayed keepalive ACK cannot restore permission after the last
            // locally confirmed validity window has already elapsed.
            if (now >= deadlineNanos || now >= next) {
                state.set(State.LOST);
                stopKeepAlive();
            } else {
                deadlineNanos = next;
            }
        }
    }
    synchronized void markLostFromKeepAlive() { if (!keepAliveStopping) markLost(); }
    synchronized void activate(ByteString key, long revision) {
        expire();
        if (state.get() != State.WAITING) throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "etcd lease expired before lock activation");
        this.key = key;
        this.createRevision = revision;
        // Keep activation and keepalive loss under the same monitor so terminal loss cannot be overwritten.
        state.set(State.HELD);
    }
    boolean isKeepAliveStopping() { return keepAliveStopping; }
    void setTask(ScheduledFuture<?> task) { this.task = task; }
    synchronized void stopKeepAlive() {
        keepAliveStopping = true;
        ScheduledFuture<?> running = task; if (running != null) running.cancel(false); task = null;
        StreamObserver<LeaseKeepAliveRequest> stream = requests; requests = null;
        if (stream != null) try { stream.onCompleted(); } catch (RuntimeException ignored) { }
    }
    synchronized void markLost() { state.set(State.LOST); stopKeepAlive(); }
    boolean revokeBounded() { stopKeepAlive(); boolean result = provider.revoke(id); provider.remove(id); markLost(); return result; }
    boolean unlockAndRevoke(ByteString opaqueKey) { return provider.unlockAndRevoke(this, opaqueKey); }
    /**
     * Performs a bounded exact-key/lease ownership check, then native Unlock when that ownership
     * is confirmed, followed by lease Revoke. Calls on this object are serialized. An
     * unconfirmed release enters {@link State#RELEASED_UNKNOWN}; a later call reports that unknown
     * result rather than replaying the operation. A locally lost lease still attempts Unlock and
     * Revoke because local expiry alone does not prove remote cleanup. Lease expiry bounds cleanup
     * if revoke is uncertain.
     *
     * @return acknowledged cleanup outcome
     * @throws DilockException with {@link DilockException.Kind#RELEASE_UNKNOWN} if cleanup is not
     *         fully confirmed
     */
    public synchronized ReleaseOutcome release() {
        expire();
        State current = state.get();
        if (current == State.RELEASED) return ReleaseOutcome.ALREADY_NOT_OWNED;
        if (current == State.RELEASED_UNKNOWN) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "previous etcd release acknowledgement was unknown");
        if (!state.compareAndSet(current, State.RELEASING)) return ReleaseOutcome.ALREADY_NOT_OWNED;
        try {
            ReleaseOutcome result = provider.release(this, key);
            terminalReleaseOutcome = result;
            state.set(State.RELEASED);
            return result;
        } catch (RuntimeException error) { state.set(State.RELEASED_UNKNOWN); throw error; }
    }
    static void requireReleasedScopeOutcome(ReleaseOutcome result) {
        if (result != ReleaseOutcome.RELEASED) {
            throw new DilockException(DilockException.Kind.LEASE_LOST,
                    "try-with-resources exited without acknowledged Unlock/revoke");
        }
    }
    /**
     * Performs bounded cleanup for try-with-resources. A normal scope exit requires acknowledged
     * Unlock/revoke; an earlier successful explicit release is accepted. Java suppresses a close
     * failure under the protected body's exception when both fail.
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
            requireReleasedScopeOutcome(result);
        }
    }
}
