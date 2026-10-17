package org.dilock.consul;

import com.fasterxml.jackson.databind.JsonNode;
import org.dilock.redis.DilockException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** One Consul session lease and its locally verified LockIndex fence. */
public final class ConsulLease implements AutoCloseable {
    public enum State { WAITING, HELD, RELEASING, RELEASED, LOST }
    public enum ReleaseOutcome { RELEASED, ALREADY_NOT_OWNED }
    private final ConsulProvider provider;
    private final String key;
    private final String session;
    private final long effectiveTtlMillis;
    private final AtomicReference<State> state = new AtomicReference<>(State.WAITING);
    private volatile long deadlineNanos;
    private volatile long lockIndex;
    private volatile ScheduledFuture<?> renewal;

    ConsulLease(ConsulProvider provider, String key, String session, long effectiveTtlMillis, long sentNanos) {
        this.provider = provider; this.key = key; this.session = session; this.effectiveTtlMillis = effectiveTtlMillis;
        this.deadlineNanos = sentNanos + TimeUnit.MILLISECONDS.toNanos(effectiveTtlMillis - Math.max(100, effectiveTtlMillis / 100));
    }

    void startRenewal() {
        long interval = Math.max(1000, effectiveTtlMillis / 3);
        renewal = provider.scheduler().scheduleWithFixedDelay(this::renew, interval, interval, TimeUnit.MILLISECONDS);
    }

    private synchronized void renew() {
        State current = state.get();
        if (current != State.WAITING && current != State.HELD) return;
        if (System.nanoTime() >= deadlineNanos) { state.set(State.LOST); stopRenewal(); return; }
        try {
            long renewedAt = provider.renewSession(session);
            long next = renewedAt + TimeUnit.MILLISECONDS.toNanos(effectiveTtlMillis - Math.max(100, effectiveTtlMillis / 100));
            if (System.nanoTime() >= deadlineNanos || System.nanoTime() >= next) { state.set(State.LOST); stopRenewal(); return; }
            if (current == State.HELD) {
                JsonNode rows = provider.owner(key);
                if (rows == null || !rows.isArray() || rows.size() != 1
                        || !session.equals(rows.get(0).path("Session").asText())
                        || rows.get(0).path("LockIndex").longValue() != lockIndex) {
                    state.set(State.LOST); stopRenewal(); return;
                }
            }
            deadlineNanos = next;
        } catch (RuntimeException e) { state.set(State.LOST); stopRenewal(); }
    }

    synchronized void activate(long index) {
        if (state.get() != State.WAITING || System.nanoTime() >= deadlineNanos) throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Consul session expired before activation");
        lockIndex = index; state.set(State.HELD);
    }

    public State state() {
        if ((state.get() == State.WAITING || state.get() == State.HELD) && System.nanoTime() >= deadlineNanos) state.set(State.LOST);
        return state.get();
    }
    public long effectiveTtlMillis() { return effectiveTtlMillis; }
    public String epoch() { return provider.epoch(); }
    public long lockIndex() { assertValid(); return lockIndex; }
    public ConsulFence fence() { return new ConsulFence(epoch(), lockIndex()); }
    public void assertValid() { if (state() != State.HELD) throw new DilockException(DilockException.Kind.LEASE_LOST, "Consul lease is no longer locally valid"); }

    public synchronized ReleaseOutcome release() {
        State current = state();
        if (current == State.RELEASED) return ReleaseOutcome.ALREADY_NOT_OWNED;
        if (current == State.RELEASING) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "Consul release is already in progress");
        state.set(State.RELEASING); stopRenewal();
        try {
            ReleaseOutcome outcome = releaseWithSessionCleanup(() -> {
                boolean released = provider.releaseKey(key, session);
                if (released) return ReleaseOutcome.RELEASED;
                JsonNode rows = provider.owner(key);
                return classifyReleaseFalse(rows, key, session);
            }, () -> provider.destroy(session));
            state.set(State.RELEASED); provider.untrack(this);
            return outcome;
        } catch (RuntimeException e) {
            state.set(State.LOST); provider.untrack(this);
            if (e instanceof DilockException de && de.kind() == DilockException.Kind.RELEASE_UNKNOWN) throw de;
            throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "Consul release or session cleanup was not confirmed", e);
        }
    }

    static ReleaseOutcome releaseWithSessionCleanup(Supplier<ReleaseOutcome> operation, Runnable destroy) {
        ReleaseOutcome outcome = null;
        RuntimeException failure = null;
        try { outcome = operation.get(); }
        catch (RuntimeException e) { failure = e; }
        try { destroy.run(); }
        catch (RuntimeException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
        if (outcome == null) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN,
                "Consul release outcome was not classified");
        return outcome;
    }

    static ReleaseOutcome classifyReleaseFalse(JsonNode rows, String key, String session) {
        if (rows == null) return ReleaseOutcome.ALREADY_NOT_OWNED;
        if (!rows.isArray() || rows.size() != 1) throw uncertainRelease();
        JsonNode row = rows.get(0);
        JsonNode index = row.path("LockIndex");
        JsonNode owner = row.get("Session");
        if (!row.isObject() || !key.equals(row.path("Key").asText())
                || !index.isIntegralNumber() || !index.canConvertToLong() || index.longValue() < 0
                || owner == null || !(owner.isTextual() || owner.isNull()))
            throw uncertainRelease();
        if (session.equals(owner.asText())) throw uncertainRelease();
        return ReleaseOutcome.ALREADY_NOT_OWNED;
    }

    private static DilockException uncertainRelease() {
        return new DilockException(DilockException.Kind.RELEASE_UNKNOWN,
                "Consul release returned false but a valid linearizable read could not confirm non-ownership");
    }

    synchronized void abandon() {
        stopRenewal();
        try { provider.releaseKey(key, session); } catch (RuntimeException ignored) { }
        try { provider.destroy(session); } catch (RuntimeException ignored) { }
        state.set(State.LOST); provider.untrack(this);
    }
    private synchronized void stopRenewal() { if (renewal != null) { renewal.cancel(false); renewal = null; } }
    @Override public void close() { release(); }
}
