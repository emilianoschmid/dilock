// Local-only API sketch. No TTL, remote I/O, renewal or fencing.
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class DilockStyle {
    enum ReleaseOutcome { RELEASED, ALREADY_NOT_OWNED }

    /** Thread-safe local test fixture. */
    static final class MemoryStore {
        private final Map<String, Object> owners = new HashMap<>();
        synchronized boolean claim(String resource, Object owner) {
            return owners.putIfAbsent(resource, owner) == null;
        }
        synchronized ReleaseOutcome release(String resource, Object owner) {
            if (owners.get(resource) != owner) return ReleaseOutcome.ALREADY_NOT_OWNED;
            owners.remove(resource);
            return ReleaseOutcome.RELEASED;
        }
        synchronized void expireForTest(String resource) { owners.remove(resource); }
    }

    /** A single acquisition; close is local-only here, not blocking remote I/O. */
    static final class Lease implements AutoCloseable {
        private final MemoryStore store;
        private final String resource;
        private final Object owner;
        private boolean closed;
        Lease(MemoryStore store, String resource, Object owner) {
            this.store = store; this.resource = resource; this.owner = owner;
        }
        synchronized ReleaseOutcome release() {
            if (closed) return ReleaseOutcome.ALREADY_NOT_OWNED;
            closed = true;
            return store.release(resource, owner);
        }
        @Override public void close() { release(); }
    }

    /** An immutable, reusable reference to a resource. */
    record ResourceLock(MemoryStore store, String resource) {
        ResourceLock {
            Objects.requireNonNull(store);
            if (Objects.requireNonNull(resource).isEmpty()) throw new IllegalArgumentException("empty resource");
        }
        Optional<Lease> tryAcquire() throws InterruptedException {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("cancelled");
            var owner = new Object();
            return store.claim(resource, owner) ? Optional.of(new Lease(store, resource, owner)) : Optional.empty();
        }
    }
    record Provider(MemoryStore store) {
        Provider { Objects.requireNonNull(store); }
        ResourceLock lock(String name) { return new ResourceLock(store, name); }
    }

    static void check(boolean condition) {
        if (!condition) throw new AssertionError("style assertion failed");
    }
    public static void main(String[] args) throws Exception {
        var store = new MemoryStore();
        var lock = new Provider(store).lock("example/resource");
        var first = lock.tryAcquire().orElseThrow();
        check(lock.tryAcquire().isEmpty());
        store.expireForTest("example/resource");
        var second = lock.tryAcquire().orElseThrow();
        check(first.release() == ReleaseOutcome.ALREADY_NOT_OWNED);
        check(lock.tryAcquire().isEmpty());
        check(second.release() == ReleaseOutcome.RELEASED);
        check(second.release() == ReleaseOutcome.ALREADY_NOT_OWNED);
        try (var held = lock.tryAcquire().orElseThrow()) {
            check(held != null);
            throw new IllegalStateException("consumer failure");
        } catch (IllegalStateException expected) { }
        try (var held = lock.tryAcquire().orElseThrow()) { check(held != null); }
        Thread.currentThread().interrupt();
        try {
            lock.tryAcquire();
            throw new AssertionError("interruption ignored");
        } catch (InterruptedException expected) { }
        finally { Thread.interrupted(); }
        System.out.println("PASS java: busy, stale owner, idempotence, try-with-resources, interruption");
    }
}
