package org.dilock.redis;

/** Immutable policy for one acquisition. Durations use native Java precision.
 * @param waitMillis positive maximum total contention wait budget in milliseconds
 * @param leaseMillis requested lease TTL in milliseconds
 */
public record AcquireOptions(long waitMillis, long leaseMillis) {
    public static final AcquireOptions DEFAULT = new AcquireOptions(5_000, 30_000);

    /** Validates the complete policy before a backend call. */
    public AcquireOptions {
        if (waitMillis < 1 || waitMillis > 86_400_000) throw new IllegalArgumentException("waitMillis must be 1 through 86400000");
        if (leaseMillis < 1 || leaseMillis > 86_400_000) throw new IllegalArgumentException("leaseMillis must be 1 through 86400000");
    }

    /** Builds a policy using native Java durations; sub-millisecond values are rejected.
     * @param wait total contention wait budget
     * @param lease requested lease TTL
     */
    public AcquireOptions(java.time.Duration wait, java.time.Duration lease) {
        this(toMillisExact(wait, "wait"), toMillisExact(lease, "lease"));
    }

    private static long toMillisExact(java.time.Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) throw new IllegalArgumentException(name + " duration must be positive");
        if (value.getNano() % 1_000_000 != 0) throw new IllegalArgumentException(name + " duration must use whole milliseconds");
        return value.toMillis();
    }
}
