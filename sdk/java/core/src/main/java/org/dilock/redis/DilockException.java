package org.dilock.redis;

/** Typed failure for Dilock operations other than ordinary contention. */
public class DilockException extends RuntimeException {
    /** Machine-readable failure category; none represents ordinary busy contention. */
    public enum Kind {
        /** The selected backend could not complete a required operation. */ BACKEND_UNAVAILABLE,
        /** Backend protocol metadata or key representation is incompatible. */ PROTOCOL_MISMATCH,
        /** Acquisition may have committed remotely, but the result was not confirmed. */ ACQUISITION_UNKNOWN,
        /** The bounded acquisition wait expired due to contention. */ ACQUISITION_TIMEOUT,
        /** Release may have committed remotely, but the result was not confirmed. */ RELEASE_UNKNOWN,
        /** The lease is no longer locally valid or owner-verified. */ LEASE_LOST,
        /** The selected backend cannot safely provide a requested capability. */ UNSUPPORTED_CAPABILITY
    }
    /** Stable category exposed to callers for programmatic handling. */
    private final Kind kind;
    /**
     * Creates a typed failure without a lower-level cause.
     * @param kind stable failure category
     * @param message human-readable diagnostic
     */
    public DilockException(Kind kind, String message) { super(message); this.kind = kind; }
    /**
     * Creates a typed failure with its lower-level cause.
     * @param kind stable failure category
     * @param message human-readable diagnostic
     * @param cause underlying failure, if known
     */
    public DilockException(Kind kind, String message, Throwable cause) { super(message, cause); this.kind = kind; }
    /** Returns the stable failure category carried by this exception.
     * @return stable failure category
     */
    public Kind kind() { return kind; }
}
