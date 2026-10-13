package org.dilock.redis;

/**
 * Interrupted Redis acquisition with explicit remote-cleanup certainty.
 * No lease is returned. The calling thread's interruption flag remains set;
 * an unconfirmed cleanup may leave an owner key until its TTL expires.
 */
public final class RedisAcquisitionCancelledException extends InterruptedException {
    private static final long serialVersionUID = 1L;
    /** Whether owner cleanup failed to receive a definite acknowledgement. */
    private final boolean remoteOutcomeUnknown;

    RedisAcquisitionCancelledException(boolean remoteOutcomeUnknown, String message) {
        super(message);
        this.remoteOutcomeUnknown = remoteOutcomeUnknown;
    }

    /**
     * Whether owner-specific cleanup lacked a definite Redis acknowledgement.
     * @return {@code true} if the acquisition may remain held remotely
     */
    public boolean remoteOutcomeUnknown() { return remoteOutcomeUnknown; }
}
