package org.dilock.etcd;

/** Interrupted acquisition with explicit knowledge about remote cleanup confirmation. */
public final class EtcdAcquisitionCancelledException extends InterruptedException {
    private static final long serialVersionUID = 1L;
    /** Whether remote cleanup failed to receive confirmation. */
    private final boolean remoteOutcomeUnknown;

    EtcdAcquisitionCancelledException(boolean remoteOutcomeUnknown, String message, Throwable cause) {
        super(message);
        this.remoteOutcomeUnknown = remoteOutcomeUnknown;
        if (cause != null) initCause(cause);
    }

    /**
     * Reports whether cleanup was left uncertain after interruption.
     * @return {@code true} if lease revoke or exact-key cleanup was not confirmed
     */
    public boolean remoteOutcomeUnknown() { return remoteOutcomeUnknown; }
}
