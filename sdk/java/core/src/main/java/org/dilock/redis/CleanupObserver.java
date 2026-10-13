package org.dilock.redis;

/** Receives uncertain automatic cleanup outcomes. Observer failures are always suppressed. */
@FunctionalInterface
public interface CleanupObserver {
    /** Receives a cleanup observation; implementations should be fast and must not throw.
     * @param observation safe observation without owner tokens
     */
    void onCleanup(CleanupObservation observation);
}
