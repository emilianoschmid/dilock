package org.dilock.redis;

/** Safe diagnostic for best-effort scope cleanup; it never contains an owner token.
 * @param backend backend identifier
 * @param outcome cleanup classification
 * @param reason cleanup path identifier
 */
public record CleanupObservation(String backend, String outcome, String reason) { }
