package org.dilock.etcd;

/**
 * Epoch-scoped etcd revision fencing token for a resource-side atomic comparison.
 * The protected resource must atomically reject stale epochs/revisions; possession of this record
 * alone does not fence effects.
 * @param epoch configured coordinator incarnation
 * @param value positive etcd create revision
 */
public record EtcdFence(String epoch, long value) {
    /** Validates that the epoch and revision can identify an etcd fencing generation. */
    public EtcdFence {
        if (epoch == null || epoch.isEmpty() || value <= 0) throw new IllegalArgumentException("fence requires a non-empty epoch and positive create_revision");
    }
}
