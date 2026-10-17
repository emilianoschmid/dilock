package org.dilock.consul;

/** Epoch-scoped fence from Consul's retained KV LockIndex. */
public record ConsulFence(String epoch, long lockIndex) {
    public ConsulFence {
        if (epoch == null || epoch.isEmpty() || lockIndex <= 0) {
            throw new IllegalArgumentException("fence requires a non-empty epoch and positive LockIndex");
        }
    }
}
