package org.dilock.examples;

import org.dilock.etcd.EtcdFence;
import org.dilock.etcd.EtcdLease;
import org.dilock.etcd.EtcdLock;
import org.dilock.etcd.EtcdProvider;
import org.dilock.redis.DilockException;

import java.util.UUID;

/** A standalone Java consumer demonstrating etcd-native lock acquisition and handoff. */
public final class EtcdConsumer {
    private static final long WAIT_MILLIS = 5_000;
    private static final long LEASE_MILLIS = 10_000;

    private EtcdConsumer() {}

    public static void main(String[] args) throws InterruptedException {
        String endpoint = requiredEnvironment("DILOCK_ETCD_ENDPOINT");
        String namespace = requiredEnvironment("DILOCK_NAMESPACE");
        String epoch = requiredEnvironment("DILOCK_EPOCH");
        String resource = "docs-example/java-etcd/" + UUID.randomUUID();

        try (EtcdProvider ownerProvider = EtcdProvider.connect(endpoint, namespace, epoch);
             EtcdProvider successorProvider = EtcdProvider.connect(endpoint, namespace, epoch)) {
            EtcdLock ownerLock = ownerProvider.lock(resource);
            EtcdLock successorLock = successorProvider.lock(resource);

            try (EtcdLease owner = ownerLock.acquire(WAIT_MILLIS, LEASE_MILLIS)) {
                owner.assertValid();
                printFence("owner", owner.fence());
                if (owner.release() != EtcdLease.ReleaseOutcome.RELEASED) {
                    throw new IllegalStateException("Owner release was not acknowledged.");
                }
            }
            System.out.println("owner: release acknowledged");

            try (EtcdLease successor = successorLock.acquire(WAIT_MILLIS, LEASE_MILLIS)) {
                successor.assertValid();
                printFence("successor", successor.fence());
                if (successor.release() != EtcdLease.ReleaseOutcome.RELEASED) {
                    throw new IllegalStateException("Successor release was not acknowledged.");
                }
            }
            System.out.println("successor: release acknowledged");
        } catch (DilockException error) {
            System.err.println("Dilock failed: " + error.kind() + ": " + error.getMessage());
            throw error;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private static void printFence(String owner, EtcdFence fence) {
        System.out.println(owner + ": acquired fence " + fence.epoch() + "/" + fence.value());
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set the required environment variable " + name);
        }
        return value;
    }
}
