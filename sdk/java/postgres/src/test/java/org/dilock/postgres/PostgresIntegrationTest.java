package org.dilock.postgres;

import org.dilock.redis.AcquireOptions;
import org.dilock.redis.DilockException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "DILOCK_POSTGRES_DSN", matches = ".+")
class PostgresIntegrationTest {
    private static final AcquireOptions OPTIONS = new AcquireOptions(250, 5_000);

    @Test
    void realPostgresContendsProgressesAndHandsOffOnTheSameProfile() {
        String dsn = System.getenv("DILOCK_POSTGRES_DSN");
        String namespace = "ci-postgres-java";
        String epoch = "ci-postgres-java-epoch";
        PostgresProvider.provisionNamespace(dsn, namespace, epoch);
        try (PostgresProvider first = PostgresProvider.connect(dsn, namespace, epoch);
             PostgresProvider second = PostgresProvider.connect(dsn, namespace, epoch)) {
            PostgresLock firstLock = first.lock("java-provider");
            PostgresLock secondLock = second.lock("java-provider");
            PostgresLease lease = firstLock.tryAcquire().orElseThrow();
            try {
                lease.renew();
                assertNull(secondLock.tryAcquire().orElse(null));
                PostgresLease different = second.lock("java-different-resource").tryAcquire().orElseThrow();
                assertEquals(PostgresLease.ReleaseOutcome.RELEASED, different.release());
                DilockException timeout = assertThrows(DilockException.class,
                        () -> secondLock.acquire(OPTIONS));
                assertEquals(DilockException.Kind.ACQUISITION_TIMEOUT, timeout.kind());
                assertEquals(PostgresLease.ReleaseOutcome.RELEASED, lease.release());
            } finally {
                lease.close();
            }
            PostgresLease handoff = secondLock.tryAcquire().orElseThrow();
            assertEquals(PostgresLease.ReleaseOutcome.RELEASED, handoff.release());
        }
    }
}
