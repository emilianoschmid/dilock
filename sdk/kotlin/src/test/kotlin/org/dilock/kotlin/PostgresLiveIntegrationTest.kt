package org.dilock.kotlin

import org.dilock.postgres.PostgresProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/** Opt-in live test of the Kotlin facade over the Java PostgreSQL session provider. */
class PostgresLiveIntegrationTest {
    @Test
    fun independentProvidersContendAndHandOffOnTheSameResource() {
        val dsn = System.getenv("DILOCK_POSTGRES_DSN")
        assumeTrue(!dsn.isNullOrBlank(), "set DILOCK_POSTGRES_DSN to opt in to live PostgreSQL")
        val namespace = "kotlin-pg-${System.nanoTime()}"
        val epoch = "kotlin-epoch-${System.nanoTime()}"
        PostgresProvider.provisionNamespace(dsn, namespace, epoch)

        val first = DilockPostgres.connect(dsn, namespace, epoch)
        val second = DilockPostgres.connect(dsn, namespace, epoch)
        try {
            val firstLease = first.lock("kotlin-provider").tryAcquire()
            checkNotNull(firstLease)
            try {
                assertNull(second.lock("kotlin-provider").tryAcquire())
                assertEquals(
                    org.dilock.postgres.PostgresLease.ReleaseOutcome.RELEASED,
                    firstLease.release(),
                )
            } finally {
                firstLease.close()
            }

            val handoff = second.lock("kotlin-provider").tryAcquire()
            checkNotNull(handoff)
            try {
                assertEquals(
                    org.dilock.postgres.PostgresLease.ReleaseOutcome.RELEASED,
                    handoff.release(),
                )
            } finally {
                handoff.close()
            }
        } finally {
            first.close()
            second.close()
        }
    }
}
