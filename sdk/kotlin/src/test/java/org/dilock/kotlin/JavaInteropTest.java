package org.dilock.kotlin;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class JavaInteropTest {
    @Test void javaCanConstructAndReadTheKotlinAcquisitionPolicy() {
        assertEquals(AcquirePolicy.DEFAULT, new AcquirePolicy(Duration.ofSeconds(5), Duration.ofSeconds(30)));
    }

    @Test void javaCanUseTheBackendNeutralReleaseOutcome() {
        assertEquals(ReleaseOutcome.RELEASED, ReleaseOutcome.valueOf("RELEASED"));
        assertEquals(ReleaseOutcome.ALREADY_NOT_OWNED, ReleaseOutcome.valueOf("ALREADY_NOT_OWNED"));
    }
}
