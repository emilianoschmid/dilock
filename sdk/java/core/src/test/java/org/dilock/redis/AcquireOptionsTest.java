package org.dilock.redis;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class AcquireOptionsTest {
    @Test void defaultsAndNativeDurationOptionsAreValidated() {
        assertEquals(new AcquireOptions(5_000, 30_000), AcquireOptions.DEFAULT);
        assertEquals(new AcquireOptions(7_000, 45_000), new AcquireOptions(Duration.ofSeconds(7), Duration.ofSeconds(45)));
        assertThrows(IllegalArgumentException.class, () -> new AcquireOptions(0, 30_000));
        assertThrows(IllegalArgumentException.class, () -> new AcquireOptions(Duration.ZERO, Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class, () -> new AcquireOptions(-1, 30_000));
        assertThrows(IllegalArgumentException.class, () -> new AcquireOptions(Duration.ofNanos(1), Duration.ofSeconds(1)));
    }
}
