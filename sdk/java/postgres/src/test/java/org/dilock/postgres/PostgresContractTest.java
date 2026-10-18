package org.dilock.postgres;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PostgresContractTest {
    @Test void advisoryKeyUsesSignedFirstDigestWord() {
        assertEquals(0L, PostgresProvider.advisoryKey("0000000000000000" + "0".repeat(48)));
        assertEquals(Long.MAX_VALUE, PostgresProvider.advisoryKey("7fffffffffffffff" + "0".repeat(48)));
        assertEquals(Long.MIN_VALUE, PostgresProvider.advisoryKey("8000000000000000" + "0".repeat(48)));
        assertEquals(-1L, PostgresProvider.advisoryKey("ffffffffffffffff" + "0".repeat(48)));
    }

    @Test void optionsValidatePostgresUrlAndNamespaceBeforeIOMustOccur() {
        assertThrows(IllegalArgumentException.class, () -> new PostgresProviderOptions("jdbc:mysql://localhost/db", "epoch"));
        assertThrows(IllegalArgumentException.class, () -> new PostgresProviderOptions("jdbc:postgresql://localhost/db", "bad namespace", "epoch"));
    }
}
