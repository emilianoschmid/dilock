package org.dilock.redis;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class RedisScriptsTest {
    @Test void bundledScriptsMatchCanonicalSources() throws Exception {
        assertEquals(Files.readString(Path.of("../../../spec/backends/redis-release.lua")).replace("\r\n", "\n"), RedisProvider.RELEASE_LUA);
        assertEquals(Files.readString(Path.of("../../../spec/backends/redis-renew.lua")).replace("\r\n", "\n"), RedisProvider.RENEW_LUA);
    }

    @Test void namespaceMetadataRejectsCoercibleButWrongJsonTypes() {
        assertDoesNotThrow(() -> RedisProvider.validateMetadataValue(metadata("1", "\"epoch\""), "epoch"));
        assertProtocolMismatch(metadata("1.0", "\"epoch\""));
        assertProtocolMismatch(metadata("1.5", "\"epoch\""));
        assertProtocolMismatch(metadata("4294967297", "\"epoch\""));
        assertProtocolMismatch(metadata("\"1\"", "\"epoch\""));
        assertProtocolMismatch(metadata("1", "123"));
        assertProtocolMismatch(metadata("1", "\"epoch\"").replace("\"protocol_major\":1", "\"protocol_major\":1,\"protocol_major\":1"));
        assertDoesNotThrow(() -> RedisProvider.validateMetadataValue(metadata("1", "\"epoch\"").replace(
                "\"fixed_lease_ms\":null", "\"fixed_lease_ms\":null,\"extension\":{\"x\":1,\"x\":2}"), "epoch"));
    }

    private static String metadata(String protocolMajor, String epoch) {
        return "{\"protocol_major\":" + protocolMajor
                + ",\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"redis-single-authority-v1\""
                + ",\"coordination_epoch\":" + epoch + ",\"fixed_lease_ms\":null}";
    }

    private static void assertProtocolMismatch(String raw) {
        var error = assertThrows(DilockException.class, () -> RedisProvider.validateMetadataValue(raw, "epoch"));
        assertEquals(DilockException.Kind.PROTOCOL_MISMATCH, error.kind());
    }
}
