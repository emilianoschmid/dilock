package org.dilock.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

final class IdentityTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test void validVectorsMatchDraftOne() throws Exception {
        JsonNode vectors = JSON.readTree(Files.readString(Path.of("../../../spec/vectors/identity.json")));
        for (JsonNode vector : vectors.path("valid")) {
            String digest = Identity.resourceId(vector.path("namespace").asText(), vector.path("resource").asText());
            assertEquals(vector.path("sha256").asText(), digest, vector.path("id").asText());
            assertEquals(vector.path("redis_key").asText(), Identity.redisKey(digest));
        }
    }

    @Test void invalidVectorsFailBeforeAnyBackendIsCreated() throws Exception {
        JsonNode vectors = JSON.readTree(Files.readString(Path.of("../../../spec/vectors/identity.json")));
        for (JsonNode vector : vectors.path("invalid")) {
            String namespace = vector.path("namespace").isTextual() ? vector.path("namespace").asText() : null;
            String resource = vector.path("resource").isTextual() ? vector.path("resource").asText() : null;
            assertThrows(IllegalArgumentException.class, () -> Identity.resourceId(namespace, resource), vector.path("id").asText());
        }
    }

    @Test void identityPreservesUnicodeNormalizationAndCase() {
        assertNotEquals(Identity.resourceId("default", "café"), Identity.resourceId("default", "cafe\u0301"));
        assertNotEquals(Identity.resourceId("default", "Camera"), Identity.resourceId("default", "camera"));
    }

    @Test void strictUtf8AcceptsF1ToF3FourByteScalarsAndRejectsUnpairedSurrogates() {
        assertEquals(64, Identity.resourceId("default", "\uD900\uDC00").length()); // U+40000
        assertEquals(64, Identity.resourceId("default", "\uDBBF\uDFFF").length()); // U+FFFFF
        assertThrows(IllegalArgumentException.class, () -> Identity.resourceId("default", "\uD800"));
        assertThrows(IllegalArgumentException.class, () -> Identity.resourceId("default", "\uDFFF"));
    }

}
