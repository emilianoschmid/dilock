package org.dilock.redis;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;

/** Draft 1 resource identity encoding shared by every Dilock SDK. */
public final class Identity {
    private static final byte[] DOMAIN = "dilock.lock.identity\0".getBytes(StandardCharsets.US_ASCII);
    private static final Pattern NAMESPACE = Pattern.compile("[a-z0-9][a-z0-9_.-]{0,63}");

    private Identity() { }

    /**
     * Validates and hashes a resource name without Unicode normalization.
     * The exact UTF-8 bytes are framed with the namespace and SHA-256 hashed; canonically
     * equivalent Unicode strings can therefore have different identities.
     *
     * @param namespace lower-case protocol namespace identifier
     * @param resource non-empty resource name, at most 1024 UTF-8 bytes, with no NUL
     * @return lowercase hexadecimal SHA-256 resource identifier
     * @throws IllegalArgumentException if either value violates the protocol encoding
     */
    public static String resourceId(String namespace, String resource) {
        if (namespace == null || !NAMESPACE.matcher(namespace).matches()) {
            throw new IllegalArgumentException("namespace must match [a-z0-9][a-z0-9_.-]{0,63}");
        }
        if (resource == null || resource.isEmpty() || resource.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("resource must be non-empty and contain no NUL");
        }
        byte[] ns = namespace.getBytes(StandardCharsets.US_ASCII);
        byte[] value;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(resource));
            value = new byte[encoded.remaining()];
            encoded.get(value);
        } catch (CharacterCodingException exception) {
            throw new IllegalArgumentException("resource contains an unpaired surrogate", exception);
        }
        if (value.length < 1 || value.length > 1024) {
            throw new IllegalArgumentException("resource UTF-8 encoding must contain 1 to 1024 bytes");
        }
        ByteBuffer frame = ByteBuffer.allocate(DOMAIN.length + 4 + ns.length + 4 + value.length);
        frame.put(DOMAIN).putInt(ns.length).put(ns).putInt(value.length).put(value);
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(frame.array()));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 is required by the Java platform", impossible);
        }
    }

    /**
     * Returns the canonical Redis key for a resource id.
     * @param resourceId lowercase hexadecimal resource identifier
     * @return canonical Redis lock key
     */
    public static String redisKey(String resourceId) { return "dilock:{" + resourceId + "}:lease"; }
}
