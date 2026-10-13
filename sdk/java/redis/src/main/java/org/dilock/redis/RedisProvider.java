package org.dilock.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.net.URI;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns one configured Redis authority and its client connection.
 * A provider is safe to share between threads. Closing it makes future operations fail and
 * attempts bounded, owner-checked cleanup of leases created through it; Redis TTL remains the
 * bound if a cleanup acknowledgement cannot be obtained. It never changes coordinators.
 */
public final class RedisProvider implements AutoCloseable {
    @FunctionalInterface
    interface OwnerReleaseCommand { Object release(String key, String owner); }
    @FunctionalInterface
    interface OwnerAcquireCommand { String acquire(String key, String owner, long ttlMillis); }

    static final String RELEASE_LUA = "-- Dilock draft 1 reference. KEYS[1] = lease key; ARGV[1] = full owner value.\n-- Atomic owner check prevents a stale releaser from deleting a successor.\nif redis.call('GET', KEYS[1]) == ARGV[1] then\n    return redis.call('DEL', KEYS[1])\nend\nreturn 0\n";
    static final String RENEW_LUA = "-- Dilock draft 1 reference. KEYS[1] = lease key.\n-- ARGV[1] = full owner value; ARGV[2] = positive integer TTL in milliseconds.\nlocal ttl = tonumber(ARGV[2])\nif not ttl or ttl ~= math.floor(ttl) or ttl <= 0 or ttl > 86400000 then\n    return redis.error_reply('DILOCK_INVALID_TTL')\nend\nif redis.call('GET', KEYS[1]) == ARGV[1] then\n    return redis.call('PEXPIRE', KEYS[1], ttl)\nend\nreturn 0\n";
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SCHEME = "sha256-framed-utf8-v1";
    private static final String PROFILE = "redis-single-authority-v1";

    private final RedisClient client;
    private final RedisClient cleanupClient;
    private final String namespace;
    private final String epoch;
    private final long rpcMillis;
    private final long cleanupMillis;
    private final AcquireOptions defaultAcquisition;
    private final CleanupObserver cleanupObserver;
    private final OwnerReleaseCommand ownerReleaseCommand;
    private final OwnerAcquireCommand ownerAcquireCommand;
    private final Map<String, RedisLease> leases = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private RedisProvider(RedisClient client, RedisClient cleanupClient, String namespace, String epoch, long rpcMillis, long cleanupMillis,
                          AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver) {
        this(client, cleanupClient, namespace, epoch, rpcMillis, cleanupMillis, defaultAcquisition, cleanupObserver,
                (key, owner) -> cleanupClient.eval(RELEASE_LUA, List.of(key), List.of(owner)));
    }

    RedisProvider(RedisClient client, RedisClient cleanupClient, String namespace, String epoch, long rpcMillis, long cleanupMillis,
                  AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver, OwnerReleaseCommand ownerReleaseCommand) {
        this(client, cleanupClient, namespace, epoch, rpcMillis, cleanupMillis, defaultAcquisition,
                cleanupObserver, ownerReleaseCommand,
                (key, owner, ttl) -> client.set(key, owner, SetParams.setParams().nx().px(ttl)));
    }

    RedisProvider(RedisClient client, RedisClient cleanupClient, String namespace, String epoch, long rpcMillis, long cleanupMillis,
                  AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver, OwnerReleaseCommand ownerReleaseCommand,
                  OwnerAcquireCommand ownerAcquireCommand) {
        this.client = client; this.cleanupClient = cleanupClient; this.namespace = namespace; this.epoch = epoch;
        this.rpcMillis = rpcMillis; this.cleanupMillis = cleanupMillis;
        this.defaultAcquisition = defaultAcquisition; this.cleanupObserver = cleanupObserver;
        this.ownerReleaseCommand = ownerReleaseCommand;
        this.ownerAcquireCommand = ownerAcquireCommand;
    }

    /**
     * Connects to one Redis authority and validates pre-provisioned draft 1 metadata.
     * The provider owns the returned client's connection pool until {@link #close()}.
     * Connection and metadata calls use the configured RPC timeout; cleanup uses a separate client
     * bounded by the configured cleanup timeout. This method does not provision or modify metadata.
     *
     * @param endpoint Redis URI using {@code redis://} or {@code rediss://}
     * @param namespace pre-provisioned namespace identifier
     * @param epoch configured coordinator incarnation recorded in namespace metadata
     * @return an open provider
     * @throws IllegalArgumentException if an argument is malformed
     * @throws DilockException if metadata is incompatible or Redis is unavailable
     */
    public static RedisProvider connect(String endpoint, String namespace, String epoch) {
        return connect(new RedisProviderOptions(endpoint, namespace, epoch));
    }

    /** Connects with namespace {@code default} and the standard finite acquisition policy.
     * @param endpoint Redis URI
     * @param epoch operator-approved epoch already recorded in namespace metadata
     * @return open provider owning one Redis client
     * @throws DilockException if metadata is invalid or backend is unavailable
     */
    public static RedisProvider connect(String endpoint, String epoch) {
        return connect(new RedisProviderOptions(endpoint, "default", epoch));
    }

    /** Connects using a validated immutable configuration and validates existing metadata.
     * @param options full provider settings
     * @return open provider owning one Redis client
     * @throws IllegalArgumentException for invalid local configuration
     * @throws DilockException if metadata is invalid or backend is unavailable
     */
    public static RedisProvider connect(RedisProviderOptions options) {
        String endpoint = options.endpoint();
        String namespace = options.namespace();
        String epoch = options.epoch();
        Identity.resourceId(namespace, "validation");
        if (epoch == null || epoch.isEmpty() || epoch.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) throw new IllegalArgumentException("epoch must be non-empty and at most 1024 UTF-8 bytes");
        URI uri;
        try { uri = URI.create(endpoint); } catch (RuntimeException error) { throw new IllegalArgumentException("invalid Redis endpoint", error); }
        if (!("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme()))) throw new IllegalArgumentException("endpoint must use redis:// or rediss://");
        int rpcMillis = Math.toIntExact(options.rpcTimeoutMillis());
        if (uri.getHost() == null) throw new IllegalArgumentException("Redis endpoint must contain a host");
        int port = uri.getPort() < 0 ? 6379 : uri.getPort();
        RedisClient client = createClient(uri, port, rpcMillis);
        RedisClient cleanupClient = createClient(uri, port, Math.toIntExact(options.cleanupTimeoutMillis()));
        RedisProvider provider = new RedisProvider(client, cleanupClient, namespace, epoch, rpcMillis, options.cleanupTimeoutMillis(), options.defaultAcquisition(), options.cleanupObserver());
        try { provider.validateMetadata(); return provider; }
        catch (RuntimeException error) { cleanupClient.close(); client.close(); throw error; }
    }

    private static RedisClient createClient(URI uri, int port, int timeoutMillis) {
        DefaultJedisClientConfig config = clientConfig(uri, timeoutMillis);
        return RedisClient.builder().hostAndPort(uri.getHost(), port).clientConfig(config).build();
    }

    static DefaultJedisClientConfig clientConfig(URI uri, int timeoutMillis) {
        return DefaultJedisClientConfig.builder(uri)
                .connectionTimeoutMillis(timeoutMillis).socketTimeoutMillis(timeoutMillis).build();
    }

    /**
     * Returns a reusable immutable resource handle.
     * Handles share this provider's Redis client and are safe for concurrent calls.
     *
     * @param resource non-empty UTF-8 resource name
     * @return a handle bound to this provider and resource
     * @throws IllegalStateException if this provider is closed
     * @throws IllegalArgumentException if the namespace/resource identity is invalid
     */
    public RedisLock lock(String resource) {
        ensureOpen();
        Identity.resourceId(namespace, resource);
        return new RedisLock(this, resource);
    }

    private void validateMetadata() {
        String key = "dilock:namespace:" + namespace + ":meta";
        final String raw;
        try { raw = client.get(key); }
        catch (RuntimeException error) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "failed reading Redis namespace metadata", error); }
        if (raw == null) throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "namespace metadata is missing; provision it explicitly before use");
        validateMetadataValue(raw, epoch);
    }

    static void validateMetadataValue(String raw, String epoch) {
        if (raw.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 8192) throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "namespace metadata exceeds 8192 bytes");
        try {
            if (!hasUniqueTopLevelProperties(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "Redis namespace metadata contains duplicate top-level properties");
            JsonNode value = JSON.readTree(raw);
            if (!hasExactMetadataTypes(value) || !value.path("protocol_major").isIntegralNumber()
                    || !value.path("protocol_major").canConvertToInt()
                    || value.path("protocol_major").intValue() != 1
                    || !SCHEME.equals(value.path("identity_scheme").textValue())
                    || !PROFILE.equals(value.path("profile").textValue()) || !epoch.equals(value.path("coordination_epoch").textValue())
                    || !value.has("fixed_lease_ms") || !value.get("fixed_lease_ms").isNull()) {
                throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "Redis namespace metadata is incompatible with draft 1 configuration");
            }
        } catch (DilockException error) { throw error; }
        catch (Exception error) { throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "Redis namespace metadata is not valid JSON", error); }
    }

    private static boolean hasUniqueTopLevelProperties(byte[] raw) throws java.io.IOException {
        try (JsonParser parser = JSON.getFactory().createParser(raw)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) return false;
            int depth = 1;
            Set<String> names = new HashSet<>();
            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                if (depth == 1 && token == JsonToken.FIELD_NAME && !names.add(parser.currentName())) return false;
                if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) depth++;
                else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                    if (--depth == 0) return parser.nextToken() == null;
                }
            }
            return false;
        }
    }

    Optional<RedisLease> tryAcquire(String resource, long leaseMillis) throws InterruptedException {
        ensureOpen();
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("acquisition interrupted before send");
        if (rpcMillis + margin(leaseMillis) >= leaseMillis) throw new IllegalArgumentException("RPC timeout plus safety margin must be less than the lease");
        String key = Identity.redisKey(Identity.resourceId(namespace, resource));
        byte[] bytes = new byte[32]; RANDOM.nextBytes(bytes);
        String owner = "dilock/1/" + HexFormat.of().formatHex(bytes);
        long sentAt = System.nanoTime();
        final String response;
        try { response = ownerAcquireCommand.acquire(key, owner, leaseMillis); }
        catch (JedisDataException error) {
            throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "Redis rejected the acquisition command", error);
        }
        catch (RuntimeException error) {
            cleanupOwner(key, owner, "acquisition-unknown");
            throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Redis did not confirm whether acquisition committed; owner-specific cleanup was attempted", error);
        }
        if (Thread.currentThread().isInterrupted()) {
            boolean cleanupConfirmed = !"OK".equals(response) || cleanupOwner(key, owner, "acquisition-interrupted");
            // Cleanup must not consume the caller's interruption even if a
            // transport clears its flag while performing bounded owner cleanup.
            Thread.currentThread().interrupt();
            throw new RedisAcquisitionCancelledException(!cleanupConfirmed,
                    "acquisition interrupted after send; remote cleanup " + (cleanupConfirmed ? "confirmed" : "unknown"));
        }
        if (response == null) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("acquisition interrupted while classifying contention");
            final String keyType;
            try { keyType = client.type(key); }
            catch (RuntimeException error) {
                throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "Redis could not classify a contended lock key", error);
            }
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("acquisition interrupted while classifying contention");
            if (!"string".equals(keyType) && !"none".equals(keyType))
                throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "Redis lock key has unexpected type '" + keyType + "'");
            return Optional.empty();
        }
        long deadline = sentAt + Duration.ofMillis(leaseMillis - margin(leaseMillis)).toNanos();
        if (System.nanoTime() >= deadline) {
            cleanupOwner(key, owner, "acquisition-late-acknowledgement");
            throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Redis confirmed acquisition after the conservative local deadline");
        }
        RedisLease lease = new RedisLease(this, key, owner, leaseMillis, deadline);
        leases.put(lease.id(), lease);
        return Optional.of(lease);
    }

    private static long margin(long ttlMillis) { return Math.max(10, (ttlMillis + 99) / 100); }
    boolean cleanupOwner(String key, String owner, String reason) {
        try {
            Object result = ownerReleaseCommand.release(key, owner);
            boolean removed = Long.valueOf(1).equals(result) || "1".equals(result);
            boolean absent = Long.valueOf(0).equals(result) || "0".equals(result);
            observeCleanup(removed ? "released" : absent ? "already_not_owned" : "unknown", reason);
            return removed || absent;
        } catch (RuntimeException error) {
            observeCleanup("unknown", reason + ": " + error.getClass().getSimpleName());
            /* Preserve the acquisition/interruption outcome. The TTL bounds an orphan. */
            return false;
        }
    }

    private static boolean hasExactMetadataTypes(JsonNode value) {
        return value != null && value.isObject()
                && value.path("protocol_major").isNumber()
                && value.path("identity_scheme").isTextual()
                && value.path("profile").isTextual()
                && value.path("coordination_epoch").isTextual();
    }
    RedisLease.ReleaseOutcome release(RedisLease lease) {
        try {
            Object result = ownerReleaseCommand.release(lease.key(), lease.owner());
            leases.remove(lease.id());
            return Long.valueOf(1).equals(result) || "1".equals(result) ? RedisLease.ReleaseOutcome.RELEASED : RedisLease.ReleaseOutcome.ALREADY_NOT_OWNED;
        } catch (RuntimeException error) { throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "Redis did not confirm lease release", error); }
    }
    boolean renew(RedisLease lease, long ttl) {
        if (rpcMillis + margin(ttl) >= ttl) throw new IllegalArgumentException("RPC timeout plus safety margin must be less than the lease");
        long sentAt = System.nanoTime();
        try {
            Object result = client.eval(RENEW_LUA, List.of(lease.key()), List.of(lease.owner(), Long.toString(ttl)));
            if (!(Long.valueOf(1).equals(result) || "1".equals(result))) return false;
            return lease.confirmRenewal(sentAt + Duration.ofMillis(ttl - margin(ttl)).toNanos());
        } catch (RuntimeException error) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "Redis lease renewal failed", error); }
    }
    AcquireOptions defaultAcquisition() { return defaultAcquisition; }
    void observeCleanup(String outcome, String reason) {
        if (cleanupObserver == null) {
            System.getLogger("org.dilock.cleanup").log(System.Logger.Level.WARNING,
                    "Dilock Redis cleanup outcome is {0} ({1})", outcome, reason);
            return;
        }
        try { cleanupObserver.onCleanup(new CleanupObservation("redis", outcome, reason)); }
        catch (RuntimeException ignored) { /* Diagnostics cannot abort lease cleanup. */ }
    }
    void ensureOpen() { if (closed.get()) throw new IllegalStateException("provider is closed"); }

    /**
     * Explicit create-if-absent metadata provisioning. Existing metadata is parsed and compared;
     * conflicting values are never overwritten. This operation does not close {@code client};
     * its caller retains ownership. It performs bounded client commands using the caller's
     * configured timeouts.
     *
     * @param client caller-owned Redis client
     * @param namespace namespace to create or validate
     * @param epoch configured coordinator incarnation
     * @return {@code "created"} or {@code "compatible-existing"}
     * @throws IllegalArgumentException if namespace or epoch is invalid
     * @throws DilockException if Redis fails or existing metadata is incompatible
     */
    public static String provisionNamespace(RedisClient client, String namespace, String epoch) {
        Identity.resourceId(namespace, "validation");
        if (epoch == null || epoch.isEmpty() || epoch.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) throw new IllegalArgumentException("invalid epoch");
        var metadataValue = JSON.createObjectNode();
        metadataValue.put("protocol_major", 1);
        metadataValue.put("identity_scheme", SCHEME);
        metadataValue.put("profile", PROFILE);
        metadataValue.put("coordination_epoch", epoch);
        metadataValue.putNull("fixed_lease_ms");
        final String metadata;
        try { metadata = JSON.writeValueAsString(metadataValue); }
        catch (Exception error) { throw new IllegalStateException("failed encoding Redis namespace metadata", error); }
        String key = "dilock:namespace:" + namespace + ":meta";
        String created;
        try { created = client.set(key, metadata, SetParams.setParams().nx()); }
        catch (RuntimeException error) { throw new DilockException(DilockException.Kind.BACKEND_UNAVAILABLE, "failed provisioning namespace metadata", error); }
        String current = client.get(key);
        if (current == null) throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "namespace metadata disappeared during provisioning");
        RedisProvider check = new RedisProvider(client, client, namespace, epoch, 2000, 2000, AcquireOptions.DEFAULT, null);
        check.validateMetadata();
        return "OK".equals(created) ? "created" : "compatible-existing";
    }

    /**
     * Attempts cleanup of leases tracked by this provider using the configured cleanup timeout,
     * then closes both Redis clients. Each command is bounded by cleanupTimeoutMillis; a provider
     * with many active leases can therefore spend up to that bound per lease. Cleanup failures
     * are observed and suppressed because expiry bounds abandoned leases. Idempotent.
     */
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (RedisLease lease : leases.values()) {
            try { lease.release(); } catch (RuntimeException error) { observeCleanup("unknown", "provider-close: " + error.getClass().getSimpleName()); }
        }
        try { cleanupClient.close(); }
        finally { client.close(); }
    }
}
