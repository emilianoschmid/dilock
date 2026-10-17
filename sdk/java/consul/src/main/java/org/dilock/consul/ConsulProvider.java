package org.dilock.consul;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.dilock.redis.AcquireOptions;
import org.dilock.redis.DilockException;
import org.dilock.redis.Identity;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Consul HTTP/KV session provider; metadata is validated and never provisioned here. */
public final class ConsulProvider implements AutoCloseable {
    static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private static final String PROFILE = "consul-kv-session-v1";
    private static final String SCHEME = "sha256-framed-utf8-v1";
    private final URI base;
    private final ConsulProviderOptions options;
    private final HttpClient client;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> { Thread t = new Thread(r, "dilock-consul-renewal"); t.setDaemon(true); return t; });
    private final Set<ConsulLease> leases = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    private ConsulProvider(ConsulProviderOptions options, URI base) {
        this.options = options; this.base = base;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(options.requestTimeoutMillis()))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        validateMetadata();
    }

    public static ConsulProvider connect(ConsulProviderOptions options) {
        URI uri;
        try { uri = URI.create(options.endpoint()); }
        catch (RuntimeException e) { throw new IllegalArgumentException("invalid Consul endpoint", e); }
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
                || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath())))
            throw new IllegalArgumentException("Consul endpoint must be an http(s) authority without credentials, path, query, or fragment");
        Identity.resourceId(options.namespace(), "validation");
        if (options.epoch().getBytes(StandardCharsets.UTF_8).length > 1024 || options.epoch().indexOf('\0') >= 0)
            throw new IllegalArgumentException("epoch must be strict UTF-8 and at most 1024 bytes");
        return new ConsulProvider(options, uri);
    }

    public static ConsulProvider connect(String endpoint, String namespace, String epoch) {
        return connect(new ConsulProviderOptions(endpoint, namespace, epoch));
    }

    public static ConsulProvider connect(String endpoint, String epoch) {
        return connect(new ConsulProviderOptions(endpoint, "default", epoch));
    }

    public ConsulLock lock(String resource) {
        ensureOpen();
        return new ConsulLock(this, "dilock/locks/" + Identity.resourceId(options.namespace(), resource));
    }

    AcquireOptions defaultAcquisition() { return options.defaultAcquisition(); }
    String epoch() { return options.epoch(); }
    void track(ConsulLease lease) { leases.add(lease); }
    void untrack(ConsulLease lease) { leases.remove(lease); }
    ScheduledExecutorService scheduler() { return scheduler; }
    long timeout() { return options.requestTimeoutMillis(); }
    long cleanupTimeout() { return options.cleanupTimeoutMillis(); }

    private void validateMetadata() {
        String key = "dilock/namespaces/" + options.namespace() + "/meta";
        JsonNode rows = request("GET", "/v1/kv/" + key + "?consistent", null, timeout(), false, true);
        try {
            if (!rows.isArray() || rows.size() != 1 || !key.equals(rows.get(0).path("Key").asText())) throw new IllegalArgumentException();
            byte[] raw = Base64.getDecoder().decode(rows.get(0).path("Value").asText());
            if (raw.length == 0 || raw.length > 8192) throw new IllegalArgumentException();
            JsonNode meta = JSON.readTree(raw);
            if (!isExactMetadata(meta, options.epoch())) throw new IllegalArgumentException();
        } catch (Exception e) { throw new DilockException(DilockException.Kind.PROTOCOL_MISMATCH, "missing or incompatible Consul namespace metadata", e); }
    }

    static boolean isExactMetadata(JsonNode meta, String epoch) {
        return meta != null && meta.isObject() && meta.size() == 5
                && meta.path("protocol_major").isIntegralNumber() && meta.path("protocol_major").intValue() == 1
                && meta.path("identity_scheme").isTextual() && SCHEME.equals(meta.path("identity_scheme").textValue())
                && meta.path("profile").isTextual() && PROFILE.equals(meta.path("profile").textValue())
                && meta.path("coordination_epoch").isTextual() && epoch.equals(meta.path("coordination_epoch").textValue())
                && meta.has("fixed_lease_ms") && meta.get("fixed_lease_ms").isNull();
    }

    JsonNode request(String method, String path, String body, long timeoutMillis, boolean mutation, boolean missingAllowed) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(base.resolve(path.startsWith("/") ? path : "/" + path))
                .timeout(Duration.ofMillis(timeoutMillis)).header("Accept", "application/json");
        if (options.token() != null) builder.header("X-Consul-Token", options.token());
        if ("GET".equals(method)) builder.GET();
        else builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        final HttpResponse<String> response;
        try { response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new DilockException(mutation ? DilockException.Kind.ACQUISITION_UNKNOWN : DilockException.Kind.BACKEND_UNAVAILABLE, "Consul request was interrupted", e); }
        catch (Exception e) { throw new DilockException(mutation ? DilockException.Kind.ACQUISITION_UNKNOWN : DilockException.Kind.BACKEND_UNAVAILABLE, "Consul request failed", e); }
        if (missingAllowed && response.statusCode() == 404) return null;
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            throw new DilockException(mutation && response.statusCode() >= 500 ? DilockException.Kind.ACQUISITION_UNKNOWN : DilockException.Kind.BACKEND_UNAVAILABLE, "Consul HTTP request rejected (" + response.statusCode() + ")");
        try { return JSON.readTree(response.body()); }
        catch (Exception e) { throw new DilockException(mutation ? DilockException.Kind.ACQUISITION_UNKNOWN : DilockException.Kind.PROTOCOL_MISMATCH, "Consul returned invalid JSON", e); }
    }

    record CreatedSession(String id, long sentNanos) { }

    CreatedSession createSession(int ttlSeconds) {
        String body = "{\"Name\":\"dilock lease\",\"Behavior\":\"release\",\"LockDelay\":\"0s\",\"TTL\":\"" + ttlSeconds + "s\",\"Checks\":[]}";
        long sentNanos = System.nanoTime();
        JsonNode result = request("PUT", "/v1/session/create", body, timeout(), true, false);
        String id = result.path("ID").asText();
        if (id.isEmpty() || id.length() > 128) throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Consul returned invalid session identity");
        return new CreatedSession(id, sentNanos);
    }

    long renewSession(String session) {
        long sentNanos = System.nanoTime();
        JsonNode result = request("PUT", "/v1/session/renew/" + segment(session), null, timeout(), true, false);
        if (!result.isArray() || result.size() != 1 || !session.equals(result.get(0).path("ID").asText()))
            throw new DilockException(DilockException.Kind.LEASE_LOST, "Consul session renewal was not confirmed");
        return sentNanos;
    }

    boolean acquireKey(String key, String session) {
        JsonNode result = request("PUT", "/v1/kv/" + key + "?acquire=" + segment(session), "", timeout(), true, false);
        if (!result.isBoolean()) throw new DilockException(DilockException.Kind.ACQUISITION_UNKNOWN, "Consul acquire response was not boolean");
        return result.booleanValue();
    }

    boolean releaseKey(String key, String session) {
        JsonNode result = request("PUT", "/v1/kv/" + key + "?release=" + segment(session), "", cleanupTimeout(), true, false);
        if (!result.isBoolean()) throw new DilockException(DilockException.Kind.RELEASE_UNKNOWN, "Consul release response was not boolean");
        return result.booleanValue();
    }

    JsonNode owner(String key) {
        return request("GET", "/v1/kv/" + key + "?consistent", null, timeout(), false, true);
    }

    void destroy(String session) {
        request("PUT", "/v1/session/destroy/" + segment(session), null, cleanupTimeout(), true, false);
    }

    private static String segment(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    void ensureOpen() { if (closed.get()) throw new IllegalStateException("Consul provider closed"); }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        // Cleanup calls are permitted during close; the lease release path no longer uses ensureOpen.
        for (ConsulLease lease : Set.copyOf(leases)) try { lease.release(); } catch (RuntimeException ignored) { }
        scheduler.shutdownNow();
    }
}
