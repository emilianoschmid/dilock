package org.dilock.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dilock.etcd.EtcdLease;
import org.dilock.etcd.EtcdAcquisitionCancelledException;
import org.dilock.etcd.EtcdProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** JSONL worker for the shared cross-language etcd conformance harness. */
final class EtcdWorker {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_LINE = 64 * 1024;
    private EtcdWorker() { }

    static void main(String[] args) throws Exception {
        JsonNode config = JSON.readTree(Files.readString(Path.of(args[0])));
        EtcdProvider provider = EtcdProvider.connect(config.path("endpoint").asText(), config.path("namespace").asText(), config.path("epoch").asText());
        Map<String, EtcdLease> leases = new ConcurrentHashMap<>(); Map<String, Future<?>> pending = new ConcurrentHashMap<>();
        Map<String, String> released = new ConcurrentHashMap<>();
        var ids = ConcurrentHashMap.<String>newKeySet(); Object outputLock = new Object();
        ExecutorService executor = Executors.newCachedThreadPool(task -> { Thread t = new Thread(task, "dilock-etcd-worker-request"); t.setDaemon(true); return t; });
        try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = boundedLine(reader)) != null) {
                JsonNode request;
                try { request = JSON.readTree(line); } catch (Exception error) { System.err.println("worker: malformed JSON input"); continue; }
                String id = request.path("id").asText(); String op = request.path("op").asText();
                if (id.isEmpty() || op.isEmpty()) continue;
                if (!ids.add(id)) { write(outputLock, envelope(id, "invalid_argument")); continue; }
                if ("cancel_request".equals(op)) {
                    Future<?> target = pending.get(request.path("target_id").asText()); boolean cancelled = target != null && target.cancel(true);
                    ObjectNode response = envelope(id, "ok"); response.put("cancelled", cancelled); write(outputLock, response); ids.remove(id); continue;
                }
                if ("shutdown".equals(op)) { ObjectNode response = envelope(id, "ok"); response.put("outstanding_cleanup_attempts", pending.size()); write(outputLock, response); ids.remove(id); break; }
                var task = new java.util.concurrent.FutureTask<Void>(() -> {
                    try { process(provider, leases, released, request, outputLock); }
                    catch (Throwable error) {
                        ObjectNode failure = envelope(id, mapError(error));
                        if (error instanceof EtcdAcquisitionCancelledException cancelled) failure.put("remote_outcome_unknown", cancelled.remoteOutcomeUnknown());
                        else if (error instanceof InterruptedException) failure.put("remote_outcome_unknown", false);
                        write(outputLock, failure);
                    }
                    finally { pending.remove(id); ids.remove(id); }
                    return null;
                });
                pending.put(id, task); executor.execute(task);
            }
        } finally {
            for (Future<?> task : pending.values()) task.cancel(true);
            executor.shutdownNow(); executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS); provider.close();
        }
    }

    private static void process(EtcdProvider provider, Map<String, EtcdLease> leases, Map<String, String> released, JsonNode request, Object outputLock) throws InterruptedException {
        String id = request.path("id").asText(); String op = request.path("op").asText(); ObjectNode response;
        switch (op) {
            case "hello" -> { response = envelope(id, "ok"); response.put("sdk_version", "0.1.0"); response.put("protocol", 1); response.put("backend", "etcd"); response.put("server_version", "unknown"); response.set("capabilities", JSON.createObjectNode().put("renewal", true).put("auto_renew", true).put("fencing", true).put("try_acquire", false)); }
            case "try_acquire" -> throw new DilockException(DilockException.Kind.UNSUPPORTED_CAPABILITY, "etcd native Lock does not provide a safe try-acquire operation");
            case "acquire" -> {
                var lock = provider.lock(request.path("resource").asText()); long ttl = request.path("lease_ms").asLong(30_000); long wait = "acquire".equals(op) ? request.path("wait_ms").asLong(5000) : 0;
                EtcdLease lease = lock.acquire(wait, ttl, request.path("auto_renew").asBoolean(false));
                if (lease == null) { write(outputLock, envelope(id, "busy")); return; }
                String localId = UUID.randomUUID().toString(); leases.put(localId, lease); response = envelope(id, "acquired"); response.put("lease_id", localId); response.put("effective_lease_ms", lease.effectiveLeaseMillis());
                ObjectNode fence = JSON.createObjectNode(); fence.put("epoch", lease.epoch()); fence.put("value", Long.toString(lease.createRevision())); response.set("fence", fence);
            }
            case "release" -> {
                String key = request.path("lease_id").asText();
                String prior = released.get(key);
                if (prior != null) { response = envelope(id, prior); break; }
                EtcdLease lease = leases.get(key); if (lease == null) throw new IllegalArgumentException("unknown lease_id");
                var outcome = lease.release(); String status = outcome == EtcdLease.ReleaseOutcome.RELEASED ? "released" : "already_not_owned";
                leases.remove(key); released.put(key, status); response = envelope(id, status);
            }
            case "inspect" -> { EtcdLease lease = leases.get(request.path("lease_id").asText()); if (lease == null) throw new IllegalArgumentException("unknown lease_id"); response = envelope(id, "ok"); response.put("state", lease.state().name().toLowerCase(java.util.Locale.ROOT)); }
            default -> throw new IllegalArgumentException("unknown operation");
        }
        write(outputLock, response);
    }
    private static String mapError(Throwable error) {
        if (error instanceof InterruptedException) { Thread.currentThread().interrupt(); return "cancelled"; }
        if (error instanceof DilockException d) return switch (d.kind()) { case BACKEND_UNAVAILABLE -> "backend_unavailable"; case PROTOCOL_MISMATCH -> "protocol_mismatch"; case ACQUISITION_UNKNOWN -> "acquisition_unknown"; case ACQUISITION_TIMEOUT -> "timeout"; case RELEASE_UNKNOWN -> "release_unknown"; case LEASE_LOST -> "already_not_owned"; case UNSUPPORTED_CAPABILITY -> "unsupported_capability"; };
        return error instanceof IllegalArgumentException ? "invalid_argument" : "internal_error";
    }
    private static String boundedLine(java.io.BufferedReader reader) throws java.io.IOException {
        StringBuilder text = new StringBuilder(); int ch;
        while ((ch = reader.read()) != -1 && ch != '\n') { if (text.length() < MAX_LINE + 1) text.append((char) ch); }
        if (ch == -1 && text.isEmpty()) return null;
        if (text.length() > MAX_LINE || text.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_LINE) throw new java.io.IOException("worker input line exceeds 65536 bytes");
        int n = text.length(); if (n > 0 && text.charAt(n - 1) == '\r') text.setLength(n - 1); return text.toString();
    }
    private static ObjectNode envelope(String id, String status) { ObjectNode result = JSON.createObjectNode(); result.put("id", id); result.put("status", status); return result; }
    private static void write(Object lock, ObjectNode response) { synchronized (lock) { try { System.out.println(JSON.writeValueAsString(response)); System.out.flush(); } catch (Exception ignored) { System.err.println("worker: response encoding failed"); } } }
}
