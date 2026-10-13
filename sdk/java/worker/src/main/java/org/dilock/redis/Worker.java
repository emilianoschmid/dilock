package org.dilock.redis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** JSONL worker used by the shared conformance harness; not a production coordinator. */
public final class Worker {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_LINE_BYTES = 64 * 1024;
    private Worker() { }

    /**
     * Runs the line-oriented shared conformance worker. This entry point is for tests and examples,
     * not a production coordinator; it reads one config path and serves JSONL on standard streams.
     *
     * @param args exactly one path to the worker configuration JSON
     * @throws Exception if setup or stream processing fails
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: Worker <config.json>");
        JsonNode config = JSON.readTree(Files.readString(Path.of(args[0])));
        if ("etcd".equals(config.path("backend").asText())) { EtcdWorker.main(args); return; }
        if (!"redis".equals(config.path("backend").asText())) throw new IllegalArgumentException("backend must be redis");
        RedisProvider provider = RedisProvider.connect(config.path("endpoint").asText(), config.path("namespace").asText(), config.path("epoch").asText());
        Map<String, RedisLease> leases = new ConcurrentHashMap<>();
        Map<String, String> released = new ConcurrentHashMap<>();
        Map<String, ScheduledFuture<?>> renewals = new ConcurrentHashMap<>();
        ScheduledExecutorService renewalScheduler = Executors.newScheduledThreadPool(2, r -> { Thread thread = new Thread(r, "dilock-redis-auto-renew"); thread.setDaemon(true); return thread; });
        Map<String, Future<?>> pending = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newCachedThreadPool(r -> { Thread thread = new Thread(r, "dilock-worker-request"); thread.setDaemon(true); return thread; });
        Object outputLock = new Object();
        String line;
        try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8))) {
            var active = ConcurrentHashMap.<String>newKeySet();
            while (true) {
                try { line = readBoundedLine(reader); }
                catch (LineTooLongException error) { System.err.println("worker: input line exceeds 65536 bytes"); break; }
                if (line == null) break;
                final JsonNode request;
                try { request = JSON.readTree(line); }
                catch (Exception error) { System.err.println("worker: malformed JSON input"); continue; }
                String id = request.path("id").asText();
                String op = request.path("op").asText();
                if (id.isEmpty() || op.isEmpty()) { System.err.println("worker: malformed request envelope"); continue; }
                if (!active.add(id)) { write(outputLock, envelope(id, "invalid_argument")); continue; }
                if ("cancel_request".equals(op)) {
                    String target = request.path("target_id").asText();
                    Future<?> future = pending.get(target);
                    if (future != null) future.cancel(true);
                    ObjectNode response = envelope(id, "ok"); response.put("cancelled", future != null); write(outputLock, response); active.remove(id); continue;
                }
                if ("shutdown".equals(op)) {
                    ObjectNode response = envelope(id, "ok"); response.put("outstanding_cleanup_attempts", 0); write(outputLock, response); active.remove(id); break;
                }
                var future = new java.util.concurrent.FutureTask<Void>(() -> {
                    try { process(provider, leases, released, renewals, renewalScheduler, request, outputLock); }
                    catch (Throwable error) { write(outputLock, envelope(id, mapError(error))); }
                    finally { pending.remove(id); active.remove(id); }
                    return null;
                });
                pending.put(id, future);
                executor.execute(future);
                if ("shutdown".equals(op)) break;
            }
        } finally {
            for (Future<?> future : pending.values()) future.cancel(true);
            executor.shutdownNow();
            for (ScheduledFuture<?> renewal : renewals.values()) renewal.cancel(false);
            renewalScheduler.shutdownNow();
            try { renewalScheduler.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            try { executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            provider.close();
        }
    }

    private static void process(RedisProvider provider, Map<String, RedisLease> leases, Map<String, String> released,
                                Map<String, ScheduledFuture<?>> renewals, ScheduledExecutorService renewalScheduler,
                                JsonNode request, Object outputLock) {
        String id = request.path("id").asText();
        String op = request.path("op").asText();
        ObjectNode response;
        switch (op) {
            case "hello" -> { response = envelope(id, "ok"); response.put("sdk_version", "0.1.0"); response.put("protocol", 1); response.put("backend", "redis"); response.set("capabilities", JSON.createObjectNode().put("renewal", true).put("auto_renew", true).put("fencing", false)); }
            case "try_acquire", "acquire" -> {
                String resource = request.path("resource").asText(null);
                if (resource == null) throw new IllegalArgumentException("resource is required");
                RedisLock lock = provider.lock(resource);
                long ttl = request.path("lease_ms").asLong(30_000);
                try {
                    RedisLease lease = "try_acquire".equals(op)
                            ? lock.tryAcquire(ttl).orElse(null)
                            : lock.acquire(request.path("wait_ms").asLong(5000), ttl);
                    if (lease == null) { write(outputLock, envelope(id, "busy")); return; }
                    leases.put(lease.id(), lease);
                    if (request.path("auto_renew").asBoolean(false)) {
                        long interval = Math.max(100, ttl / 3);
                        AtomicReference<ScheduledFuture<?>> taskRef = new AtomicReference<>();
                        ScheduledFuture<?> task = renewalScheduler.scheduleWithFixedDelay(() -> {
                            try { lease.renew(ttl); }
                            catch (RuntimeException error) {
                                ScheduledFuture<?> current = taskRef.get();
                                if (current != null) {
                                    current.cancel(false);
                                    renewals.remove(lease.id(), current);
                                }
                            }
                        }, interval, interval, TimeUnit.MILLISECONDS);
                        taskRef.set(task);
                        renewals.put(lease.id(), task);
                    }
                    response = envelope(id, "acquired"); response.put("lease_id", lease.id()); response.put("effective_lease_ms", ttl);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt(); response = envelope(id, "cancelled");
                    response.put("remote_outcome_unknown", error instanceof RedisAcquisitionCancelledException cancelled
                            && cancelled.remoteOutcomeUnknown());
                }
            }
            case "release" -> {
                String leaseId = request.path("lease_id").asText();
                String prior = released.get(leaseId);
                if (prior != null) { response = envelope(id, prior); break; }
                RedisLease lease = leases.get(leaseId);
                if (lease == null) throw new IllegalArgumentException("unknown lease_id");
                ScheduledFuture<?> renewal = renewals.remove(leaseId);
                if (renewal != null) renewal.cancel(false);
                RedisLease.ReleaseOutcome outcome = lease.release();
                leases.remove(leaseId);
                String status = outcome == RedisLease.ReleaseOutcome.RELEASED ? "released" : "already_not_owned";
                released.put(leaseId, status);
                response = envelope(id, status);
            }
            case "inspect" -> {
                String leaseId = request.path("lease_id").asText();
                RedisLease lease = leases.get(leaseId);
                if (lease == null) throw new IllegalArgumentException("unknown lease_id");
                response = envelope(id, "ok"); response.put("state", lease.state().name().toLowerCase(java.util.Locale.ROOT));
            }
            case "shutdown" -> { response = envelope(id, "ok"); response.put("outstanding_cleanup_attempts", 0); }
            default -> throw new IllegalArgumentException("unknown op");
        }
        write(outputLock, response);
    }

    private static String mapError(Throwable error) {
        if (error instanceof DilockException dilock) return switch (dilock.kind()) {
            case BACKEND_UNAVAILABLE -> "backend_unavailable"; case PROTOCOL_MISMATCH -> "protocol_mismatch";
            case ACQUISITION_UNKNOWN -> "acquisition_unknown"; case ACQUISITION_TIMEOUT -> "timeout";
            case RELEASE_UNKNOWN -> "release_unknown"; case LEASE_LOST -> "already_not_owned"; case UNSUPPORTED_CAPABILITY -> "unsupported_capability";
        };
        return error instanceof IllegalArgumentException ? "invalid_argument" : "internal_error";
    }
    private static String readBoundedLine(java.io.BufferedReader reader) throws java.io.IOException, LineTooLongException {
        StringBuilder line = new StringBuilder();
        boolean oversized = false;
        int value;
        while ((value = reader.read()) != -1 && value != '\n') {
            if (!oversized) {
                line.append((char) value);
                if (line.length() > MAX_LINE_BYTES) oversized = true;
            }
        }
        if (value == -1 && line.isEmpty() && !oversized) return null;
        if (oversized || line.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_LINE_BYTES) throw new LineTooLongException();
        int length = line.length();
        if (length > 0 && line.charAt(length - 1) == '\r') line.setLength(length - 1);
        return line.toString();
    }
    private static final class LineTooLongException extends Exception { private static final long serialVersionUID = 1L; }
    private static ObjectNode envelope(String id, String status) { ObjectNode value = JSON.createObjectNode(); value.put("id", id); value.put("status", status); return value; }
    private static void write(Object lock, ObjectNode value) { synchronized (lock) { try { System.out.println(JSON.writeValueAsString(value)); System.out.flush(); } catch (Exception error) { System.err.println("worker: failed encoding response"); } } }
}
