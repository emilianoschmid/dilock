package org.dilock.etcd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.protobuf.ByteString;
import io.etcd.jetcd.api.KVGrpc;
import io.etcd.jetcd.api.LeaseGrpc;
import io.etcd.jetcd.api.RangeRequest;
import io.etcd.jetcd.api.LeaseGrantRequest;
import io.etcd.jetcd.api.LeaseRevokeRequest;
import io.etcd.jetcd.api.LeaseKeepAliveRequest;
import io.etcd.jetcd.api.LeaseKeepAliveResponse;
import io.etcd.jetcd.api.lock.LockGrpc;
import io.etcd.jetcd.api.lock.LockRequest;
import io.etcd.jetcd.api.lock.UnlockRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.dilock.redis.DilockException;
import org.dilock.redis.Identity;
import org.dilock.redis.AcquireOptions;
import org.dilock.redis.CleanupObservation;
import org.dilock.redis.CleanupObserver;

import java.net.URI;
import java.util.Map;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns one etcd gRPC channel and one scheduled keeper for queued attempt leases.
 * Safe for concurrent use. Closing it cancels pending calls, attempts bounded Unlock/Revoke cleanup,
 * then closes its channel; the server lease TTL bounds any unconfirmed cleanup. No coordinator
 * failover or exactly-once effect guarantee is provided.
 */
public final class EtcdProvider implements AutoCloseable {
    enum ReleaseOwnership { OWNED, NOT_OWNED, MISMATCH, UNKNOWN }
    enum RevokeObservation { ACKNOWLEDGED, NOT_FOUND, UNKNOWN }
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROFILE = "etcd-native-lock-v1";
    private static final String SCHEME = "sha256-framed-utf8-v1";
    private static final long CHANNEL_STARTUP_TIMEOUT_MILLIS = 5_000;
    private final ManagedChannel channel;
    private final String namespace;
    private final String epoch;
    private final long rpcMillis;
    private final long cleanupMillis;
    private final AcquireOptions defaultAcquisition;
    private final CleanupObserver cleanupObserver;
    private final KVGrpc.KVFutureStub kv;
    private final LeaseGrpc.LeaseFutureStub lease;
    private final LockGrpc.LockFutureStub lock;
    private final LeaseGrpc.LeaseStub leaseStreaming;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> { Thread t = new Thread(r, "dilock-etcd-keepalive"); t.setDaemon(true); return t; });
    private final Map<Long, EtcdLease> leases = new ConcurrentHashMap<>();
    private final Set<com.google.common.util.concurrent.ListenableFuture<?>> pendingLocks = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    private EtcdProvider(String target, String namespace, String epoch, long rpcMillis, long cleanupMillis,
                         AcquireOptions defaultAcquisition, CleanupObserver cleanupObserver) {
        this.namespace = namespace; this.epoch = epoch; this.rpcMillis = rpcMillis; this.cleanupMillis = cleanupMillis;
        this.defaultAcquisition = defaultAcquisition; this.cleanupObserver = cleanupObserver;
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        this.kv = KVGrpc.newFutureStub(channel);
        this.lease = LeaseGrpc.newFutureStub(channel);
        this.lock = LockGrpc.newFutureStub(channel);
        this.leaseStreaming = LeaseGrpc.newStub(channel);
    }

    /** Connects to one insecure etcd v3 endpoint and validates pre-provisioned metadata. */
    /**
     * Connects to one etcd authority and validates pre-provisioned draft 1 metadata. The provider
     * waits up to five seconds for the gRPC channel to become ready, then uses the configured
     * RPC bound to validate metadata. It does not provision metadata.
     *
     * @param endpoint etcd HTTP endpoint, with optional scheme and port
     * @param namespace pre-provisioned namespace identifier
     * @param epoch configured coordinator incarnation recorded in metadata
     * @return an open provider
     * @throws IllegalArgumentException if endpoint, namespace, or epoch is invalid
     * @throws DilockException if metadata is incompatible or etcd is unavailable
     */
    public static EtcdProvider connect(String endpoint, String namespace, String epoch) {
        return connect(new EtcdProviderOptions(endpoint, namespace, epoch));
    }

    /** Connects with namespace {@code default} and the standard finite acquisition policy.
     * @param endpoint etcd authority
     * @param epoch operator-approved epoch already recorded in namespace metadata
     * @return open provider owning one channel
     * @throws DilockException if metadata is invalid or backend is unavailable
     */
    public static EtcdProvider connect(String endpoint, String epoch) {
        return connect(new EtcdProviderOptions(endpoint, "default", epoch));
    }

    /** Connects using a validated immutable configuration and validates existing metadata.
     * @param options full provider settings
     * @return open provider owning one channel
     * @throws IllegalArgumentException for invalid local configuration
     * @throws DilockException if metadata is invalid or backend is unavailable
     */
    public static EtcdProvider connect(EtcdProviderOptions options) {
        String endpoint = options.endpoint();
        String namespace = options.namespace();
        String epoch = options.epoch();
        Identity.resourceId(namespace, "validation");
        if (epoch == null || epoch.isEmpty() || epoch.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) throw new IllegalArgumentException("epoch must be non-empty and at most 1024 UTF-8 bytes");
        URI uri;
        try { uri = URI.create(endpoint.contains("://") ? endpoint : "http://" + endpoint); }
        catch (RuntimeException error) { throw new IllegalArgumentException("invalid etcd endpoint", error); }
        if (!"http".equals(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) throw new IllegalArgumentException("endpoint must be an http host and port without credentials or path");
        int port = uri.getPort() < 0 ? 2379 : uri.getPort();
        EtcdProvider provider = new EtcdProvider(uri.getHost() + ":" + port, namespace, epoch, options.rpcTimeoutMillis(), options.cleanupTimeoutMillis(), options.defaultAcquisition(), options.cleanupObserver());
        try { provider.validateMetadata(); return provider; }
        catch (RuntimeException error) { try { provider.close(); } catch (RuntimeException ignored) { } throw error; }
    }

    /**
     * Returns a reusable immutable resource handle safe for concurrent calls.
     *
     * @param resource non-empty UTF-8 resource name
     * @return a handle bound to this provider and resource
     * @throws IllegalStateException if this provider is closed
     * @throws IllegalArgumentException if the resource identity is invalid
     */
    public EtcdLock lock(String resource) { ensureOpen(); Identity.resourceId(namespace, resource); return new EtcdLock(this, resource); }

    AcquireOptions defaultAcquisition() { return defaultAcquisition; }
    void observeCleanup(String outcome, String reason) {
        if (cleanupObserver == null) {
            System.getLogger("org.dilock.cleanup").log(System.Logger.Level.WARNING,
                    "Dilock etcd cleanup outcome is {0} ({1})", outcome, reason);
            return;
        }
        try { cleanupObserver.onCleanup(new CleanupObservation("etcd", outcome, reason)); }
        catch (RuntimeException ignored) { /* Diagnostics cannot abort lease cleanup. */ }
    }

    private void validateMetadata() {
        String path = "/dilock/namespaces/" + namespace + "/meta";
        awaitChannelReady();
        final io.etcd.jetcd.api.RangeResponse response;
        try { response = await(kv.withDeadlineAfter(rpcMillis, TimeUnit.MILLISECONDS).range(RangeRequest.newBuilder().setKey(bytes(path)).build()), rpcMillis); }
        catch (Exception error) { throw failure(DilockException.Kind.BACKEND_UNAVAILABLE, "failed reading etcd namespace metadata", error); }
        if (response.getKvsCount() == 0) throw failure(DilockException.Kind.PROTOCOL_MISMATCH, "namespace metadata is missing; provision it explicitly before use", null);
        byte[] raw = response.getKvs(0).getValue().toByteArray();
        validateMetadataValue(raw, epoch);
    }

    /** Waits for the cold gRPC channel handshake separately from any bounded metadata RPC. */
    private void awaitChannelReady() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CHANNEL_STARTUP_TIMEOUT_MILLIS);
        io.grpc.ConnectivityState state = channel.getState(true);
        while (state != io.grpc.ConnectivityState.READY) {
            if (state == io.grpc.ConnectivityState.SHUTDOWN) {
                throw failure(DilockException.Kind.BACKEND_UNAVAILABLE,
                        "etcd gRPC channel shut down before becoming ready", null);
            }
            long remainingNanos = remainingChannelStartupNanos(deadline, System.nanoTime());
            if (remainingNanos <= 0) {
                throw failure(DilockException.Kind.BACKEND_UNAVAILABLE,
                        "timed out waiting for etcd gRPC channel readiness",
                        new TimeoutException("etcd gRPC channel did not become ready within "
                                + CHANNEL_STARTUP_TIMEOUT_MILLIS + " ms"));
            }
            CountDownLatch stateChanged = new CountDownLatch(1);
            channel.notifyWhenStateChanged(state, stateChanged::countDown);
            try {
                if (!stateChanged.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                    throw failure(DilockException.Kind.BACKEND_UNAVAILABLE,
                            "timed out waiting for etcd gRPC channel readiness",
                            new TimeoutException("etcd gRPC channel did not become ready within "
                                    + CHANNEL_STARTUP_TIMEOUT_MILLIS + " ms"));
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw failure(DilockException.Kind.BACKEND_UNAVAILABLE,
                        "interrupted while waiting for etcd gRPC channel readiness", error);
            }
            state = channel.getState(true);
        }
    }

    static long remainingChannelStartupNanos(long deadlineNanos, long nowNanos) {
        return Math.max(0, deadlineNanos - nowNanos);
    }

    static void validateMetadataValue(byte[] raw, String epoch) {
        if (raw.length > 8192) throw failure(DilockException.Kind.PROTOCOL_MISMATCH, "namespace metadata exceeds 8192 bytes", null);
        try {
            if (!hasUniqueTopLevelProperties(raw)) throw failure(DilockException.Kind.PROTOCOL_MISMATCH, "etcd namespace metadata contains duplicate top-level properties", null);
            JsonNode value = JSON.readTree(raw);
            if (value == null || !value.isObject() || !value.path("protocol_major").isIntegralNumber()
                    || !value.path("protocol_major").canConvertToInt()
                    || value.path("protocol_major").intValue() != 1
                    || !value.path("identity_scheme").isTextual() || !SCHEME.equals(value.path("identity_scheme").textValue())
                    || !value.path("profile").isTextual() || !PROFILE.equals(value.path("profile").textValue())
                    || !value.path("coordination_epoch").isTextual() || !epoch.equals(value.path("coordination_epoch").textValue())
                    || !value.has("fixed_lease_ms") || !value.get("fixed_lease_ms").isNull()) throw failure(DilockException.Kind.PROTOCOL_MISMATCH, "etcd namespace metadata is incompatible with draft 1", null);
        } catch (DilockException error) { throw error; }
        catch (Exception error) { throw failure(DilockException.Kind.PROTOCOL_MISMATCH, "etcd namespace metadata is invalid JSON", error); }
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

    EtcdLease acquire(String resource, long waitMillis, long leaseMillis) throws InterruptedException {
        return acquire(resource, waitMillis, leaseMillis, false);
    }

    EtcdLease acquire(String resource, long waitMillis, long leaseMillis, boolean autoRenew) throws InterruptedException {
        ensureOpen();
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("interrupted before acquisition");
        if (leaseMillis < 1 || leaseMillis > 86_400_000 || waitMillis < 1 || waitMillis > 86_400_000) throw new IllegalArgumentException("wait and lease durations must be within 1..86400000 ms");
        long margin = Math.max(10, (leaseMillis + 99) / 100);
        if (rpcMillis + margin >= leaseMillis) throw new IllegalArgumentException("RPC timeout plus safety margin must be less than lease duration");
        long acquisitionStarted = System.nanoTime();
        long waitDeadlineNanos = acquisitionStarted + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        final io.etcd.jetcd.api.LeaseGrantResponse grant;
        long grantBudget = Math.min(rpcMillis, remainingWaitMillis(waitDeadlineNanos, System.nanoTime()));
        if (grantBudget <= 0) throw failure(DilockException.Kind.ACQUISITION_TIMEOUT, "acquisition wait budget expired before etcd lease grant", null);
        try { grant = await(lease.withDeadlineAfter(grantBudget, TimeUnit.MILLISECONDS).leaseGrant(LeaseGrantRequest.newBuilder().setTTL(Math.max(1, (leaseMillis + 999) / 1000)).build()), grantBudget); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new EtcdAcquisitionCancelledException(true, "interrupted during lease grant; remote outcome is unknown", error); }
        catch (Exception error) { throw failure(DilockException.Kind.ACQUISITION_UNKNOWN, "etcd did not confirm the attempt lease grant", error); }
        long id = grant.getID(); long ttlMillis = grant.getTTL() * 1000;
        if (id == 0 || ttlMillis <= 0) throw failure(DilockException.Kind.ACQUISITION_UNKNOWN, "etcd returned an invalid attempt lease", null);
        EtcdLease attempt = new EtcdLease(this, id, ttlMillis, acquisitionStarted + TimeUnit.MILLISECONDS.toNanos(ttlMillis - Math.max(10, ttlMillis / 100)));
        leases.put(id, attempt); attempt.startKeepAlive();
        long lockBudget = remainingWaitMillis(waitDeadlineNanos, System.nanoTime());
        if (lockBudget <= 0) {
            boolean revoked = attempt.revokeBounded();
            throw failure(revoked ? DilockException.Kind.ACQUISITION_TIMEOUT : DilockException.Kind.ACQUISITION_UNKNOWN,
                    revoked ? "acquisition wait budget expired after etcd lease grant; revoke was confirmed"
                            : "acquisition wait budget expired after etcd lease grant and revoke was not confirmed", null);
        }
        final io.etcd.jetcd.api.lock.LockResponse locked;
        var lockCall = lock.withDeadlineAfter(lockBudget, TimeUnit.MILLISECONDS).lock(LockRequest.newBuilder().setName(bytes("/dilock/locks/" + Identity.resourceId(namespace, resource))).setLease(id).build());
        pendingLocks.add(lockCall);
        try { locked = await(lockCall, lockBudget); }
        catch (InterruptedException error) {
            boolean revoked = attempt.revokeBounded(); Thread.currentThread().interrupt();
            throw new EtcdAcquisitionCancelledException(!revoked, revoked ? "interrupted while waiting; attempt lease revoke was confirmed" : "interrupted while waiting; attempt lease cleanup is unknown", error);
        }
        catch (Exception error) {
            boolean revoked = attempt.revokeBounded();
            if (waitMillis > 0 && isDeadline(error) && revoked) throw failure(DilockException.Kind.ACQUISITION_TIMEOUT, "timed out waiting for etcd native Lock", error);
            throw failure(revoked ? DilockException.Kind.BACKEND_UNAVAILABLE : DilockException.Kind.ACQUISITION_UNKNOWN, "etcd native Lock failed; attempt lease revocation " + (revoked ? "confirmed" : "unknown"), error);
        }
        finally { pendingLocks.remove(lockCall); }
        ByteString key = locked.getKey();
        if (key.isEmpty()) { attempt.revokeBounded(); throw failure(DilockException.Kind.ACQUISITION_UNKNOWN, "etcd native Lock returned an empty opaque key", null); }
        if (waitBudgetExpired(waitDeadlineNanos, System.nanoTime())) {
            boolean cleaned = attempt.unlockAndRevoke(key);
            throw failure(cleaned ? DilockException.Kind.ACQUISITION_TIMEOUT : DilockException.Kind.ACQUISITION_UNKNOWN,
                    cleaned ? "etcd native Lock completed after the acquisition wait budget; Unlock and Revoke were confirmed"
                            : "etcd native Lock completed after the acquisition wait budget and cleanup was not confirmed", null);
        }
        final io.etcd.jetcd.api.RangeResponse range;
        long verifyBudget = Math.min(rpcMillis, remainingWaitMillis(waitDeadlineNanos, System.nanoTime()));
        if (verifyBudget <= 0) {
            boolean cleaned = attempt.unlockAndRevoke(key);
            throw failure(cleaned ? DilockException.Kind.ACQUISITION_TIMEOUT : DilockException.Kind.ACQUISITION_UNKNOWN,
                    cleaned ? "acquisition wait budget expired before ownership verification; cleanup was confirmed"
                            : "acquisition wait budget expired before ownership verification and cleanup was not confirmed", null);
        }
        try {
            range = await(kv.withDeadlineAfter(verifyBudget, TimeUnit.MILLISECONDS).range(RangeRequest.newBuilder().setKey(key).build()), verifyBudget);
            if (range.getKvsCount() != 1 || !range.getKvs(0).getKey().equals(key) || range.getKvs(0).getLease() != id || range.getKvs(0).getCreateRevision() <= 0 || !attempt.isValid()) throw failure(DilockException.Kind.ACQUISITION_UNKNOWN, "linearizable Range did not verify the exact native key, lease and create_revision", null);
        } catch (InterruptedException error) {
            boolean cleaned = attempt.unlockAndRevoke(key); Thread.currentThread().interrupt();
            throw new EtcdAcquisitionCancelledException(!cleaned, cleaned ? "interrupted during verification; Unlock and Revoke were confirmed" : "interrupted during verification; cleanup acknowledgement is unknown", error);
        }
        catch (Exception error) { attempt.unlockAndRevoke(key); throw failure(DilockException.Kind.ACQUISITION_UNKNOWN, "could not verify exact native lock key; cleanup was attempted", error); }
        if (waitBudgetExpired(waitDeadlineNanos, System.nanoTime())) {
            boolean cleaned = attempt.unlockAndRevoke(key);
            throw failure(cleaned ? DilockException.Kind.ACQUISITION_TIMEOUT : DilockException.Kind.ACQUISITION_UNKNOWN,
                    cleaned ? "etcd lock verification completed after the acquisition wait budget; Unlock and Revoke were confirmed"
                            : "etcd lock verification completed after the acquisition wait budget and cleanup was not confirmed", null);
        }
        attempt.activate(key, range.getKvs(0).getCreateRevision());
        // Keepalive protects queued attempts by default and remains active while held only
        // when the caller explicitly opts into auto-renewal.
        if (!autoRenew) attempt.stopKeepAlive();
        return attempt;
    }

    boolean keepAlive(EtcdLease value, long sentAtNanos) {
        ConcurrentLinkedQueue<Long> sentTimes = new ConcurrentLinkedQueue<>();
        try {
            value.openKeepAlive(leaseStreaming.leaseKeepAlive(new StreamObserver<>() {
                @Override public void onNext(LeaseKeepAliveResponse response) {
                    if (value.isKeepAliveStopping()) return;
                    if (response.getTTL() <= 0) { value.markLostFromKeepAlive(); return; }
                    Long requestSent = sentTimes.poll();
                    if (requestSent != null) value.confirmDeadline(requestSent + TimeUnit.SECONDS.toNanos(response.getTTL()) - TimeUnit.MILLISECONDS.toNanos(Math.max(10, response.getTTL() * 10L)));
                }
                @Override public void onError(Throwable error) { value.markLostFromKeepAlive(); }
                @Override public void onCompleted() { value.markLostFromKeepAlive(); }
            }), sentTimes);
            scheduleKeepAlive(value, sentTimes); return true;
        } catch (RuntimeException error) { value.markLost(); return false; }
    }
    private void scheduleKeepAlive(EtcdLease value, ConcurrentLinkedQueue<Long> sentTimes) {
        long interval = Math.max(200, value.effectiveLeaseMillis() / 3);
        value.setTask(scheduler.scheduleAtFixedRate(() -> {
            if (!value.isValid()) { value.markLost(); return; }
            value.sendKeepAlive(sentTimes, id -> LeaseKeepAliveRequest.newBuilder().setID(id).build());
        }, interval, interval, TimeUnit.MILLISECONDS));
    }
    boolean revoke(long id) {
        return revoke(id, cleanupDeadline());
    }
    private boolean revoke(long id, long deadlineNanos) {
        return revokeObservation(id, deadlineNanos) == RevokeObservation.ACKNOWLEDGED;
    }
    private RevokeObservation revokeObservation(long id, long deadlineNanos) {
        long budget = remainingCleanupMillis(deadlineNanos, System.nanoTime());
        if (budget < 1) return RevokeObservation.UNKNOWN;
        try {
            awaitCleanup(lease.withDeadlineAfter(budget, TimeUnit.MILLISECONDS)
                    .leaseRevoke(LeaseRevokeRequest.newBuilder().setID(id).build()), budget);
            return RevokeObservation.ACKNOWLEDGED;
        } catch (Exception error) {
            return classifyRevokeError(error);
        }
    }
    boolean unlockAndRevoke(EtcdLease value, ByteString key) {
        long deadlineNanos = cleanupDeadline();
        value.stopKeepAlive(); boolean unlocked;
        long unlockBudget = remainingCleanupMillis(deadlineNanos, System.nanoTime());
        try {
            if (unlockBudget < 1) throw new TimeoutException("cleanup budget expired before Unlock");
            awaitCleanup(lock.withDeadlineAfter(unlockBudget, TimeUnit.MILLISECONDS).unlock(UnlockRequest.newBuilder().setKey(key).build()), unlockBudget);
            unlocked = true;
        }
        catch (Exception ignored) { unlocked = false; }
        boolean revoked = revoke(value.id(), deadlineNanos); leases.remove(value.id()); value.markLost(); return unlocked && revoked;
    }
    EtcdLease.ReleaseOutcome release(EtcdLease value, ByteString key) {
        long deadlineNanos = cleanupDeadline();
        value.stopKeepAlive();
        ReleaseOwnership ownership = observeOwnership(key, value.id(), deadlineNanos);
        Exception unlockFailure = null;
        boolean unlocked = false;
        if (ownership == ReleaseOwnership.OWNED) {
            long unlockBudget = remainingCleanupMillis(deadlineNanos, System.nanoTime());
            try {
                if (unlockBudget < 1) throw new TimeoutException("cleanup budget expired before Unlock");
                awaitCleanup(lock.withDeadlineAfter(unlockBudget, TimeUnit.MILLISECONDS)
                        .unlock(UnlockRequest.newBuilder().setKey(key).build()), unlockBudget);
                unlocked = true;
            } catch (Exception error) { unlockFailure = error; }
        }
        RevokeObservation revocation = revokeObservation(value.id(), deadlineNanos);
        leases.remove(value.id());
        boolean keyAbsent = false;
        boolean outcomeKnownWithoutPostcheck = ownership == ReleaseOwnership.NOT_OWNED
                || (ownership == ReleaseOwnership.MISMATCH
                    && revocation != RevokeObservation.UNKNOWN)
                || (ownership == ReleaseOwnership.OWNED
                    && revocation == RevokeObservation.ACKNOWLEDGED);
        if (!outcomeKnownWithoutPostcheck) {
            // Unlock can acknowledge a no-op if the lease expires between the
            // ownership Range and Unlock. A postcheck is therefore needed when
            // the strict acknowledged-release matrix did not decide the result.
            keyAbsent = keyIsAbsent(key, deadlineNanos);
        }
        return classifyReleaseOutcome(ownership, unlocked, revocation, keyAbsent, unlockFailure);
    }
    private ReleaseOwnership observeOwnership(ByteString key, long leaseId, long deadlineNanos) {
        long budget = remainingCleanupMillis(deadlineNanos, System.nanoTime());
        if (budget < 1) return ReleaseOwnership.UNKNOWN;
        try {
            var response = awaitCleanup(kv.withDeadlineAfter(budget, TimeUnit.MILLISECONDS)
                    .range(RangeRequest.newBuilder().setKey(key).build()), budget);
            if (response.getKvsCount() == 0) return classifyReleaseOwnership(false, false, 0, leaseId);
            if (response.getKvsCount() != 1) return classifyReleaseOwnership(true, false, 0, leaseId);
            var kv = response.getKvs(0);
            return classifyReleaseOwnership(true, kv.getKey().equals(key), kv.getLease(), leaseId);
        } catch (Exception ignored) {
            return ReleaseOwnership.UNKNOWN;
        }
    }
    static ReleaseOwnership classifyReleaseOwnership(boolean keyPresent, boolean exactKey, long observedLeaseId, long expectedLeaseId) {
        if (!keyPresent) return ReleaseOwnership.NOT_OWNED;
        if (!exactKey || observedLeaseId != expectedLeaseId) return ReleaseOwnership.MISMATCH;
        return ReleaseOwnership.OWNED;
    }
    static EtcdLease.ReleaseOutcome classifyReleaseOutcome(ReleaseOwnership ownership,
            boolean unlockAcknowledged, RevokeObservation revocation, boolean exactKeyAbsent,
            Exception cause) {
        if (ownership == ReleaseOwnership.NOT_OWNED || exactKeyAbsent) {
            return EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED;
        }
        if (ownership == ReleaseOwnership.OWNED && unlockAcknowledged
                && revocation == RevokeObservation.ACKNOWLEDGED) {
            return EtcdLease.ReleaseOutcome.RELEASED;
        }
        if (ownership == ReleaseOwnership.MISMATCH
                && revocation != RevokeObservation.UNKNOWN) {
            return EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED;
        }
        // A Revoke acknowledgement for an exactly observed owner proves cleanup,
        // but without an Unlock acknowledgement it is not a RELEASED result.
        if (ownership == ReleaseOwnership.OWNED
                && revocation == RevokeObservation.ACKNOWLEDGED) {
            return EtcdLease.ReleaseOutcome.ALREADY_NOT_OWNED;
        }
        throw failure(DilockException.Kind.RELEASE_UNKNOWN,
                ownership == ReleaseOwnership.OWNED
                        ? "etcd did not confirm exact Unlock and lease revoke acknowledgements, and the exact lock key could not be confirmed absent"
                        : "etcd could not confirm ownership of the exact native key or its absence; Unlock was not sent",
                cause);
    }
    private boolean keyIsAbsent(ByteString key, long deadlineNanos) {
        long budget = remainingCleanupMillis(deadlineNanos, System.nanoTime());
        if (budget < 1) return false;
        try {
            var response = awaitCleanup(kv.withDeadlineAfter(budget, TimeUnit.MILLISECONDS)
                    .range(RangeRequest.newBuilder().setKey(key).build()), budget);
            return response.getKvsCount() == 0;
        } catch (Exception ignored) {
            return false;
        }
    }
    private long cleanupDeadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cleanupMillis);
    }
    static long remainingCleanupMillis(long deadlineNanos, long nowNanos) {
        long remainingNanos = deadlineNanos - nowNanos;
        return remainingNanos <= 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }
    static boolean waitBudgetExpired(long deadlineNanos, long nowNanos) { return deadlineNanos - nowNanos <= 0; }
    static long remainingWaitMillis(long deadlineNanos, long nowNanos) {
        long remaining = deadlineNanos - nowNanos;
        return remaining <= 0 ? 0 : Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }
    long deadlineAfterGrant(long millis) { return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis - Math.max(10, millis / 100)); }
    String epoch() { return epoch; }
    void remove(long id) { leases.remove(id); }
    private void ensureOpen() { if (closed.get()) throw new IllegalStateException("provider is closed"); }
    private static ByteString bytes(String text) { return ByteString.copyFromUtf8(text); }
    static <T> T await(com.google.common.util.concurrent.ListenableFuture<T> future, long timeoutMillis) throws Exception {
        try { return future.get(Math.max(1, timeoutMillis), TimeUnit.MILLISECONDS); }
        catch (InterruptedException error) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw error;
        }
        catch (TimeoutException error) { future.cancel(true); throw error; }
    }
    /** Waits for one bounded cleanup RPC through interruptions and restores the interrupt flag. */
    static <T> T awaitCleanup(com.google.common.util.concurrent.ListenableFuture<T> future, long timeoutMillis) throws Exception {
        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1, timeoutMillis));
        long deadline = System.nanoTime() + budgetNanos;
        boolean restoreInterrupt = Thread.interrupted();
        try {
            for (;;) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    future.cancel(true);
                    throw new TimeoutException("cleanup RPC exceeded its bounded wait");
                }
                try { return future.get(remaining, TimeUnit.NANOSECONDS); }
                catch (InterruptedException ignored) {
                    // Keep waiting within the same cleanup deadline; interruption
                    // must not cancel an independent owner-cleanup RPC early.
                    restoreInterrupt = true;
                }
                catch (TimeoutException error) {
                    future.cancel(true);
                    throw error;
                }
            }
        }
        finally {
            if (Thread.interrupted() || restoreInterrupt) Thread.currentThread().interrupt();
        }
    }
    private static DilockException failure(DilockException.Kind kind, String message, Throwable cause) { return cause == null ? new DilockException(kind, message) : new DilockException(kind, message, cause); }
    private static boolean isDeadline(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof TimeoutException) return true;
            if (current instanceof io.grpc.StatusException status && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
            if (current instanceof io.grpc.StatusRuntimeException status && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) return true;
        }
        return false;
    }
    static RevokeObservation classifyRevokeError(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof io.grpc.StatusException status) {
                if (status.getStatus().getCode() == Status.Code.NOT_FOUND) return RevokeObservation.NOT_FOUND;
            }
            if (current instanceof io.grpc.StatusRuntimeException status) {
                if (status.getStatus().getCode() == Status.Code.NOT_FOUND) return RevokeObservation.NOT_FOUND;
            }
        }
        return RevokeObservation.UNKNOWN;
    }
    /**
     * Cancels pending locks, attempts bounded cleanup of tracked leases, and closes the channel.
     * Cleanup uncertainty is bounded by etcd lease expiry. Idempotent; interruption during channel
     * termination is restored on the calling thread.
     */
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (var call : pendingLocks) call.cancel(true);
        for (EtcdLease value : leases.values()) {
            try {
                if (value.state() == EtcdLease.State.HELD) value.release();
                else if (!value.revokeBounded()) observeCleanup("unknown", "provider-close-unlock-unavailable");
            } catch (RuntimeException error) { observeCleanup("unknown", "provider-close"); /* Server TTL bounds cleanup uncertainty. */ }
        }
        scheduler.shutdownNow(); channel.shutdown();
        try { channel.awaitTermination(cleanupMillis, TimeUnit.MILLISECONDS); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
    }
}
