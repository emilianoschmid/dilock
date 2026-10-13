using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dilock;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using V3Lockpb;

namespace Dilock.Etcd;

/// <summary>Immutable connection and default acquisition policy for one etcd authority.</summary>
public sealed record EtcdProviderOptions
{
    /// <summary>One HTTP/2 etcd authority; TLS trust remains enabled for https endpoints.</summary>
    public required string Endpoint { get; init; }
    /// <summary>Provisioned protocol namespace. Defaults to <c>default</c>.</summary>
    public string Namespace { get; init; } = "default";
    /// <summary>Deployment-approved epoch already recorded in namespace metadata.</summary>
    public required string ExpectedEpoch { get; init; }
    /// <summary>Budget for validating namespace metadata before returning the provider.</summary>
    public TimeSpan MetadataTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Optional factory returning a fresh handler owned by the provider's channel.</summary>
    public Func<HttpMessageHandler>? HttpHandlerFactory { get; init; }
    /// <summary>Policy used by handle.AcquireAsync() when no per-acquisition policy is supplied.</summary>
    public AcquireOptions DefaultAcquisition { get; init; } = new();
    /// <summary>Optional, non-throwing observer for Unknown outcomes from scope cleanup.</summary>
    public Action<CleanupObservation>? CleanupObserver { get; init; }
}

/// <summary>An etcd lease with its epoch-scoped create-revision fence.</summary>
public interface IEtcdLease : ILease
{
    /// <summary>Linearizable etcd <c>create_revision</c> of the exact native lock key returned for this acquisition.</summary>
    /// <remarks>This revision is useful only when the protected resource atomically validates the matching epoch and rejects stale revisions.</remarks>
    long CreateRevision { get; }
    /// <summary>Deployment-approved coordinator incarnation captured by this provider; protected resources must validate it with the revision.</summary>
    string Epoch { get; }
}

/// <summary>One etcd authority using native Lock/Unlock and Lease RPCs.</summary>
public sealed class EtcdProvider : ILockProvider
{
    internal readonly KV.KVClient Kv;
    internal readonly Lease.LeaseClient LeaseClient;
    internal readonly Lock.LockClient LockClient;
    private readonly GrpcChannel channel;
    private readonly string ns, epoch;
    internal readonly AcquireOptions DefaultAcquisition;
    private readonly Action<CleanupObservation>? cleanupObserver;
    private readonly ConcurrentDictionary<EtcdLease, byte> leases = new();
    private int closed;

    private EtcdProvider(GrpcChannel channel, EtcdProviderOptions options)
    {
        this.channel = channel;
        ns = options.Namespace;
        epoch = options.ExpectedEpoch;
        DefaultAcquisition = options.DefaultAcquisition with { Lease = options.DefaultAcquisition.Lease with { } };
        cleanupObserver = options.CleanupObserver;
        // Construct the generated native stubs directly. The channel has no retry policy:
        // a missing Lock/Lease acknowledgement must stay an explicit uncertain outcome.
        Kv = new KV.KVClient(channel);
        LeaseClient = new Lease.LeaseClient(channel);
        LockClient = new Lock.LockClient(channel);
    }

    /// <summary>
    /// Connect to exactly one HTTP/2 etcd endpoint. The provider owns its channel and active
    /// leases. This overload uses the default HTTP handler and trust configuration.
    /// </summary>
    /// <param name="endpoint">One etcd <c>http://</c> or <c>https://</c> authority with no user info, path, query, or fragment.</param>
    /// <param name="ns">Validated protocol namespace whose metadata must already exist.</param>
    /// <param name="epoch">Deployment-approved coordinator incarnation that must match namespace metadata.</param>
    /// <param name="cancellationToken">Cancels connection setup and the initial metadata check.</param>
    /// <returns>A provider owning the channel and active leases.</returns>
    /// <exception cref="ArgumentException">The endpoint, namespace, or epoch is invalid.</exception>
    /// <exception cref="OperationCanceledException">Setup or metadata validation was cancelled.</exception>
    /// <exception cref="DilockException">The authority is unavailable or namespace metadata is missing/incompatible.</exception>
    public static async Task<EtcdProvider> ConnectAsync(string endpoint, string ns, string epoch,
        CancellationToken cancellationToken = default)
        => await ConnectAsync(endpoint, ns, epoch, httpHandlerFactory: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Connect to exactly one HTTP/2 etcd endpoint, optionally supplying a fresh transport
    /// handler for TLS trust or client certificates. The provider owns and disposes the returned
    /// handler with its channel; do not share it with other channels. The channel is created
    /// without a gRPC retry service configuration.
    /// </summary>
    /// <param name="endpoint">One etcd <c>http://</c> or <c>https://</c> authority with no user info, path, query, or fragment.</param>
    /// <param name="ns">Validated protocol namespace whose metadata must already exist.</param>
    /// <param name="epoch">Deployment-approved coordinator incarnation that must match namespace metadata.</param>
    /// <param name="httpHandlerFactory">Optional factory returning a fresh handler owned by this provider; TLS validation must remain enabled.</param>
    /// <param name="cancellationToken">Cancels connection setup and the initial metadata check.</param>
    /// <returns>A provider owning the channel, handler, and active leases.</returns>
    /// <exception cref="ArgumentException">The endpoint, namespace, or epoch is invalid.</exception>
    /// <exception cref="OperationCanceledException">Setup or metadata validation was cancelled.</exception>
    /// <exception cref="DilockException">The authority is unavailable or namespace metadata is missing/incompatible.</exception>
    public static async Task<EtcdProvider> ConnectAsync(string endpoint, string ns, string epoch,
        Func<HttpMessageHandler>? httpHandlerFactory, CancellationToken cancellationToken = default)
        => await ConnectAsync(new EtcdProviderOptions
        {
            Endpoint = endpoint, Namespace = ns, ExpectedEpoch = epoch, HttpHandlerFactory = httpHandlerFactory
        }, cancellationToken).ConfigureAwait(false);

    /// <summary>Connects to the default namespace using an explicit deployment epoch and common defaults.</summary>
    public static Task<EtcdProvider> ConnectAsync(string endpoint, string expectedEpoch,
        CancellationToken cancellationToken = default)
        => ConnectAsync(new EtcdProviderOptions { Endpoint = endpoint, ExpectedEpoch = expectedEpoch }, cancellationToken);

    /// <summary>Connects using a complete immutable provider configuration and validates existing metadata.</summary>
    public static async Task<EtcdProvider> ConnectAsync(EtcdProviderOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ResourceIdentity.Hash(options.Namespace, "validation");
        if (string.IsNullOrWhiteSpace(options.ExpectedEpoch) || options.ExpectedEpoch.Length > 1024)
            throw new ArgumentException("Invalid coordination epoch", nameof(options));
        if (options.MetadataTimeout <= TimeSpan.Zero || options.MetadataTimeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options), "Metadata timeout must be positive and at most one day");
        ArgumentNullException.ThrowIfNull(options.DefaultAcquisition);
        options.DefaultAcquisition.Validate();
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("http" or "https") || uri.UserInfo.Length != 0
            || uri.Query.Length != 0 || uri.Fragment.Length != 0 || uri.AbsolutePath is not ("" or "/"))
            throw new ArgumentException("Endpoint must be one http:// or https:// authority without user info, path, query or fragment", nameof(options));
        cancellationToken.ThrowIfCancellationRequested();
        var channelOptions = new GrpcChannelOptions();
        if (options.HttpHandlerFactory is not null) channelOptions.HttpHandler = options.HttpHandlerFactory();
        var channel = GrpcChannel.ForAddress(uri, channelOptions);
        try
        {
            var provider = new EtcdProvider(channel, options);
            await provider.ValidateMetadataAsync(options.MetadataTimeout, cancellationToken).ConfigureAwait(false);
            return provider;
        }
        catch (OperationCanceledException) { channel.Dispose(); throw; }
        catch (DilockException) { channel.Dispose(); throw; }
        catch (Exception e) when (e is RpcException or HttpRequestException or IOException or TimeoutException)
        {
            channel.Dispose();
            throw new DilockException(FailureKind.BackendUnavailable, "etcd connection or metadata check failed", e);
        }
    }

    private async Task ValidateMetadataAsync(TimeSpan budget, CancellationToken ct)
    {
        var key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta");
        try
        {
            var response = await Kv.RangeAsync(new RangeRequest { Key = key },
                deadline: DateTime.UtcNow + budget, cancellationToken: ct).ConfigureAwait(false);
            if (response.Kvs.Count != 1 || !response.Kvs[0].Key.Equals(key)
                || response.Kvs[0].Lease != 0 || response.Kvs[0].Value.Length > 8192)
                throw new JsonException("Missing or invalid namespace metadata key");
            using var document = JsonDocument.Parse(response.Kvs[0].Value.ToByteArray(), new JsonDocumentOptions { MaxDepth = 16 });
            var root = document.RootElement;
            if (!HasUniqueTopLevelProperties(root)
                || root.GetProperty("protocol_major").GetInt32() != 1
                || root.GetProperty("identity_scheme").GetString() != "sha256-framed-utf8-v1"
                || root.GetProperty("profile").GetString() != "etcd-native-lock-v1"
                || root.GetProperty("coordination_epoch").GetString() != epoch
                || root.GetProperty("fixed_lease_ms").ValueKind != JsonValueKind.Null)
                throw new JsonException("Namespace metadata does not match the configured etcd profile");
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException or KeyNotFoundException or FormatException)
        { throw new DilockException(FailureKind.ProtocolMismatch, "Missing or incompatible etcd namespace metadata", e); }
        catch (RpcException e) { throw new DilockException(FailureKind.BackendUnavailable, "etcd namespace metadata read failed", e); }
    }

    internal static bool HasUniqueTopLevelProperties(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object) return false;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in value.EnumerateObject())
            if (!names.Add(property.Name)) return false;
        return true;
    }

    /// <inheritdoc/>
    public ILockHandle Lock(string resource)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
        var id = ResourceIdentity.Hash(ns, resource);
        return new EtcdHandle(this, Encoding.UTF8.GetBytes($"/dilock/locks/{id}"));
    }

    internal async Task<(long Id, int TtlSeconds, long Sent)> GrantAsync(int ttlSeconds, TimeSpan budget, CancellationToken ct)
    {
        try
        {
            var sent = Stopwatch.GetTimestamp();
            var result = await LeaseClient.LeaseGrantAsync(new LeaseGrantRequest { TTL = ttlSeconds },
                deadline: DateTime.UtcNow + budget, cancellationToken: ct).ConfigureAwait(false);
            if (result.ID == 0 || result.TTL <= 0) throw new DilockException(FailureKind.ProtocolMismatch, "etcd returned an invalid lease grant");
            return (result.ID, checked((int)result.TTL), sent);
        }
        catch (DilockException) { throw; }
        catch (OperationCanceledException e) { throw new DilockException(FailureKind.AcquisitionUnknown, "Lease grant was dispatched but not acknowledged", e); }
        catch (Exception e) when (e is RpcException or TimeoutException or HttpRequestException)
        { throw new DilockException(FailureKind.AcquisitionUnknown, "Lease grant was dispatched but not acknowledged", e); }
    }

    internal async Task<bool> KeepAliveAsync(long id, TimeSpan budget, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(budget);
        using var call = LeaseClient.LeaseKeepAlive(cancellationToken: timeout.Token);
        try
        {
            await call.RequestStream.WriteAsync(new LeaseKeepAliveRequest { ID = id }).ConfigureAwait(false);
            if (!await call.ResponseStream.MoveNext(timeout.Token).ConfigureAwait(false)) return false;
            return call.ResponseStream.Current.ID == id && call.ResponseStream.Current.TTL > 0;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return false; }
        catch (RpcException) { return false; }
    }

    internal enum RevokeObservation { Acknowledged, NotFound, Unknown }

    internal async Task<RevokeObservation> RevokeAsync(long id, TimeSpan budget, CancellationToken cancellationToken = default)
    {
        if (budget <= TimeSpan.Zero || cancellationToken.IsCancellationRequested) return RevokeObservation.Unknown;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(budget);
        try
        {
            await LeaseClient.LeaseRevokeAsync(new LeaseRevokeRequest { ID = id },
                deadline: DateTime.UtcNow + budget, cancellationToken: timeout.Token).ConfigureAwait(false);
            return RevokeObservation.Acknowledged;
        }
        catch (RpcException e) when (e.StatusCode == StatusCode.NotFound) { return RevokeObservation.NotFound; }
        catch (Exception e) when (e is RpcException or OperationCanceledException or TimeoutException or HttpRequestException) { return RevokeObservation.Unknown; }
    }

    internal static TimeSpan RemainingCleanup(TimeSpan budget, long started)
        => RemainingCleanup(budget, Stopwatch.GetElapsedTime(started));

    internal static TimeSpan RemainingCleanup(TimeSpan budget, TimeSpan elapsed)
    {
        var remaining = budget - elapsed;
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    internal enum OwnershipObservation { Owned, NotOwned, Unknown }

    internal static ReleaseOutcome ClassifyRelease(
        OwnershipObservation ownership, bool unlockAcknowledged, RevokeObservation revoke)
    {
        if (ownership == OwnershipObservation.Owned && unlockAcknowledged && revoke == RevokeObservation.Acknowledged)
            return ReleaseOutcome.Released;
        if (ownership != OwnershipObservation.Unknown && revoke is RevokeObservation.Acknowledged or RevokeObservation.NotFound)
            return ReleaseOutcome.AlreadyNotOwned;
        return ReleaseOutcome.Unknown;
    }

    internal async Task<OwnershipObservation> ObserveOwnershipAsync(ByteString exactKey, long leaseId,
        TimeSpan budget, CancellationToken cancellationToken)
    {
        if (exactKey.IsEmpty || budget <= TimeSpan.Zero) return OwnershipObservation.Unknown;
        try
        {
            var range = await Kv.RangeAsync(new RangeRequest { Key = exactKey },
                deadline: DateTime.UtcNow + budget, cancellationToken: cancellationToken).ConfigureAwait(false);
            return range.Kvs.Count == 1 && range.Kvs[0].Key.Equals(exactKey) && range.Kvs[0].Lease == leaseId
                ? OwnershipObservation.Owned
                : OwnershipObservation.NotOwned;
        }
        catch (Exception e) when (e is RpcException or OperationCanceledException or TimeoutException or HttpRequestException)
        {
            return OwnershipObservation.Unknown;
        }
    }

    internal void Track(EtcdLease lease) => leases.TryAdd(lease, 0);
    internal void Untrack(EtcdLease lease) => leases.TryRemove(lease, out _);
    internal void ObserveCleanup(ReleaseOutcome outcome, string reason)
    {
        if (outcome == ReleaseOutcome.Released || cleanupObserver is null) return;
        try { cleanupObserver(new CleanupObservation("etcd-native-lock-v1", outcome, reason)); }
        catch (Exception) { /* Diagnostics must not escape a scope-cleanup path. */ }
    }
    internal string Epoch => epoch;
    internal bool Closed => Volatile.Read(ref closed) != 0;

    /// <summary>Withdraws local reliance, attempts bounded cleanup for active leases, and disposes the channel.</summary>
    /// <remarks>Disposal is idempotent. It cannot stop stale external work or prove cleanup when etcd did not acknowledge it.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        foreach (var lease in leases.Keys) { lease.MarkLost(); await lease.DisposeFromProviderAsync().ConfigureAwait(false); }
        channel.Dispose();
    }
}

internal sealed class EtcdHandle(EtcdProvider provider, byte[] lockName) : ILockHandle
{
    public Task<ILease?> TryAcquireAsync(LeaseOptions? options = null, CancellationToken cancellationToken = default)
    {
        (options ?? provider.DefaultAcquisition.Lease).Validate();
        if (provider.Closed) throw new ObjectDisposedException(nameof(EtcdProvider));
        cancellationToken.ThrowIfCancellationRequested();
        throw new DilockException(FailureKind.UnsupportedCapability,
            "etcd's native Lock service has no non-blocking try-lock RPC; use AcquireAsync with a bounded wait");
    }

    public async Task<ILease> AcquireAsync(TimeSpan wait, LeaseOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (wait <= TimeSpan.Zero || wait > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(wait));
        var o = options ?? provider.DefaultAcquisition.Lease;
        o.Validate();
        ObjectDisposedException.ThrowIf(provider.Closed, provider);
        cancellationToken.ThrowIfCancellationRequested();
        var started = Stopwatch.GetTimestamp();
        var grantBudget = MinBudget(o.RpcTimeout, RemainingWait(wait, started));
        if (grantBudget <= TimeSpan.Zero)
            throw new DilockException(FailureKind.WaitTimeout, "etcd acquisition wait budget expired before lease grant");
        var ttlSeconds = checked((int)Math.Ceiling(o.LeaseDuration.TotalSeconds));
        var grant = await provider.GrantAsync(ttlSeconds, grantBudget, cancellationToken).ConfigureAwait(false);
        var leaseId = grant.Id;
        var lease = new EtcdLease(provider, leaseId, o, TimeSpan.FromSeconds(grant.TtlSeconds), grant.Sent, acquiring: true);
        provider.Track(lease);
        try
        {
            var lockBudget = RemainingWait(wait, started);
            if (lockBudget <= TimeSpan.Zero)
                throw new DilockException(FailureKind.WaitTimeout, "etcd acquisition wait budget expired after lease grant");
            var reply = await provider.LockClient.LockAsync(new LockRequest
            { Name = ByteString.CopyFrom(lockName), Lease = leaseId }, deadline: DateTime.UtcNow + lockBudget,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            if (reply.Key.IsEmpty) throw new DilockException(FailureKind.ProtocolMismatch, "etcd Lock returned an empty ownership key");
            var verifyBudget = MinBudget(o.RpcTimeout, RemainingWait(wait, started));
            if (verifyBudget <= TimeSpan.Zero)
                throw new DilockException(FailureKind.WaitTimeout, "etcd acquisition wait budget expired before ownership verification");
            var range = await provider.Kv.RangeAsync(new RangeRequest { Key = reply.Key },
                deadline: DateTime.UtcNow + verifyBudget, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (range.Kvs.Count != 1 || !range.Kvs[0].Key.Equals(reply.Key) || range.Kvs[0].Lease != leaseId || range.Kvs[0].CreateRevision <= 0)
                throw new DilockException(FailureKind.ProtocolMismatch, "Native lock key did not verify against its lease");
            if (lease.State != LeaseState.Held || cancellationToken.IsCancellationRequested)
                throw new DilockException(FailureKind.AcquisitionUnknown, "Lease validity ended before ownership verification completed");
            if (RemainingWait(wait, started) <= TimeSpan.Zero)
                throw new DilockException(FailureKind.WaitTimeout, "etcd Lock completed after the acquisition wait budget");
            lease.Activate(reply.Key, range.Kvs[0].CreateRevision);
            return lease;
        }
        catch (RpcException e) when (e.StatusCode == StatusCode.DeadlineExceeded && !cancellationToken.IsCancellationRequested)
        {
            if (await lease.AbandonAsync().ConfigureAwait(false))
            {
                if (RemainingWait(wait, started) <= TimeSpan.Zero)
                    throw new DilockException(FailureKind.WaitTimeout, "etcd acquisition wait budget expired", e);
                throw new DilockException(FailureKind.BackendUnavailable, "etcd acquisition RPC exceeded its deadline", e);
            }
            throw new DilockException(FailureKind.AcquisitionUnknown, "Timed-out etcd Lock may still have ownership because lease revocation was not confirmed", e);
        }
        catch (OperationCanceledException e)
        {
            var cleaned = await lease.AbandonAsync().ConfigureAwait(false);
            if (cleaned) throw new OperationCanceledException("etcd acquisition was cancelled and its lease was revoked", e, cancellationToken);
            throw new DilockException(FailureKind.AcquisitionUnknown, "Cancelled etcd acquisition could not be confirmed clean", e);
        }
        catch (RpcException e) when (e.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
        {
            var cleaned = await lease.AbandonAsync().ConfigureAwait(false);
            if (cleaned) throw new OperationCanceledException("etcd acquisition was cancelled and its lease was revoked", e, cancellationToken);
            throw new DilockException(FailureKind.AcquisitionUnknown, "Cancelled etcd acquisition could not be confirmed clean", e);
        }
        catch (Exception e) when (e is RpcException or TimeoutException or HttpRequestException)
        {
            var cleaned = await lease.AbandonAsync().ConfigureAwait(false);
            if (!cleaned) throw new DilockException(FailureKind.AcquisitionUnknown, "etcd acquisition failed and cleanup was not acknowledged", e);
            if (e is DilockException de) throw de;
            throw new DilockException(FailureKind.BackendUnavailable, "etcd acquisition failed", e);
        }
        catch (DilockException e)
        {
            if (!await lease.AbandonAsync().ConfigureAwait(false))
                throw new DilockException(FailureKind.AcquisitionUnknown, "etcd acquisition could not be verified and cleanup was not acknowledged", e);
            throw;
        }
        catch
        {
            await lease.AbandonAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal static TimeSpan RemainingWait(TimeSpan wait, long started)
        => RemainingWaitAt(wait, Stopwatch.GetElapsedTime(started));

    internal static TimeSpan RemainingWaitAt(TimeSpan wait, TimeSpan elapsed)
    {
        var remaining = wait - elapsed;
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private static TimeSpan MinBudget(TimeSpan left, TimeSpan right) => left < right ? left : right;

    public Task<ILease> AcquireAsync(CancellationToken cancellationToken = default)
        => AcquireAsync(provider.DefaultAcquisition, cancellationToken);

    public Task<ILease> AcquireAsync(AcquireOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return AcquireAsync(options.WaitTimeout, options.Lease, cancellationToken);
    }
}

internal sealed class EtcdLease : IEtcdLease
{
    private readonly EtcdProvider provider;
    private readonly long id;
    private readonly LeaseOptions options;
    private readonly bool autoRenew;
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly CancellationTokenSource stop = new(), lost = new();
    private readonly Task renewal;
    private ByteString? key;
    private long createRevision;
    private long validUntil;
    private readonly object validityGate = new();
    private int acquiring;
    private int state = (int)LeaseState.Held;
    private readonly Lazy<Task<ReleaseOutcome>> release;

    internal EtcdLease(EtcdProvider provider, long id, LeaseOptions options, TimeSpan initialTtl, long grantSent, bool acquiring)
    {
        this.provider = provider; this.id = id; this.options = options; autoRenew = options.AutoRenew;
        this.acquiring = acquiring ? 1 : 0;
        release = new Lazy<Task<ReleaseOutcome>>(ReleaseCoreAsync, LazyThreadSafetyMode.ExecutionAndPublication);
        SetValidity(initialTtl, grantSent, initial: true);
        renewal = RunRenewalAsync();
    }

    public long EffectiveLeaseMilliseconds { get; private set; }
    public long CreateRevision => Volatile.Read(ref createRevision);
    public string Epoch => provider.Epoch;
    public CancellationToken Lost => lost.Token;
    public LeaseState State
    {
        get
        {
            lock (validityGate)
            {
                if (Stopwatch.GetTimestamp() >= Volatile.Read(ref validUntil)) Lose();
            }
            return (LeaseState)Volatile.Read(ref state);
        }
    }

    private bool SetValidity(TimeSpan ttl, long sent, bool initial = false)
    {
        lock (validityGate)
        {
            var now = Stopwatch.GetTimestamp();
            if (Volatile.Read(ref state) != (int)LeaseState.Held
                || !TryCalculateValidityDeadline(sent, now, Volatile.Read(ref validUntil), ttl, initial, out var newDeadline))
            {
                Lose();
                return false;
            }
            EffectiveLeaseMilliseconds = (long)ttl.TotalMilliseconds;
            Volatile.Write(ref validUntil, newDeadline);
            return true;
        }
    }
    internal static bool TryCalculateValidityDeadline(long sent, long now, long previousDeadline,
        TimeSpan ttl, bool initial, out long deadline)
    {
        var effectiveMilliseconds = (long)ttl.TotalMilliseconds;
        var allowance = TimeSpan.FromMilliseconds(Math.Max(10, effectiveMilliseconds * .01));
        deadline = sent + (long)((ttl - allowance).TotalSeconds * Stopwatch.Frequency);
        return (initial || now < previousDeadline) && now < deadline;
    }
    private void Lose()
    { if (Interlocked.CompareExchange(ref state, (int)LeaseState.Lost, (int)LeaseState.Held) == (int)LeaseState.Held) lost.Cancel(); }
    internal void MarkLost() => Lose();
    internal void Activate(ByteString key, long revision)
    { this.key = key; Volatile.Write(ref createRevision, revision); Volatile.Write(ref acquiring, 0); }

    private async Task<bool> KeepAliveCoreAsync(CancellationToken ct)
    {
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (State != LeaseState.Held) return false;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(options.RpcTimeout);
            using var call = provider.LeaseClient.LeaseKeepAlive(cancellationToken: timeout.Token);
            var sent = Stopwatch.GetTimestamp();
            await call.RequestStream.WriteAsync(new LeaseKeepAliveRequest { ID = id }).ConfigureAwait(false);
            if (!await call.ResponseStream.MoveNext(timeout.Token).ConfigureAwait(false)) { Lose(); return false; }
            var ack = call.ResponseStream.Current;
            if (ack.ID != id || ack.TTL <= 0) { Lose(); return false; }
            return SetValidity(TimeSpan.FromSeconds(ack.TTL), sent);
        }
        catch (OperationCanceledException) { Lose(); if (ct.IsCancellationRequested) throw; return false; }
        catch (RpcException) { Lose(); return false; }
        finally { gate.Release(); }
    }

    private async Task RunRenewalAsync()
    {
        try
        {
            while (!stop.IsCancellationRequested && State == LeaseState.Held)
            {
                if (acquiring == 0 && !autoRenew) break;
                var delay = TimeSpan.FromMilliseconds(Math.Max(100, EffectiveLeaseMilliseconds / 3));
                await Task.Delay(delay, stop.Token).ConfigureAwait(false);
                if (acquiring == 0 && !autoRenew) break;
                if (!await KeepAliveCoreAsync(stop.Token).ConfigureAwait(false)) break;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch (Exception) { Lose(); }
    }

    public Task<bool> RenewAsync(CancellationToken cancellationToken = default) => KeepAliveCoreAsync(cancellationToken);

    internal async Task<bool> AbandonAsync()
    {
        if (release.IsValueCreated)
            return await ReleaseAsync().ConfigureAwait(false) != ReleaseOutcome.Unknown;
        var cleanupStarted = Stopwatch.GetTimestamp();
        stop.Cancel();
        var drainBudget = EtcdProvider.RemainingCleanup(options.CleanupTimeout, cleanupStarted);
        try { await renewal.WaitAsync(drainBudget).ConfigureAwait(false); }
        catch (Exception e) when (e is OperationCanceledException or TimeoutException) { }
        Lose();
        var revokeBudget = EtcdProvider.RemainingCleanup(options.CleanupTimeout, cleanupStarted);
        var revoked = await provider.RevokeAsync(id, revokeBudget).ConfigureAwait(false);
        provider.Untrack(this);
        return revoked is EtcdProvider.RevokeObservation.Acknowledged or EtcdProvider.RevokeObservation.NotFound;
    }

    public Task<ReleaseOutcome> ReleaseAsync() => release.Value;
    private async Task<ReleaseOutcome> ReleaseCoreAsync()
    {
        var cleanupStarted = Stopwatch.GetTimestamp();
        using var cleanup = new CancellationTokenSource(options.CleanupTimeout);
        stop.Cancel();
        try { await renewal.WaitAsync(cleanup.Token).ConfigureAwait(false); } catch (OperationCanceledException) { }
        try { await gate.WaitAsync(cleanup.Token).ConfigureAwait(false); }
        catch (OperationCanceledException)
        {
            Interlocked.Exchange(ref state, (int)LeaseState.ReleasedUnknown);
            provider.Untrack(this);
            return ReleaseOutcome.Unknown;
        }
        try
        {
            if (Interlocked.Exchange(ref state, (int)LeaseState.Releasing) == (int)LeaseState.Released) return ReleaseOutcome.AlreadyNotOwned;
            var ownership = key is { } exact
                ? await provider.ObserveOwnershipAsync(exact, id,
                    EtcdProvider.RemainingCleanup(options.CleanupTimeout, cleanupStarted), cleanup.Token).ConfigureAwait(false)
                : EtcdProvider.OwnershipObservation.NotOwned;
            var unlockAcknowledged = false;
            if (key is { } ownedKey && ownership == EtcdProvider.OwnershipObservation.Owned && !cleanup.IsCancellationRequested)
            {
                try
                {
                    await provider.LockClient.UnlockAsync(new UnlockRequest { Key = ownedKey }, deadline: DateTime.UtcNow + EtcdProvider.RemainingCleanup(options.CleanupTimeout, cleanupStarted),
                        cancellationToken: cleanup.Token).ConfigureAwait(false);
                    unlockAcknowledged = true;
                }
                catch (RpcException e) when (e.StatusCode == StatusCode.NotFound) { }
                catch (Exception e) when (e is RpcException or OperationCanceledException or TimeoutException or HttpRequestException) { }
            }
            var revoked = await provider.RevokeAsync(id, EtcdProvider.RemainingCleanup(options.CleanupTimeout, cleanupStarted), cleanup.Token).ConfigureAwait(false);
            var result = EtcdProvider.ClassifyRelease(ownership, unlockAcknowledged, revoked);
            Interlocked.Exchange(ref state, (int)(result == ReleaseOutcome.Unknown ? LeaseState.ReleasedUnknown : LeaseState.Released));
            provider.Untrack(this);
            return result;
        }
        finally { gate.Release(); }
    }
    public async ValueTask DisposeAsync()
    {
        ReleaseOutcome result;
        try { result = await ReleaseAsync().ConfigureAwait(false); }
        catch (Exception) { result = ReleaseOutcome.Unknown; }
        provider.ObserveCleanup(result, "lease-dispose");
    }

    internal async ValueTask DisposeFromProviderAsync()
    {
        ReleaseOutcome result;
        try { result = await ReleaseAsync().ConfigureAwait(false); }
        catch (Exception) { result = ReleaseOutcome.Unknown; }
        provider.ObserveCleanup(result, "provider-dispose");
    }
}
