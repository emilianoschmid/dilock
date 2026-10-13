using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using StackExchange.Redis;

namespace Dilock.Redis;

/// <summary>Immutable connection and default acquisition policy for one Redis authority.</summary>
public sealed record RedisProviderOptions
{
    /// <summary>One redis:// or rediss:// authority; credentials in URI user info are not logged.</summary>
    public required string Endpoint { get; init; }
    /// <summary>Provisioned protocol namespace. Defaults to <c>default</c>.</summary>
    public string Namespace { get; init; } = "default";
    /// <summary>Deployment-approved epoch already recorded in namespace metadata.</summary>
    public required string ExpectedEpoch { get; init; }
    /// <summary>Maximum time to establish the owned StackExchange.Redis connection.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Policy used by handle.AcquireAsync() when no per-acquisition policy is supplied.</summary>
    public AcquireOptions DefaultAcquisition { get; init; } = new();
    /// <summary>Optional, non-throwing observer for Unknown outcomes from scope cleanup.</summary>
    public Action<CleanupObservation>? CleanupObserver { get; init; }
}

/// <summary>One Redis authority; owns its connections, leases, renewal and cleanup tasks.</summary>
public sealed class RedisProvider : ILockProvider
{
    internal readonly IDatabase Database;
    private readonly ConnectionMultiplexer connection;
    private readonly string ns, epoch;
    internal readonly AcquireOptions DefaultAcquisition;
    private readonly Action<CleanupObservation>? cleanupObserver;
    internal readonly ConcurrentDictionary<RedisLease, byte> Leases = new();
    private readonly ConcurrentDictionary<Task, byte> cleanup = new();
    private int closed;
    internal bool Closed => Volatile.Read(ref closed) != 0;
    internal static readonly string ReleaseScript = Script("redis-release.lua"), RenewScript = Script("redis-renew.lua");
    private RedisProvider(ConnectionMultiplexer connection, RedisProviderOptions options)
    {
        this.connection = connection; Database = connection.GetDatabase();
        ns = options.Namespace; epoch = options.ExpectedEpoch;
        DefaultAcquisition = options.DefaultAcquisition with { Lease = options.DefaultAcquisition.Lease with { } };
        cleanupObserver = options.CleanupObserver;
    }

    /// <summary>Connects to one Redis authority using fail-fast commands and no command retries.</summary>
    /// <remarks>
    /// The provider owns the resulting connection and disposes it from <see cref="DisposeAsync"/>.
    /// The endpoint accepts <c>redis://</c> or <c>rediss://</c> with optional URI-escaped user info;
    /// it must identify one standalone authority. Redis does not provide strong fencing, so
    /// <see cref="LeaseOptions.RequireFencing"/> is rejected during acquisition. Credentials are not logged.
    /// Namespace metadata must already exist and match the Redis profile; connect never provisions it.
    /// </remarks>
    /// <param name="endpoint">One Redis authority URI without a path, query, or fragment.</param>
    /// <param name="ns">Validated protocol namespace.</param>
    /// <param name="epoch">Deployment-approved coordinator incarnation.</param>
    /// <param name="cancellationToken">Cancels connection and initial metadata validation before a provider is returned.</param>
    /// <returns>A provider that owns the Redis connection and any leases created through it.</returns>
    /// <exception cref="ArgumentException">The endpoint, namespace, or epoch is invalid, or the endpoint names multiple authorities.</exception>
    /// <exception cref="OperationCanceledException">Connection or metadata validation was cancelled; no provider is returned.</exception>
    /// <exception cref="DilockException">Connection failed or namespace metadata is missing/incompatible.</exception>
    public static Task<RedisProvider> ConnectAsync(string endpoint, string ns, string epoch, CancellationToken cancellationToken = default)
        => ConnectAsync(new RedisProviderOptions { Endpoint = endpoint, Namespace = ns, ExpectedEpoch = epoch }, cancellationToken);

    /// <summary>Connects to the default namespace using the explicit deployment epoch and common defaults.</summary>
    public static Task<RedisProvider> ConnectAsync(string endpoint, string expectedEpoch, CancellationToken cancellationToken = default)
        => ConnectAsync(new RedisProviderOptions { Endpoint = endpoint, ExpectedEpoch = expectedEpoch }, cancellationToken);

    /// <summary>Connects using a complete immutable provider configuration and validates existing metadata.</summary>
    public static async Task<RedisProvider> ConnectAsync(RedisProviderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ResourceIdentity.Hash(options.Namespace, "validation");
        if (string.IsNullOrWhiteSpace(options.ExpectedEpoch) || options.ExpectedEpoch.Length > 1024) throw new ArgumentException("Invalid epoch", nameof(options));
        if (options.ConnectTimeout <= TimeSpan.Zero || options.ConnectTimeout > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(options), "Connect timeout must be positive and at most one day");
        ArgumentNullException.ThrowIfNull(options.DefaultAcquisition);
        options.DefaultAcquisition.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        var config = ParseEndpoint(options.Endpoint);
        if (config.EndPoints.Count != 1 || config.ServiceName != null) throw new ArgumentException("Exactly one authority is required", nameof(options));
        config.BacklogPolicy = BacklogPolicy.FailFast; config.ConnectRetry = 0; config.AbortOnConnectFail = true;
        config.AsyncTimeout = (int)Math.Ceiling(options.ConnectTimeout.TotalMilliseconds);
        config.SyncTimeout = config.AsyncTimeout; config.ConnectTimeout = config.AsyncTimeout;
        ConnectionMultiplexer cx;
        try { cx = await ConnectionMultiplexer.ConnectAsync(config).ConfigureAwait(false); }
        catch (Exception e) when (e is RedisException or TimeoutException) { throw new DilockException(FailureKind.BackendUnavailable, "Redis connection failed", e); }
        if (cancellationToken.IsCancellationRequested) { await cx.DisposeAsync(); cancellationToken.ThrowIfCancellationRequested(); }
        var provider = new RedisProvider(cx, options);
        try
        {
            await provider.ValidateMetadata(options.ConnectTimeout, cancellationToken).ConfigureAwait(false);
            return provider;
        }
        catch
        {
            // Metadata validation is the primary factory operation. A failed
            // best-effort connection cleanup must not replace its exception.
            try { await provider.DisposeAsync().ConfigureAwait(false); }
            catch (Exception) { }
            throw;
        }
    }
    private static ConfigurationOptions ParseEndpoint(string endpoint)
    {
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || uri.Scheme is not ("redis" or "rediss")
            || uri.Query.Length != 0 || uri.Fragment.Length != 0 || uri.AbsolutePath is not ("" or "/"))
            throw new ArgumentException("Endpoint must be redis://host:port or rediss://host:port without a path/query", nameof(endpoint));
        var config = ConfigurationOptions.Parse(uri.Authority);
        config.Ssl = uri.Scheme == "rediss";
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var fields = uri.UserInfo.Split(':', 2);
            if (fields.Length == 1) config.Password = Uri.UnescapeDataString(fields[0]);
            else { config.User = Uri.UnescapeDataString(fields[0]); config.Password = Uri.UnescapeDataString(fields[1]); }
        }
        return config;
    }
    private static string Script(string name)
    { using var s = typeof(RedisProvider).Assembly.GetManifestResourceStream(name)!; using var r = new StreamReader(s); return r.ReadToEnd(); }
    /// <inheritdoc/>
    public ILockHandle Lock(string resource)
    { ObjectDisposedException.ThrowIf(Closed, this); return new RedisHandle(this, $"dilock:{{{ResourceIdentity.Hash(ns, resource)}}}:lease"); }

    internal async Task ValidateMetadata(TimeSpan budget, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Closed, this);
        var started = Stopwatch.GetTimestamp();
        var key = $"dilock:namespace:{ns}:meta";
        var typeBudget = RemainingBudget(budget, started);
        if (typeBudget <= TimeSpan.Zero)
            throw new DilockException(FailureKind.BackendUnavailable, "Namespace metadata validation budget elapsed");
        RedisType type;
        try { type = await Database.KeyTypeAsync(key).WaitAsync(typeBudget, ct).ConfigureAwait(false); }
        catch (Exception e) when (e is RedisException or TimeoutException)
        { throw new DilockException(FailureKind.BackendUnavailable, "Namespace type check failed", e); }
        ValidateMetadataType(type);

        var readBudget = RemainingBudget(budget, started);
        if (readBudget <= TimeSpan.Zero) throw new DilockException(FailureKind.BackendUnavailable, "Namespace metadata validation budget elapsed");
        RedisValue raw;
        try { raw = await Database.StringGetAsync(key).WaitAsync(readBudget, ct).ConfigureAwait(false); }
        catch (Exception e) when (e is RedisException or TimeoutException) { throw new DilockException(FailureKind.BackendUnavailable, "Namespace read failed", e); }
        try
        {
            if (raw.IsNull || ((byte[]?)raw)!.Length > 8192) throw new JsonException();
            using var doc = JsonDocument.Parse((string)raw!, new JsonDocumentOptions { MaxDepth = 16 });
            var m = doc.RootElement;
            if (!HasUniqueTopLevelProperties(m)
                || m.GetProperty("protocol_major").GetInt32() != 1 || m.GetProperty("identity_scheme").GetString() != "sha256-framed-utf8-v1"
                || m.GetProperty("profile").GetString() != "redis-single-authority-v1" || m.GetProperty("coordination_epoch").GetString() != epoch
                || m.GetProperty("fixed_lease_ms").ValueKind != JsonValueKind.Null) throw new JsonException();
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException or KeyNotFoundException or FormatException)
        { throw new DilockException(FailureKind.ProtocolMismatch, "Missing or incompatible namespace metadata", e); }
    }

    internal static void ValidateMetadataType(RedisType type)
    {
        if (type != RedisType.String)
            throw new DilockException(FailureKind.ProtocolMismatch, "Namespace metadata must be a Redis string");
    }

    internal static bool HasUniqueTopLevelProperties(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object) return false;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in value.EnumerateObject())
            if (!names.Add(property.Name)) return false;
        return true;
    }

    internal static TimeSpan RemainingBudget(TimeSpan budget, long started)
    {
        var remaining = budget - Stopwatch.GetElapsedTime(started);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }
    internal static FailureKind LateAcquisitionFailure(ReleaseOutcome cleanup)
        => cleanup == ReleaseOutcome.Unknown ? FailureKind.AcquisitionUnknown : FailureKind.WaitTimeout;
    internal static Exception CancelledAcquisitionFailure(ReleaseOutcome cleanup, CancellationToken cancellationToken)
        => cleanup == ReleaseOutcome.Unknown
            ? new DilockException(FailureKind.AcquisitionUnknown, "Cancelled Redis acquisition cleanup was not acknowledged")
            : new OperationCanceledException("Redis acquisition was cancelled after owner cleanup was acknowledged", cancellationToken);
    internal void ObserveCleanup(ReleaseOutcome outcome, string reason)
    {
        if (outcome == ReleaseOutcome.Released || cleanupObserver is null) return;
        try { cleanupObserver(new CleanupObservation("redis-single-authority-v1", outcome, reason)); }
        catch (Exception) { /* Diagnostics must not escape a scope-cleanup path. */ }
    }
    internal Task<ReleaseOutcome> Cleanup(string key, string owner, TimeSpan budget)
    {
        var task = Execute(); cleanup.TryAdd(task, 0);
        _ = task.ContinueWith(t => cleanup.TryRemove(t, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
        async Task<ReleaseOutcome> Execute()
        {
            try
            { var r = (long)await Database.ScriptEvaluateAsync(ReleaseScript, [(RedisKey)key], [(RedisValue)owner]).WaitAsync(budget).ConfigureAwait(false); return r == 1 ? ReleaseOutcome.Released : r == 0 ? ReleaseOutcome.AlreadyNotOwned : ReleaseOutcome.Unknown; }
            catch (Exception e) when (e is RedisException or TimeoutException) { return ReleaseOutcome.Unknown; }
        }
    }
    internal Task Abandon(Task< bool > request, string key, string owner, TimeSpan budget)
    {
        // Cleanup again AFTER any late SET result. Owner comparison cannot delete a successor.
        var task = Observe(); cleanup.TryAdd(task, 0);
        _ = task.ContinueWith(t => cleanup.TryRemove(t, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
        async Task Observe()
        {
            await Cleanup(key, owner, budget).ConfigureAwait(false);
            try { await request.ConfigureAwait(false); } catch (Exception e) when (e is RedisException or TimeoutException) { }
            await Cleanup(key, owner, budget).ConfigureAwait(false);
        }
    }
    /// <summary>Withdraws local reliance, attempts bounded cleanup of active leases, and disposes the private client.</summary>
    /// <remarks>Disposal is idempotent. Completion does not prove that an unacknowledged orphan is absent from Redis.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        foreach (var lease in Leases.Keys) { lease.MarkLost(); await lease.DisposeFromProviderAsync().ConfigureAwait(false); }
        try { await Task.WhenAll(cleanup.Keys).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); } catch (TimeoutException) { }
        await connection.DisposeAsync().ConfigureAwait(false);
    }
}

internal sealed class RedisHandle(RedisProvider provider, string key) : ILockHandle
{
    public Task<ILease?> TryAcquireAsync(LeaseOptions? options = null, CancellationToken cancellationToken = default)
        => TryAcquireCoreAsync(options, cancellationToken, waitBudget: null, waitStarted: 0);

    private async Task<ILease?> TryAcquireCoreAsync(LeaseOptions? options, CancellationToken cancellationToken,
        TimeSpan? waitBudget, long waitStarted)
    {
        var o = options ?? provider.DefaultAcquisition.Lease; o.Validate();
        if (o.RequireFencing) throw new DilockException(FailureKind.UnsupportedCapability, "Redis does not support strong fencing");
        var metadataBudget = AttemptBudget(o.RpcTimeout, waitBudget, waitStarted);
        if (metadataBudget <= TimeSpan.Zero) throw WaitExpired();
        try { await provider.ValidateMetadata(metadataBudget, cancellationToken).ConfigureAwait(false); }
        catch (DilockException e) when (waitBudget.HasValue && RemainingWait(waitBudget.Value, waitStarted) <= TimeSpan.Zero
            && e.Kind == FailureKind.BackendUnavailable)
        { throw WaitExpired(); }
        if (waitBudget.HasValue && RemainingWait(waitBudget.Value, waitStarted) <= TimeSpan.Zero) throw WaitExpired();
        cancellationToken.ThrowIfCancellationRequested();
        var acquisitionBudget = AttemptBudget(o.RpcTimeout, waitBudget, waitStarted);
        if (acquisitionBudget <= TimeSpan.Zero) throw WaitExpired();
        var owner = "dilock/1/" + Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant();
        var sent = Stopwatch.GetTimestamp();
        var command = provider.Database.StringSetAsync(key, owner, TimeSpan.FromMilliseconds(o.TtlMilliseconds), When.NotExists);
        bool ok;
        try { ok = await command.WaitAsync(acquisitionBudget, cancellationToken).ConfigureAwait(false); }
        catch (RedisServerException e) { throw new DilockException(FailureKind.BackendUnavailable, "Redis rejected acquisition", e); }
        catch (Exception e) when (e is RedisException or TimeoutException or OperationCanceledException)
        {
            _ = provider.Abandon(command, key, owner, o.CleanupTimeout);
            throw new DilockException(FailureKind.AcquisitionUnknown, "Dispatched SET has no ownership acknowledgement", e);
        }
        if (!ok)
        {
            var classificationBudget = AttemptBudget(o.RpcTimeout, waitBudget, waitStarted);
            if (classificationBudget <= TimeSpan.Zero) throw WaitExpired();
            RedisType keyType;
            try { keyType = await provider.Database.KeyTypeAsync(key).WaitAsync(classificationBudget, cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) { throw; }
            catch (TimeoutException) when (waitBudget.HasValue && RemainingWait(waitBudget.Value, waitStarted) <= TimeSpan.Zero)
            { throw WaitExpired(); }
            catch (Exception e) { throw new DilockException(FailureKind.BackendUnavailable, "Redis could not classify a contended lock key", e); }
            if (waitBudget.HasValue && RemainingWait(waitBudget.Value, waitStarted) <= TimeSpan.Zero) throw WaitExpired();
            if (keyType is not (RedisType.String or RedisType.None))
                throw new DilockException(FailureKind.ProtocolMismatch, $"Redis lock key has unexpected type '{keyType}'");
            return null;
        }
        var validity = TimeSpan.FromMilliseconds(o.TtlMilliseconds) - o.Margin;
        if (Stopwatch.GetElapsedTime(sent) >= validity || provider.Closed || cancellationToken.IsCancellationRequested)
        {
            var cleanup = await provider.Cleanup(key, owner, o.CleanupTimeout).ConfigureAwait(false);
            if (cleanup == ReleaseOutcome.Unknown)
                throw new DilockException(FailureKind.AcquisitionUnknown, "Ownership acknowledgement arrived too late or cancellation cleanup was not acknowledged");
            if (cancellationToken.IsCancellationRequested)
                throw RedisProvider.CancelledAcquisitionFailure(cleanup, cancellationToken);
            if (waitBudget.HasValue && RemainingWait(waitBudget.Value, waitStarted) <= TimeSpan.Zero)
                throw WaitExpired();
            if (provider.Closed) throw new ObjectDisposedException(nameof(RedisProvider));
            throw new DilockException(FailureKind.LeaseLost, "Redis ownership acknowledgement arrived after conservative lease validity ended");
        }
        var lease = new RedisLease(provider, key, owner, o, sent); provider.Leases.TryAdd(lease, 0); return lease;
    }
    public async Task<ILease> AcquireAsync(TimeSpan wait, LeaseOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (wait <= TimeSpan.Zero || wait > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(wait));
        var o = options ?? provider.DefaultAcquisition.Lease; o.Validate(); var start = Stopwatch.GetTimestamp();
        while (true)
        {
            var remaining = wait - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "Contention wait exhausted");
            var maxRpc = o.LeaseDuration - o.Margin - TimeSpan.FromMilliseconds(1);
            if (maxRpc <= TimeSpan.Zero) { await Task.Delay(remaining, cancellationToken).ConfigureAwait(false); continue; }
            var attempt = remaining < o.RpcTimeout ? remaining : o.RpcTimeout;
            if (attempt >= maxRpc) attempt = maxRpc;
            var lease = await TryAcquireCoreAsync(o with { RpcTimeout = attempt }, cancellationToken, wait, start).ConfigureAwait(false);
            if (lease != null)
            {
                if (Stopwatch.GetElapsedTime(start) < wait && !cancellationToken.IsCancellationRequested) return lease;
                var cleanup = await lease.ReleaseAsync().ConfigureAwait(false);
                if (cleanup == ReleaseOutcome.Unknown)
                    throw new DilockException(FailureKind.AcquisitionUnknown, "Late Redis acquisition cleanup was not acknowledged");
                if (cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException("Redis acquisition was cancelled after owner cleanup was acknowledged", cancellationToken);
                throw new DilockException(RedisProvider.LateAcquisitionFailure(cleanup), "Late acquisition was cleaned up after the caller wait deadline");
            }
            remaining = wait - Stopwatch.GetElapsedTime(start);
            if (remaining > TimeSpan.Zero) await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(remaining.TotalMilliseconds, Random.Shared.Next(20, 51))), cancellationToken).ConfigureAwait(false);
        }
    }

    internal static TimeSpan RemainingWait(TimeSpan wait, long started)
    {
        var remaining = wait - Stopwatch.GetElapsedTime(started);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    internal static TimeSpan AttemptBudget(TimeSpan rpcTimeout, TimeSpan? wait, long started)
    {
        if (wait is null) return rpcTimeout;
        var remaining = RemainingWait(wait.Value, started);
        return remaining < rpcTimeout ? remaining : rpcTimeout;
    }

    private static DilockException WaitExpired()
        => new(FailureKind.WaitTimeout, "Contention wait exhausted");

    public Task<ILease> AcquireAsync(CancellationToken cancellationToken = default)
        => AcquireAsync(provider.DefaultAcquisition, cancellationToken);

    public Task<ILease> AcquireAsync(AcquireOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return AcquireAsync(options.WaitTimeout, options.Lease, cancellationToken);
    }
}

internal sealed class RedisLease : ILease
{
    private readonly RedisProvider provider;
    private readonly string key, owner;
    private readonly LeaseOptions options;
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly CancellationTokenSource lost = new(), stop = new();
    private readonly Task renewal;
    private readonly Timer expiry;
    private long sent;
    private int state = (int)LeaseState.Held;
    private ReleaseOutcome? outcome;
    public RedisLease(RedisProvider provider, string key, string owner, LeaseOptions options, long sent)
    {
        this.provider = provider; this.key = key; this.owner = owner; this.options = options; this.sent = sent;
        expiry = new Timer(_ => Expire(), null, RemainingValidity(sent, options.LeaseDuration, options.Margin), Timeout.InfiniteTimeSpan);
        renewal = options.AutoRenew ? Run() : Task.CompletedTask;
    }
    internal static TimeSpan RemainingValidity(long sent, TimeSpan leaseDuration, TimeSpan margin)
    {
        var remaining = leaseDuration - margin - Stopwatch.GetElapsedTime(sent);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }
    public long EffectiveLeaseMilliseconds => options.TtlMilliseconds;
    public CancellationToken Lost => lost.Token;
    public LeaseState State
    {
        get
        { if (Stopwatch.GetElapsedTime(Volatile.Read(ref sent)) >= TimeSpan.FromMilliseconds(options.TtlMilliseconds) - options.Margin || provider.Closed) Lose(); return (LeaseState)Volatile.Read(ref state); }
    }
    private void Lose()
    { if (Interlocked.CompareExchange(ref state, (int)LeaseState.Lost, (int)LeaseState.Held) == (int)LeaseState.Held) lost.Cancel(); }
    internal void MarkLost() => Lose();
    private void Expire()
    { if (Stopwatch.GetElapsedTime(Volatile.Read(ref sent)) >= options.LeaseDuration - options.Margin) Lose(); }
    private async Task Run()
    {
        try
        {
            while (State == LeaseState.Held)
            {
                await Task.Delay(options.LeaseDuration / 3, stop.Token).ConfigureAwait(false);
                if (!await RenewAsync(stop.Token).ConfigureAwait(false)) break;
            }
        }
        catch (Exception e) when (e is RedisException or TimeoutException or OperationCanceledException) { if (!stop.IsCancellationRequested) Lose(); }
    }
    public async Task<bool> RenewAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (State != LeaseState.Held) return false;
            cancellationToken.ThrowIfCancellationRequested(); var start = Stopwatch.GetTimestamp();
            var remaining = TimeSpan.FromMilliseconds(options.TtlMilliseconds) - options.Margin - Stopwatch.GetElapsedTime(sent);
            var budget = remaining < options.RpcTimeout ? remaining : options.RpcTimeout;
            long result;
            try { result = (long)await provider.Database.ScriptEvaluateAsync(RedisProvider.RenewScript, [(RedisKey)key], [(RedisValue)owner, options.TtlMilliseconds]).WaitAsync(budget, cancellationToken).ConfigureAwait(false); }
            catch { Lose(); throw; }
            if (State != LeaseState.Held || result != 1 || Stopwatch.GetElapsedTime(start) >= TimeSpan.FromMilliseconds(options.TtlMilliseconds) - options.Margin) { Lose(); return false; }
            Volatile.Write(ref sent, start); expiry.Change(RemainingValidity(start, options.LeaseDuration, options.Margin), Timeout.InfiniteTimeSpan); return true;
        }
        finally { gate.Release(); }
    }
    public async Task<ReleaseOutcome> ReleaseAsync()
    {
        stop.Cancel(); await renewal.ConfigureAwait(false); await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (outcome.HasValue) return outcome.Value;
            Interlocked.Exchange(ref state, (int)LeaseState.Releasing);
            expiry.Dispose();
            outcome = await provider.Cleanup(key, owner, options.CleanupTimeout).ConfigureAwait(false);
            Interlocked.Exchange(ref state, (int)(outcome == ReleaseOutcome.Unknown ? LeaseState.ReleasedUnknown : LeaseState.Released));
            provider.Leases.TryRemove(this, out _); return outcome.Value;
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
