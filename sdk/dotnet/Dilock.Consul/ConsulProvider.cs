using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dilock;

namespace Dilock.Consul;

/// <summary>Immutable configuration for one Consul HTTP agent and namespace.</summary>
public sealed record ConsulProviderOptions
{
    /// <summary>One HTTP(S) Consul agent authority without credentials or path.</summary>
    public required string Endpoint { get; init; }
    /// <summary>Provisioned Dilock namespace; defaults to <c>default</c>.</summary>
    public string Namespace { get; init; } = "default";
    /// <summary>Deployment-approved coordinator epoch already stored in Consul metadata.</summary>
    public required string ExpectedEpoch { get; init; }
    /// <summary>Optional ACL token sent only in the <c>X-Consul-Token</c> request header.</summary>
    public string? Token { get; init; }
    /// <summary>Bound for the initial metadata validation request.</summary>
    public TimeSpan MetadataTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Optional fresh owned HTTP handler factory, useful for TLS and isolated tests.</summary>
    public Func<HttpMessageHandler>? HttpHandlerFactory { get; init; }
    /// <summary>Default wait and lease policy used by this provider.</summary>
    public AcquireOptions DefaultAcquisition { get; init; } = new();
    /// <summary>Optional diagnostic callback for unconfirmed or already completed scope cleanup.</summary>
    public Action<CleanupObservation>? CleanupObserver { get; init; }
}

/// <summary>A Consul LockIndex fence paired with its approved coordination epoch.</summary>
public interface IConsulLease : ILease
{
    /// <summary>Approved Consul coordination epoch captured at acquisition.</summary>
    string Epoch { get; }
    /// <summary>Per-key Consul fence; safe to rely on only while the lock KV entry is retained.</summary>
    long LockIndex { get; }
}

/// <summary>Consul HTTP/KV session adapter for the shared lock contract.</summary>
public sealed class ConsulProvider : ILockProvider
{
    private const string Profile = "consul-kv-session-v1";
    private const string Scheme = "sha256-framed-utf8-v1";
    private const int MinTtlSeconds = 10;
    private const int MaxTtlSeconds = 86400;
    private readonly HttpClient client;
    private readonly string ns, epoch;
    private readonly AcquireOptions defaults;
    private readonly Action<CleanupObservation>? cleanupObserver;
    private readonly ConcurrentDictionary<ConsulLease, byte> leases = new();
    private readonly ConcurrentDictionary<Task, byte> cleanups = new();
    private int closed;
    internal bool Closed => Volatile.Read(ref closed) != 0;

    private ConsulProvider(HttpClient client, ConsulProviderOptions options)
    {
        this.client = client;
        ns = options.Namespace;
        epoch = options.ExpectedEpoch;
        defaults = options.DefaultAcquisition with { Lease = options.DefaultAcquisition.Lease with { } };
        cleanupObserver = options.CleanupObserver;
    }

    /// <summary>Creates a provider after a bounded linearizable metadata check.</summary>
    public static async Task<ConsulProvider> ConnectAsync(
        ConsulProviderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = ResourceIdentity.Hash(options.Namespace, "validation");
        if (string.IsNullOrWhiteSpace(options.ExpectedEpoch)
            || Encoding.UTF8.GetByteCount(options.ExpectedEpoch) > 1024
            || options.ExpectedEpoch.Contains('\0'))
            throw new ArgumentException("Invalid coordination epoch", nameof(options));
        if (options.MetadataTimeout <= TimeSpan.Zero || options.MetadataTimeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options), "Metadata timeout must be positive and at most one day");
        ArgumentNullException.ThrowIfNull(options.DefaultAcquisition);
        options.DefaultAcquisition.Validate();
        if (!Uri.TryCreate(options.Endpoint, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("http" or "https") || uri.UserInfo.Length != 0
            || uri.Query.Length != 0 || uri.Fragment.Length != 0 || uri.AbsolutePath is not ("" or "/"))
            throw new ArgumentException("Endpoint must be one http(s) authority without credentials, path, query or fragment", nameof(options));
        if (options.Token is not null && (options.Token.Length == 0 || options.Token.Any(char.IsControl)
            || options.Token.Any(c => c > 0x7e || c < 0x21)))
            throw new ArgumentException("Consul token must be nonempty ASCII header text", nameof(options));

        var handler = options.HttpHandlerFactory?.Invoke() ?? new SocketsHttpHandler { AllowAutoRedirect = false };
        if (options.HttpHandlerFactory is not null && handler is SocketsHttpHandler sockets)
            sockets.AllowAutoRedirect = false;
        var client = new HttpClient(handler, disposeHandler: true) { BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority) + "/"), Timeout = Timeout.InfiniteTimeSpan };
        if (options.Token is not null) client.DefaultRequestHeaders.TryAddWithoutValidation("X-Consul-Token", options.Token);
        var provider = new ConsulProvider(client, options);
        try
        {
            await provider.ValidateMetadataAsync(options.MetadataTimeout, cancellationToken).ConfigureAwait(false);
            return provider;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>Connects to the default namespace using common options.</summary>
    public static Task<ConsulProvider> ConnectAsync(string endpoint, string expectedEpoch,
        CancellationToken cancellationToken = default)
        => ConnectAsync(new ConsulProviderOptions { Endpoint = endpoint, ExpectedEpoch = expectedEpoch }, cancellationToken);

    /// <summary>Connects to the specified, previously provisioned namespace.</summary>
    public static Task<ConsulProvider> ConnectAsync(string endpoint, string nameSpace, string expectedEpoch,
        CancellationToken cancellationToken = default)
        => ConnectAsync(new ConsulProviderOptions { Endpoint = endpoint, Namespace = nameSpace, ExpectedEpoch = expectedEpoch }, cancellationToken);

    /// <inheritdoc/>
    public ILockHandle Lock(string resource)
    {
        ObjectDisposedException.ThrowIf(Closed, this);
        return new ConsulHandle(this, "dilock/locks/" + ResourceIdentity.Hash(ns, resource));
    }

    internal async Task ValidateMetadataAsync(TimeSpan budget, CancellationToken ct)
    {
        var key = $"dilock/namespaces/{ns}/meta";
        var response = await SendAsync(HttpMethod.Get, $"v1/kv/{key}?consistent=true", budget, ct, allow404: true).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            throw new DilockException(FailureKind.ProtocolMismatch, "Missing Consul namespace metadata");
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false), new JsonDocumentOptions { MaxDepth = 16 });
            var rows = doc.RootElement;
            if (rows.ValueKind != JsonValueKind.Array || rows.GetArrayLength() != 1) throw new JsonException();
            var row = rows[0];
            if (row.GetProperty("Key").GetString() != key) throw new JsonException();
            var raw = Convert.FromBase64String(row.GetProperty("Value").GetString() ?? "");
            if (raw.Length is 0 or > 8192) throw new JsonException();
            using var metaDoc = JsonDocument.Parse(raw, new JsonDocumentOptions { MaxDepth = 16 });
            var meta = metaDoc.RootElement;
            if (!UniqueProperties(meta) || meta.EnumerateObject().Count() != 5
                || meta.GetProperty("protocol_major").GetInt32() != 1
                || meta.GetProperty("identity_scheme").GetString() != Scheme
                || meta.GetProperty("profile").GetString() != Profile
                || meta.GetProperty("coordination_epoch").GetString() != epoch
                || meta.GetProperty("fixed_lease_ms").ValueKind != JsonValueKind.Null)
                throw new JsonException();
        }
        catch (Exception e) when (e is JsonException or FormatException or InvalidOperationException or KeyNotFoundException)
        { throw new DilockException(FailureKind.ProtocolMismatch, "Missing or incompatible Consul namespace metadata", e); }
    }

    private static bool UniqueProperties(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object) return false;
        var names = new HashSet<string>(StringComparer.Ordinal);
        return value.EnumerateObject().All(property => names.Add(property.Name));
    }

    internal async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, TimeSpan budget,
        CancellationToken ct, bool mutation = false, bool allow404 = false, HttpContent? content = null)
    {
        ct.ThrowIfCancellationRequested();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(budget);
        using var request = new HttpRequestMessage(method, path) { Content = content };
        try
        {
            var response = await client.SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token).ConfigureAwait(false);
            if (allow404 && response.StatusCode == System.Net.HttpStatusCode.NotFound) return response;
            if ((int)response.StatusCode is < 200 or >= 300)
            {
                var status = (int)response.StatusCode;
                response.Dispose();
                if (mutation && status >= 500)
                    throw new DilockException(FailureKind.AcquisitionUnknown, "Consul mutation acknowledgement is unknown");
                throw new DilockException(FailureKind.BackendUnavailable, $"Consul HTTP request rejected ({status})");
            }
            return response;
        }
        catch (DilockException) { throw; }
        catch (OperationCanceledException e) when (ct.IsCancellationRequested)
        {
            if (mutation) throw new DilockException(FailureKind.AcquisitionUnknown, "Consul mutation was cancelled after dispatch", e);
            throw;
        }
        catch (Exception e) when (e is OperationCanceledException or HttpRequestException or IOException)
        {
            if (mutation) throw new DilockException(FailureKind.AcquisitionUnknown, "Consul mutation acknowledgement is unknown", e);
            throw new DilockException(FailureKind.BackendUnavailable, "Consul request failed", e);
        }
    }

    internal async Task<(string Id, long Sent)> CreateSessionAsync(int ttl, TimeSpan budget, CancellationToken ct)
    {
        var sent = Stopwatch.GetTimestamp();
        using var body = JsonContent(new { Name = "dilock lease", Behavior = "release", LockDelay = "0s", TTL = $"{ttl}s", Checks = Array.Empty<string>() });
        using var response = await SendAsync(HttpMethod.Put, "v1/session/create", budget, ct, mutation: true, content: body).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false));
            var id = doc.RootElement.GetProperty("ID").GetString();
            if (string.IsNullOrWhiteSpace(id) || id.Length > 128) throw new JsonException();
            return (id, sent);
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException)
        { throw new DilockException(FailureKind.AcquisitionUnknown, "Consul session creation response was invalid", e); }
    }

    internal async Task<long?> RenewSessionAsync(string id, TimeSpan budget, CancellationToken ct)
    {
        var sent = Stopwatch.GetTimestamp();
        using var response = await SendAsync(HttpMethod.Put, "v1/session/renew/" + Uri.EscapeDataString(id), budget, ct, mutation: true).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false));
            var rows = doc.RootElement;
            return rows.ValueKind == JsonValueKind.Array && rows.GetArrayLength() == 1
                && rows[0].GetProperty("ID").GetString() == id ? sent : null;
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException)
        { throw new DilockException(FailureKind.ProtocolMismatch, "Consul session renewal response was invalid", e); }
    }

    internal async Task<bool> AcquireKeyAsync(string key, string session, TimeSpan budget, CancellationToken ct)
    {
        using var content = new ByteArrayContent([]);
        using var response = await SendAsync(HttpMethod.Put, $"v1/kv/{key}?acquire={Uri.EscapeDataString(session)}", budget, ct, mutation: true, content: content).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false));
            if (doc.RootElement.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw new JsonException();
            return doc.RootElement.GetBoolean();
        }
        catch (JsonException e) { throw new DilockException(FailureKind.AcquisitionUnknown, "Consul acquire response was invalid", e); }
    }

    internal async Task<(string Session, long Index)?> OwnerAsync(string key, TimeSpan budget, CancellationToken ct)
    {
        using var response = await SendAsync(HttpMethod.Get, $"v1/kv/{key}?consistent=true", budget, ct, allow404: true).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false));
            var rows = doc.RootElement;
            if (rows.ValueKind != JsonValueKind.Array || rows.GetArrayLength() != 1 || rows[0].GetProperty("Key").GetString() != key)
                throw new JsonException();
            var row = rows[0];
            var session = row.GetProperty("Session").GetString() ?? "";
            var index = row.GetProperty("LockIndex").GetInt64();
            if (index < 0) throw new JsonException();
            return (session, index);
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException or FormatException)
        { throw new DilockException(FailureKind.ProtocolMismatch, "Consul lock observation was malformed", e); }
    }

    internal async Task<bool> ReleaseKeyAsync(string key, string session, TimeSpan budget)
    {
        using var content = new ByteArrayContent([]);
        using var response = await SendAsync(HttpMethod.Put, $"v1/kv/{key}?release={Uri.EscapeDataString(session)}", budget, CancellationToken.None, mutation: true, content: content).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
            if (doc.RootElement.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw new JsonException();
            return doc.RootElement.GetBoolean();
        }
        catch (JsonException e) { throw new DilockException(FailureKind.AcquisitionUnknown, "Consul release response was invalid", e); }
    }

    internal async Task DestroySessionAsync(string session, TimeSpan budget)
    {
        using var response = await SendAsync(HttpMethod.Put, "v1/session/destroy/" + Uri.EscapeDataString(session), budget, CancellationToken.None, mutation: true).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
            if (doc.RootElement.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw new JsonException();
        }
        catch (JsonException e) { throw new DilockException(FailureKind.AcquisitionUnknown, "Consul destroy response was invalid", e); }
    }

    internal void Track(Task task)
    {
        cleanups.TryAdd(task, 0);
        _ = task.ContinueWith(t => cleanups.TryRemove(t, out _), CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    internal void Observe(ReleaseOutcome outcome, string reason)
    {
        if (outcome == ReleaseOutcome.Released || cleanupObserver is null) return;
        try { cleanupObserver(new CleanupObservation(Profile, outcome, reason)); }
        catch (Exception) { }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        foreach (var lease in leases.Keys) await lease.ReleaseFromProviderAsync().ConfigureAwait(false);
        if (!cleanups.IsEmpty)
        {
            try { await Task.WhenAll(cleanups.Keys).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
            catch (TimeoutException) { }
        }
        client.Dispose();
    }

    internal void Register(ConsulLease lease) => leases.TryAdd(lease, 0);
    internal void Unregister(ConsulLease lease) => leases.TryRemove(lease, out _);
    internal string Epoch => epoch;
    internal AcquireOptions Defaults => defaults;
    internal static int TtlSeconds(LeaseOptions options)
    {
        var requested = options.LeaseDuration.TotalSeconds;
        if (!double.IsFinite(requested) || requested < MinTtlSeconds || requested > MaxTtlSeconds)
            throw new DilockException(FailureKind.UnsupportedCapability, "Consul sessions require a TTL from 10 seconds through 24 hours");
        return checked((int)Math.Ceiling(requested));
    }
    internal static TimeSpan Validity(int ttl) => TimeSpan.FromSeconds(ttl) - TimeSpan.FromMilliseconds(Math.Max(10, ttl * 10));
    private static HttpContent JsonContent<T>(T value) => new StringContent(JsonSerializer.Serialize(value), Encoding.UTF8, "application/json");
}

internal sealed class ConsulHandle(ConsulProvider provider, string key) : ILockHandle
{
    public Task<ILease?> TryAcquireAsync(LeaseOptions? options = null, CancellationToken cancellationToken = default)
        => AcquireCoreAsync(options ?? provider.Defaults.Lease, Stopwatch.GetTimestamp(), null, trying: true, cancellationToken);

    public Task<ILease> AcquireAsync(TimeSpan wait, LeaseOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (wait <= TimeSpan.Zero || wait > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(wait));
        var policy = options ?? provider.Defaults.Lease;
        policy.Validate();
        return WaitCoreAsync(wait, policy, cancellationToken);
    }

    private async Task<ILease> WaitCoreAsync(TimeSpan wait, LeaseOptions options, CancellationToken ct)
    {
        var start = Stopwatch.GetTimestamp();
        while (true)
        {
            var remaining = wait - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "Consul contention wait exhausted");
            var result = await AcquireCoreAsync(options, start, wait, trying: false, ct).ConfigureAwait(false);
            if (result is not null) return result;
            remaining = wait - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "Consul contention wait exhausted");
            await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(remaining.TotalMilliseconds, Random.Shared.Next(50, 150))), ct).ConfigureAwait(false);
        }
    }

    private async Task<ILease?> AcquireCoreAsync(LeaseOptions options, long started, TimeSpan? waitLeft,
        bool trying, CancellationToken ct)
    {
        options.Validate();
        var ttl = ConsulProvider.TtlSeconds(options);
        if (provider.Closed) throw new ObjectDisposedException(nameof(ConsulProvider));
        TimeSpan Budget() => waitLeft is null ? options.RpcTimeout
            : Min(options.RpcTimeout, waitLeft.Value - Stopwatch.GetElapsedTime(started));
        var budget = Budget();
        if (budget <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "Consul contention wait exhausted");
        await provider.ValidateMetadataAsync(budget, ct).ConfigureAwait(false);
        if (options.RequireFencing == false) { /* LockIndex is available even when not required. */ }
        budget = Budget();
        var created = await provider.CreateSessionAsync(ttl, budget, ct).ConfigureAwait(false);
        var sessionDeadline = created.Sent + (long)(ConsulProvider.Validity(ttl).TotalSeconds * Stopwatch.Frequency);
        var nextRenew = Stopwatch.GetTimestamp() + (long)(ttl * Stopwatch.Frequency / 3d);
        try
        {
            while (true)
            {
                var now = Stopwatch.GetTimestamp();
                var remaining = Math.Min(waitLeft is null ? long.MaxValue : (long)(Budget().TotalSeconds * Stopwatch.Frequency), sessionDeadline - now);
                if (waitLeft is not null && Budget() <= TimeSpan.Zero)
                    throw new DilockException(FailureKind.WaitTimeout, "Consul contention wait exhausted");
                if (sessionDeadline <= now) throw new DilockException(FailureKind.BackendUnavailable, "Consul attempt session expired");
                var acquired = await provider.AcquireKeyAsync(key, created.Id, Min(options.RpcTimeout, TimeSpan.FromSeconds(Math.Max(.001, remaining / (double)Stopwatch.Frequency))), ct).ConfigureAwait(false);
                if (acquired)
                {
                    var owner = await provider.OwnerAsync(key, options.RpcTimeout, ct).ConfigureAwait(false);
                    if (owner is null || owner.Value.Session != created.Id || owner.Value.Index <= 0)
                        throw new DilockException(FailureKind.AcquisitionUnknown, "Consul could not confirm lock ownership");
                    if (Stopwatch.GetTimestamp() >= Math.Min(sessionDeadline, waitLeft is null ? long.MaxValue : started + (long)(waitLeft.Value.TotalSeconds * Stopwatch.Frequency)))
                        throw new DilockException(FailureKind.AcquisitionUnknown, "Consul grant arrived beyond its conservative deadline");
                    var lease = new ConsulLease(provider, key, created.Id, owner.Value.Index, options, ttl, sessionDeadline);
                    provider.Register(lease);
                    return lease;
                }
                if (trying)
                {
                    await CleanupSessionAsync(provider, created.Id, options.CleanupTimeout).ConfigureAwait(false);
                    return null;
                }
                now = Stopwatch.GetTimestamp();
                if (now >= nextRenew)
                {
                    var renewed = await provider.RenewSessionAsync(created.Id, options.RpcTimeout, ct).ConfigureAwait(false);
                    if (renewed is null) throw new DilockException(FailureKind.BackendUnavailable, "Consul waiter session expired");
                    sessionDeadline = renewed.Value + (long)(ConsulProvider.Validity(ttl).TotalSeconds * Stopwatch.Frequency);
                    nextRenew = Stopwatch.GetTimestamp() + (long)(ttl * Stopwatch.Frequency / 3d);
                }
                var pauseTicks = Math.Min(nextRenew - Stopwatch.GetTimestamp(), (long)(.25 * Stopwatch.Frequency));
                await Task.Delay(TimeSpan.FromSeconds(Math.Max(.02, pauseTicks / (double)Stopwatch.Frequency)), ct).ConfigureAwait(false);
            }
        }
        catch
        {
            var cleanup = CleanupSessionAsync(provider, created.Id, options.CleanupTimeout);
            provider.Track(cleanup);
            try { await cleanup.WaitAsync(options.CleanupTimeout).ConfigureAwait(false); }
            catch (Exception) { }
            throw;
        }
    }

    private static async Task CleanupSessionAsync(ConsulProvider provider, string session, TimeSpan budget)
    {
        try { await provider.DestroySessionAsync(session, budget).ConfigureAwait(false); }
        catch (Exception) { }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}

internal sealed class ConsulLease : IConsulLease
{
    private readonly ConsulProvider provider;
    private readonly string key, session;
    private readonly LeaseOptions options;
    private readonly int ttl;
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly CancellationTokenSource lost = new(), stop = new();
    private readonly Task keeper;
    private long deadline;
    private int state = (int)LeaseState.Held;
    private ReleaseOutcome? outcome;
    public ConsulLease(ConsulProvider provider, string key, string session, long lockIndex,
        LeaseOptions options, int ttl, long deadline)
    {
        this.provider = provider; this.key = key; this.session = session; LockIndex = lockIndex;
        this.options = options; this.ttl = ttl; this.deadline = deadline;
        keeper = options.AutoRenew ? KeepAliveAsync() : Task.CompletedTask;
    }
    public string Epoch => provider.Epoch;
    public long LockIndex { get; }
    public long EffectiveLeaseMilliseconds => ttl * 1000L;
    public CancellationToken Lost => lost.Token;
    public LeaseState State
    {
        get
        {
            if (Volatile.Read(ref state) == (int)LeaseState.Held && Stopwatch.GetTimestamp() >= Volatile.Read(ref deadline)) Lose();
            return (LeaseState)Volatile.Read(ref state);
        }
    }
    private void Lose()
    {
        if (Interlocked.CompareExchange(ref state, (int)LeaseState.Lost, (int)LeaseState.Held) == (int)LeaseState.Held)
            try { lost.Cancel(); } catch (ObjectDisposedException) { }
    }
    public async Task<bool> RenewAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (State != LeaseState.Held) return false;
            var prior = Volatile.Read(ref deadline);
            var renewed = await provider.RenewSessionAsync(session, options.RpcTimeout, cancellationToken).ConfigureAwait(false);
            if (renewed is null) { Lose(); return false; }
            var owner = await provider.OwnerAsync(key, options.RpcTimeout, cancellationToken).ConfigureAwait(false);
            if (owner is null || owner.Value.Session != session || owner.Value.Index != LockIndex
                || Stopwatch.GetTimestamp() >= prior)
            { Lose(); return false; }
            var next = renewed.Value + (long)(ConsulProvider.Validity(ttl).TotalSeconds * Stopwatch.Frequency);
            if (Stopwatch.GetTimestamp() >= next) { Lose(); return false; }
            Volatile.Write(ref deadline, next);
            return State == LeaseState.Held;
        }
        catch
        {
            if (Stopwatch.GetTimestamp() >= Volatile.Read(ref deadline)) Lose();
            throw;
        }
        finally { gate.Release(); }
    }
    private async Task KeepAliveAsync()
    {
        try
        {
            while (State == LeaseState.Held)
            {
                await Task.Delay(TimeSpan.FromSeconds(ttl / 3d), stop.Token).ConfigureAwait(false);
                if (!await RenewAsync(stop.Token).ConfigureAwait(false)) return;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch (Exception) { Lose(); }
    }
    public async Task<ReleaseOutcome> ReleaseAsync()
    {
        stop.Cancel();
        try { await keeper.ConfigureAwait(false); } catch (Exception) { }
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (outcome.HasValue) return outcome.Value;
            Interlocked.Exchange(ref state, (int)LeaseState.Releasing);
            var started = Stopwatch.GetTimestamp();
            var released = false;
            var result = ReleaseOutcome.Unknown;
            try
            {
                released = await provider.ReleaseKeyAsync(key, session, options.CleanupTimeout).ConfigureAwait(false);
                if (released) result = ReleaseOutcome.Released;
                else
                {
                    var remaining = options.CleanupTimeout - Stopwatch.GetElapsedTime(started);
                    if (remaining > TimeSpan.Zero)
                    {
                        var owner = await provider.OwnerAsync(key, remaining, CancellationToken.None).ConfigureAwait(false);
                        if (owner is null || owner.Value.Session != session) result = ReleaseOutcome.AlreadyNotOwned;
                    }
                }
            }
            catch (Exception) { }
            try
            {
                var destroyStarted = Stopwatch.GetTimestamp();
                var remaining = options.CleanupTimeout - Stopwatch.GetElapsedTime(destroyStarted);
                if (remaining > TimeSpan.Zero) await provider.DestroySessionAsync(session, remaining).ConfigureAwait(false);
            }
            catch (Exception) { }
            outcome = result;
            Interlocked.Exchange(ref state, (int)(result == ReleaseOutcome.Unknown ? LeaseState.ReleasedUnknown : LeaseState.Released));
            provider.Unregister(this);
            return result;
        }
        finally { gate.Release(); }
    }
    public async ValueTask DisposeAsync()
    {
        ReleaseOutcome result;
        try { result = await ReleaseAsync().ConfigureAwait(false); }
        catch (Exception) { result = ReleaseOutcome.Unknown; }
        provider.Observe(result, "lease-dispose");
    }
    internal async Task ReleaseFromProviderAsync()
    {
        ReleaseOutcome result;
        try { result = await ReleaseAsync().ConfigureAwait(false); }
        catch (Exception) { result = ReleaseOutcome.Unknown; }
        provider.Observe(result, "provider-dispose");
    }
}
