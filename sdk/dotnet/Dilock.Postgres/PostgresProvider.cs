using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Dilock;
using Npgsql;

namespace Dilock.Postgres;

/// <summary>Configuration for one PostgreSQL database and provisioned Dilock namespace.</summary>
public sealed record PostgresProviderOptions
{
    /// <summary>PostgreSQL connection string for one coordination database.</summary>
    public required string ConnectionString { get; init; }
    /// <summary>Provisioned Dilock namespace; defaults to <c>default</c>.</summary>
    public string Namespace { get; init; } = "default";
    /// <summary>Deployment-approved coordinator epoch already stored in metadata.</summary>
    public required string ExpectedEpoch { get; init; }
    /// <summary>Budget for one connection or metadata query.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Default wait and capability policy.</summary>
    public AcquireOptions DefaultAcquisition { get; init; } = new();
    /// <summary>Optional diagnostic callback for unconfirmed scope cleanup.</summary>
    public Action<CleanupObservation>? CleanupObserver { get; init; }
}

/// <summary>PostgreSQL session-advisory provider. It owns no pooled connections.</summary>
public sealed class PostgresProvider : ILockProvider
{
    internal const string Profile = "postgres-advisory-session-v1";
    internal const string Scheme = "sha256-framed-utf8-v1";
    private const string MetadataSql = "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms "
        + "FROM dilock_namespace_meta WHERE namespace = @namespace";
    private readonly string connectionString;
    private readonly string ns;
    private readonly string epoch;
    private readonly TimeSpan connectTimeout;
    private readonly Action<CleanupObservation>? cleanupObserver;
    private readonly AcquireOptions defaults;
    private readonly ConcurrentDictionary<PostgresLease, byte> leases = new();
    private int closed;

    private PostgresProvider(PostgresProviderOptions options, string connectionString)
    {
        this.connectionString = connectionString;
        ns = options.Namespace;
        epoch = options.ExpectedEpoch;
        connectTimeout = options.ConnectTimeout;
        defaults = options.DefaultAcquisition with { Lease = options.DefaultAcquisition.Lease with { } };
        cleanupObserver = options.CleanupObserver;
    }

    /// <summary>Connects and validates the operator-provisioned metadata row.</summary>
    public static async Task<PostgresProvider> ConnectAsync(
        PostgresProviderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = ResourceIdentity.Hash(options.Namespace, "validation");
        if (string.IsNullOrWhiteSpace(options.ExpectedEpoch)
            || options.ExpectedEpoch.Contains('\0')
            || Encoding.UTF8.GetByteCount(options.ExpectedEpoch) > 1024)
            throw new ArgumentException("Invalid coordination epoch", nameof(options));
        if (options.ConnectTimeout <= TimeSpan.Zero || options.ConnectTimeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options), "Connect timeout must be positive and at most one day");
        ArgumentNullException.ThrowIfNull(options.DefaultAcquisition);
        options.DefaultAcquisition.Validate();
        var connectionString = NormalizeConnectionString(options.ConnectionString);
        var provider = new PostgresProvider(options, connectionString);
        await provider.ValidateMetadataAsync(options.ConnectTimeout, cancellationToken).ConfigureAwait(false);
        return provider;
    }

    /// <summary>Connects to the default namespace using the approved epoch.</summary>
    public static Task<PostgresProvider> ConnectAsync(
        string connectionString, string expectedEpoch, CancellationToken cancellationToken = default)
        => ConnectAsync(new PostgresProviderOptions
        {
            ConnectionString = connectionString,
            ExpectedEpoch = expectedEpoch,
        }, cancellationToken);

    /// <summary>Connects to a previously provisioned namespace.</summary>
    public static Task<PostgresProvider> ConnectAsync(
        string connectionString, string nameSpace, string expectedEpoch,
        CancellationToken cancellationToken = default)
        => ConnectAsync(new PostgresProviderOptions
        {
            ConnectionString = connectionString,
            Namespace = nameSpace,
            ExpectedEpoch = expectedEpoch,
        }, cancellationToken);

    /// <summary>Creates the metadata table/row only during explicit setup.</summary>
    public static async Task<string> ProvisionNamespaceAsync(
        PostgresProviderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        _ = ResourceIdentity.Hash(options.Namespace, "validation");
        if (string.IsNullOrWhiteSpace(options.ExpectedEpoch)
            || options.ExpectedEpoch.Contains('\0')
            || Encoding.UTF8.GetByteCount(options.ExpectedEpoch) > 1024)
            throw new ArgumentException("Invalid coordination epoch", nameof(options));
        var connectionString = NormalizeConnectionString(options.ConnectionString);
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using (var serialize = connection.CreateCommand())
            {
                serialize.Transaction = transaction;
                serialize.CommandText = "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))";
                await serialize.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            await using (var create = connection.CreateCommand())
            {
                create.Transaction = transaction;
                create.CommandText = "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)";
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (@namespace, 1, @scheme, @profile, @epoch, NULL) ON CONFLICT (namespace) DO NOTHING";
            insert.Parameters.AddWithValue("namespace", options.Namespace);
            insert.Parameters.AddWithValue("scheme", Scheme);
            insert.Parameters.AddWithValue("profile", Profile);
            insert.Parameters.AddWithValue("epoch", options.ExpectedEpoch);
            var rows = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            var provider = new PostgresProvider(options, connectionString);
            await provider.DisposeAsync().ConfigureAwait(false);
            return rows == 1 ? "created" : "compatible-existing";
        }
        catch
        {
            try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
            catch { /* Preserve the provisioning failure. */ }
            throw;
        }
    }

    /// <summary>Creates a resource handle after validating its identity locally.</summary>
    public ILockHandle Lock(string resource)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
        return new PostgresHandle(this, AdvisoryKey(ResourceIdentity.Hash(ns, resource)));
    }

    internal bool Closed => Volatile.Read(ref closed) != 0;
    internal AcquireOptions Defaults => defaults;
    internal TimeSpan ConnectTimeout => connectTimeout;

    internal async Task<NpgsqlConnection> OpenAsync(TimeSpan budget, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Closed, this);
        var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).WaitAsync(budget, cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch (OperationCanceledException) { await DisposeConnectionQuietlyAsync(connection).ConfigureAwait(false); throw; }
        catch (Exception e)
        {
            await DisposeConnectionQuietlyAsync(connection).ConfigureAwait(false);
            throw new DilockException(FailureKind.BackendUnavailable, "PostgreSQL connection failed", e);
        }
    }

    private static async ValueTask DisposeConnectionQuietlyAsync(NpgsqlConnection connection)
    {
        try { await connection.DisposeAsync().ConfigureAwait(false); }
        catch { /* Preserve the primary connection or cancellation failure. */ }
    }

    internal async Task<object?> ExecuteAsync(NpgsqlConnection connection, string sql,
        TimeSpan budget, CancellationToken cancellationToken, long? key = null)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (sql.Contains("@namespace", StringComparison.OrdinalIgnoreCase))
            command.Parameters.AddWithValue("namespace", ns);
        if (key.HasValue) command.Parameters.AddWithValue("key", key.Value);
        try
        {
            return await command.ExecuteScalarAsync(cancellationToken)
                .WaitAsync(budget, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception e)
        {
            throw new DilockException(FailureKind.BackendUnavailable, "PostgreSQL query failed", e);
        }
    }

    internal async Task ValidateMetadataAsync(TimeSpan budget, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(budget, cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = MetadataSql;
        command.Parameters.AddWithValue("namespace", ns);
        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken)
                .WaitAsync(budget, cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new DilockException(FailureKind.ProtocolMismatch, "Missing PostgreSQL namespace metadata");
            if (reader.GetInt32(0) != 1
                || reader.GetString(1) != Scheme
                || reader.GetString(2) != Profile
                || reader.GetString(3) != epoch
                || !reader.IsDBNull(4))
                throw new DilockException(FailureKind.ProtocolMismatch, "Incompatible PostgreSQL namespace metadata");
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new DilockException(FailureKind.ProtocolMismatch, "Duplicate PostgreSQL namespace metadata");
        }
        catch (DilockException) { throw; }
        catch (OperationCanceledException) { throw; }
        catch (Exception e)
        {
            throw new DilockException(FailureKind.BackendUnavailable, "PostgreSQL metadata query failed", e);
        }
    }

    internal void Register(PostgresLease lease) => leases.TryAdd(lease, 0);
    internal void Unregister(PostgresLease lease) => leases.TryRemove(lease, out _);
    internal void Observe(ReleaseOutcome outcome, string reason)
    {
        if (outcome == ReleaseOutcome.Released || cleanupObserver is null) return;
        try { cleanupObserver(new CleanupObservation(Profile, outcome, reason)); }
        catch (Exception) { }
    }

    /// <summary>Converts the first 64 identity bits to PostgreSQL's signed bigint key.</summary>
    internal static long AdvisoryKey(string resourceId)
    {
        if (resourceId.Length != 64) throw new ArgumentException("Invalid resource identity", nameof(resourceId));
        ulong value = Convert.ToUInt64(resourceId[..16], 16);
        return unchecked((long)value);
    }

    private static string NormalizeConnectionString(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("Connection string is required", nameof(value));
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(value)
            {
                Pooling = false,
                Multiplexing = false,
            };
            return builder.ConnectionString;
        }
        catch (Exception e) { throw new ArgumentException("Invalid PostgreSQL connection string", nameof(value), e); }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        foreach (var lease in leases.Keys) await lease.ReleaseFromProviderAsync().ConfigureAwait(false);
    }
}

internal sealed class PostgresHandle(PostgresProvider provider, long key) : ILockHandle
{
    public Task<ILease?> TryAcquireAsync(LeaseOptions? options = null, CancellationToken cancellationToken = default)
        => AcquireCoreAsync(options ?? provider.Defaults.Lease, trying: true, Stopwatch.GetTimestamp(), null, cancellationToken);

    public async Task<ILease> AcquireAsync(TimeSpan wait, LeaseOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (wait <= TimeSpan.Zero || wait > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(wait));
        var selected = options ?? provider.Defaults.Lease;
        selected.Validate();
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            var remaining = wait - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "PostgreSQL contention wait exhausted");
            var result = await AcquireCoreAsync(selected, false, started, remaining, cancellationToken).ConfigureAwait(false);
            if (result is not null) return result;
            remaining = wait - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "PostgreSQL contention wait exhausted");
            await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(50, remaining.TotalMilliseconds)), cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<ILease?> AcquireCoreAsync(LeaseOptions options, bool trying, long started,
        TimeSpan? waitLeft, CancellationToken cancellationToken)
    {
        options.Validate();
        if (options.RequireFencing)
            throw new DilockException(FailureKind.UnsupportedCapability, "PostgreSQL advisory locks do not provide fencing");
        if (options.AutoRenew)
            throw new DilockException(FailureKind.UnsupportedCapability, "PostgreSQL advisory locks have no TTL to renew");
        if (provider.Closed) throw new ObjectDisposedException(nameof(PostgresProvider));
        TimeSpan Budget() => waitLeft is null ? options.RpcTimeout
            : Min(options.RpcTimeout, waitLeft.Value - Stopwatch.GetElapsedTime(started));
        var budget = Budget();
        if (budget <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "PostgreSQL contention wait exhausted");
        await provider.ValidateMetadataAsync(budget, cancellationToken).ConfigureAwait(false);
        budget = Budget();
        if (budget <= TimeSpan.Zero) throw new DilockException(FailureKind.WaitTimeout, "PostgreSQL contention wait exhausted");
        var connection = await provider.OpenAsync(budget, cancellationToken).ConfigureAwait(false);
        try
        {
            var value = await provider.ExecuteAsync(connection,
                "SELECT pg_try_advisory_lock(@key::bigint)", budget, cancellationToken, key).ConfigureAwait(false);
            if (value is not bool acquired)
                throw new DilockException(FailureKind.AcquisitionUnknown, "PostgreSQL advisory-lock response was malformed");
            if (!acquired)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                return null;
            }
            var lease = new PostgresLease(provider, connection, key, options);
            provider.Register(lease);
            return lease;
        }
        catch (DilockException e) when (e.Kind == FailureKind.BackendUnavailable)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw new DilockException(FailureKind.AcquisitionUnknown, "PostgreSQL advisory-lock acknowledgement is unknown", e);
        }
        catch (OperationCanceledException e)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw new DilockException(FailureKind.AcquisitionUnknown, "PostgreSQL advisory-lock acknowledgement is unknown", e);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static TimeSpan Min(TimeSpan first, TimeSpan second) => first < second ? first : second;
}

internal sealed class PostgresLease : ILease
{
    private readonly PostgresProvider provider;
    private readonly NpgsqlConnection connection;
    private readonly long key;
    private readonly LeaseOptions options;
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly CancellationTokenSource lost = new();
    private int state = (int)LeaseState.Held;
    private ReleaseOutcome? outcome;

    internal PostgresLease(PostgresProvider provider, NpgsqlConnection connection, long key, LeaseOptions options)
    { this.provider = provider; this.connection = connection; this.key = key; this.options = options; }

    public LeaseState State => (LeaseState)Volatile.Read(ref state);
    public CancellationToken Lost => lost.Token;
    public long EffectiveLeaseMilliseconds => 0;

    public async Task<bool> RenewAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (State != LeaseState.Held) return false;
            try
            {
                var value = await provider.ExecuteAsync(connection, "SELECT 1", options.RpcTimeout,
                    cancellationToken).ConfigureAwait(false);
                if (value is not int result || result != 1) throw new InvalidOperationException("Invalid liveness response");
                return true;
            }
            catch (OperationCanceledException) { Lose(); throw; }
            catch (Exception e) { Lose(); throw new DilockException(FailureKind.BackendUnavailable, "PostgreSQL session liveness failed", e); }
        }
        finally { gate.Release(); }
    }

    public async Task<ReleaseOutcome> ReleaseAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (outcome.HasValue) return outcome.Value;
            Interlocked.Exchange(ref state, (int)LeaseState.Releasing);
            var result = ReleaseOutcome.Unknown;
            try
            {
                var value = await provider.ExecuteAsync(connection,
                    "SELECT pg_advisory_unlock(@key::bigint)", options.CleanupTimeout,
                    CancellationToken.None, key).ConfigureAwait(false);
                if (value is bool released)
                    result = released ? ReleaseOutcome.Released : ReleaseOutcome.AlreadyNotOwned;
            }
            catch (Exception) { }
            try { await connection.DisposeAsync().ConfigureAwait(false); }
            catch (Exception) { result = ReleaseOutcome.Unknown; }
            outcome = result;
            Interlocked.Exchange(ref state, (int)(result == ReleaseOutcome.Unknown ? LeaseState.ReleasedUnknown : LeaseState.Released));
            provider.Unregister(this);
            return result;
        }
        finally { gate.Release(); }
    }

    private void Lose()
    {
        if (Interlocked.CompareExchange(ref state, (int)LeaseState.Lost, (int)LeaseState.Held) == (int)LeaseState.Held)
            try { lost.Cancel(); } catch (ObjectDisposedException) { }
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
