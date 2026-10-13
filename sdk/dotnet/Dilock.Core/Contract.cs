using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Dilock;

/// <summary>Immutable per-acquisition timing and capability options.</summary>
/// <remarks>
/// <see cref="LeaseDuration"/> is the requested server TTL. The SDK subtracts a conservative
/// local validity margin when deciding whether it may still rely on a lease. RPC and cleanup
/// budgets are independent; cleanup does not reuse an acquisition cancellation token. A backend
/// may round the effective TTL, which is reported by <see cref="ILease.EffectiveLeaseMilliseconds"/>.
/// </remarks>
public sealed record LeaseOptions
{
    /// <summary>Requested server TTL (100 milliseconds through one day).</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Maximum acknowledgement budget for one backend command; not the total acquisition wait.</summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Independent bounded budget for owner cleanup after release, cancellation, or ambiguity.</summary>
    public TimeSpan CleanupTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>Whether the provider renews an acquired lease until release or loss; disabled by default.</summary>
    public bool AutoRenew { get; init; }
    /// <summary>Require a backend with an epoch-scoped monotonic fence; unsupported backends fail explicitly.</summary>
    public bool RequireFencing { get; init; }
    /// <summary>Requested TTL rounded down to whole milliseconds for backend calls.</summary>
    public long TtlMilliseconds => (long)LeaseDuration.TotalMilliseconds;
    /// <summary>Conservative local drift allowance; it cannot protect a suspended process from stale work.</summary>
    public TimeSpan Margin => TimeSpan.FromMilliseconds(Math.Max(10, TtlMilliseconds * .01));
    /// <summary>Validates all timing constraints locally before backend I/O.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A duration is outside the supported range or leaves no conservative RPC budget.</exception>
    public void Validate()
    {
        if (LeaseDuration < TimeSpan.FromMilliseconds(100) || LeaseDuration > TimeSpan.FromDays(1)
            || RpcTimeout <= TimeSpan.Zero || CleanupTimeout <= TimeSpan.Zero
            || RpcTimeout >= LeaseDuration - Margin || CleanupTimeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(LeaseDuration), "Invalid lease/RPC/cleanup budgets");
    }
}

/// <summary>Immutable policy for one acquisition, including the total wait budget.</summary>
/// <remarks>Omitted values use a 5 second wait and the shared 30 second lease policy. This policy is snapshotted when supplied.</remarks>
public sealed record AcquireOptions
{
    /// <summary>Total contention and attempt budget.</summary>
    public TimeSpan WaitTimeout { get; init; } = TimeSpan.FromSeconds(5);
    /// <summary>Lease TTL, per-command RPC budget, cleanup budget and capability requests.</summary>
    public LeaseOptions Lease { get; init; } = new();

    /// <summary>Rejects invalid acquisition or lease budgets before backend access.</summary>
    public void Validate()
    {
        if (WaitTimeout <= TimeSpan.Zero || WaitTimeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(WaitTimeout), "Wait timeout must be positive and at most one day");
        ArgumentNullException.ThrowIfNull(Lease);
        Lease.Validate();
    }
}

/// <summary>Local states. Terminal states never return to Held.</summary>
public enum LeaseState
{
    /// <summary>Conservative permission remains.</summary>
    Held,
    /// <summary>Permission withdrawn; cleanup in progress.</summary>
    Releasing,
    /// <summary>Cleanup acknowledged.</summary>
    Released,
    /// <summary>Cleanup was not acknowledged.</summary>
    ReleasedUnknown,
    /// <summary>Ownership or local validity was lost.</summary>
    Lost
}
/// <summary>Owner-specific remote cleanup acknowledgement.</summary>
public enum ReleaseOutcome
{
    /// <summary>This owner's key was removed.</summary>
    Released,
    /// <summary>No key belonging to this owner remains.</summary>
    AlreadyNotOwned,
    /// <summary>No reliable cleanup acknowledgement.</summary>
    Unknown
}
/// <summary>Typed failure categories; only Busy returns null.</summary>
public enum FailureKind
{
    /// <summary>Dispatched acquisition lacks ownership evidence.</summary>
    AcquisitionUnknown,
    /// <summary>Transport unavailable or operation rejected.</summary>
    BackendUnavailable,
    /// <summary>Metadata or reply violates the profile.</summary>
    ProtocolMismatch,
    /// <summary>Backend cannot satisfy a requirement.</summary>
    UnsupportedCapability,
    /// <summary>Local reliance ended.</summary>
    LeaseLost,
    /// <summary>Contention wait budget exhausted.</summary>
    WaitTimeout
}
/// <summary>A distributed-lock operation failed for a reason other than acknowledged contention.</summary>
/// <remarks>
/// Inspect <see cref="Kind"/> for stable handling. Only <see cref="FailureKind.WaitTimeout"/> means an
/// acquisition contention budget elapsed; backend errors, capability failures, protocol mismatches,
/// lease loss, and ambiguous acquisition are distinct failures. An inner exception is retained when available.
/// </remarks>
public sealed class DilockException(FailureKind kind, string message, Exception? inner = null) : Exception(message, inner)
{
    /// <summary>Stable category for error handling. This is not a retry recommendation.</summary>
    public FailureKind Kind { get; } = kind;
}
/// <summary>A reusable handle for one named distributed resource.</summary>
/// <remarks>
/// A handle does not own the provider or a lease. Concurrent acquisition calls create independent
/// ownership generations; a handle is not a process-local mutex. Implementations are safe for concurrent calls.
/// Dispose the provider, not the handle, to close backend resources.
/// </remarks>
public interface ILockHandle
{
    /// <summary>Attempts one acquisition without waiting for contention to clear.</summary>
    /// <remarks>
    /// Returns <see langword="null"/> only when the backend acknowledged that another owner holds the
    /// resource. A backend without a non-blocking primitive may throw <see cref="DilockException"/> with
    /// <see cref="FailureKind.UnsupportedCapability"/>. Once a mutation is dispatched, loss of its
    /// acknowledgement is uncertain acquisition, never contention. See the backend API docs for its
    /// precise cancellation cleanup behavior.
    /// </remarks>
    /// <param name="options">Immutable timing and renewal settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels this attempt. Cancellation does not cancel the independent cleanup budget.</param>
    /// <returns>The acquired lease, or <see langword="null"/> for acknowledged contention only.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The options contain invalid timing budgets.</exception>
    /// <exception cref="DilockException">A backend, protocol, capability, lease, or ambiguous-outcome error occurred.</exception>
    /// <exception cref="OperationCanceledException">The operation was cancelled before dispatch, or after cleanup was confirmed.</exception>
    Task<ILease?> TryAcquireAsync(LeaseOptions? options = null, CancellationToken cancellationToken = default);
    /// <summary>Waits for the resource within a total contention budget.</summary>
    /// <remarks>
    /// The wait includes backend attempts and contention delays. Only acknowledged contention is
    /// retried; backend failures, ambiguous outcomes, and cancellation are not retried. The backend
    /// may have weaker support or different cleanup details, which are documented by its provider.
    /// </remarks>
    /// <param name="wait">Total maximum time to wait for an acquisition, including attempts and delays.</param>
    /// <param name="options">Immutable lease timing and renewal settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels this wait and acquisition attempt; cleanup uses an independent bounded budget.</param>
    /// <returns>A newly acquired lease.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="wait"/> or an option is outside its supported range.</exception>
    /// <exception cref="DilockException">Contention exhausted the wait budget, or a backend/protocol/capability/ambiguous-outcome error occurred.</exception>
    /// <exception cref="OperationCanceledException">Cancellation completed with confirmed cleanup; an unconfirmed dispatched acquisition is reported as <see cref="FailureKind.AcquisitionUnknown"/>.</exception>
    Task<ILease> AcquireAsync(TimeSpan wait, LeaseOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Acquires with this provider's common wait and lease defaults.</summary>
    Task<ILease> AcquireAsync(CancellationToken cancellationToken = default)
        => AcquireAsync(new AcquireOptions(), cancellationToken);

    /// <summary>Acquires with a complete immutable per-acquisition policy.</summary>
    Task<ILease> AcquireAsync(AcquireOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return AcquireAsync(options.WaitTimeout, options.Lease, cancellationToken);
    }
}

/// <summary>Non-released cleanup observation emitted by best-effort scope cleanup.</summary>
/// <param name="Backend">Backend profile, without endpoint or credentials.</param>
/// <param name="Outcome">Acknowledged, already absent, or unknown owner cleanup.</param>
/// <param name="Reason">Whether cleanup came from lease or provider scope exit.</param>
public sealed record CleanupObservation(string Backend, ReleaseOutcome Outcome, string Reason);
/// <summary>One acquisition generation for a distributed resource.</summary>
/// <remarks>
/// The lease is owned by the provider that created it; both should be disposed with
/// <see langword="await using"/>. Lease operations are safe for concurrent callers and mutations are
/// serialized. A lost lease withdraws local permission but cannot stop work already running in this process
/// or elsewhere. A lease is not a local mutex and does not guarantee exactly-once effects.
/// </remarks>
public interface ILease : IAsyncDisposable
{
    /// <summary>Current conservative local observation; reading it is not a remote ownership check.</summary>
    LeaseState State { get; }
    /// <summary>A token cancelled once when local reliance is withdrawn; it never stops protected work automatically.</summary>
    CancellationToken Lost { get; }
    /// <summary>Effective server TTL in milliseconds, not remaining validity.</summary>
    long EffectiveLeaseMilliseconds { get; }
    /// <summary>Explicit owner-checked renewal; renewal never reacquires ownership after loss.</summary>
    /// <param name="cancellationToken">Cancels the renewal request; cancellation or transport failure may make the lease lost.</param>
    /// <returns><see langword="true"/> only when renewal is acknowledged and the local lease remains valid.</returns>
    /// <exception cref="DilockException">The backend or protocol rejected the renewal.</exception>
    Task<bool> RenewAsync(CancellationToken cancellationToken = default);
    /// <summary>Releases this acquisition with the provider's independent bounded cleanup budget.</summary>
    /// <remarks>Repeated calls share the first release operation and return its cached outcome. DisposeAsync delegates to this operation.</remarks>
    /// <returns>Whether cleanup was acknowledged, was already unnecessary, or remains unknown.</returns>
    Task<ReleaseOutcome> ReleaseAsync();
}
/// <summary>Owns one backend authority and every lease created through it.</summary>
/// <remarks>
/// Providers own their backend connections and must be disposed with <see langword="await using"/>.
/// Disposing withdraws local reliance from active leases and attempts their bounded cleanup. Providers
/// do not share or switch coordinators implicitly.
/// </remarks>
public interface ILockProvider : IAsyncDisposable
{
    /// <summary>Creates a reusable handle after validating the resource identity locally.</summary>
    /// <param name="resource">Case-sensitive resource name; the identity scheme performs no Unicode normalization.</param>
    /// <returns>A handle bound to this provider and resource.</returns>
    /// <exception cref="ArgumentException">The resource or configured namespace is invalid.</exception>
    /// <exception cref="ObjectDisposedException">The provider has already been disposed.</exception>
    ILockHandle Lock(string resource);
}

/// <summary>Draft 1 strict UTF-8 resource identity; no Unicode normalization or culture-sensitive casing is applied.</summary>
public static class ResourceIdentity
{
    /// <summary>Returns the lowercase SHA-256 digest of the protocol's length-framed namespace and resource bytes.</summary>
    /// <remarks>The result is a stable identity only for the shared draft-1 scheme; it is not an ownership token or fencing value.</remarks>
    /// <param name="ns">A lowercase ASCII namespace identifier from the protocol's restricted alphabet.</param>
    /// <param name="resource">A nonempty resource with at most 1,024 strict UTF-8 bytes and no NUL character.</param>
    /// <returns>A 64-character lowercase hexadecimal SHA-256 digest.</returns>
    /// <exception cref="ArgumentException">The namespace/resource violates the protocol identity constraints.</exception>
    public static string Hash(string ns, string resource)
    {
        if (!Regex.IsMatch(ns, "\\A[a-z0-9][a-z0-9_.-]{0,63}\\z", RegexOptions.CultureInvariant))
            throw new ArgumentException("Invalid namespace", nameof(ns));
        if (resource.Contains('\0')) throw new ArgumentException("NUL resource", nameof(resource));
        byte[] data;
        try { data = new UTF8Encoding(false, true).GetBytes(resource); }
        catch (EncoderFallbackException e) { throw new ArgumentException("Invalid Unicode scalar", nameof(resource), e); }
        if (data.Length is < 1 or > 1024) throw new ArgumentException("Resource requires 1..1024 UTF-8 bytes", nameof(resource));
        var namespaceBytes = Encoding.ASCII.GetBytes(ns);
        using var bytes = new MemoryStream();
        bytes.Write("dilock.lock.identity\0"u8);
        Span<byte> frame = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)namespaceBytes.Length); bytes.Write(frame); bytes.Write(namespaceBytes);
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)data.Length); bytes.Write(frame); bytes.Write(data);
        return Convert.ToHexString(SHA256.HashData(bytes.ToArray())).ToLowerInvariant();
    }
}
