// Local-only style example. No backend, TTL, renewal or network uncertainty.
namespace Dilock.Style;

public enum ReleaseOutcome { Released, AlreadyNotOwned }

/// <summary>A reusable resource reference, not an acquired lease.</summary>
public interface IResourceLock
{
    Task<Lease?> TryAcquireAsync(CancellationToken cancellationToken = default);
}

/// <summary>One local acquisition. Remote cleanup requires a separate bounded budget.</summary>
public sealed class Lease : IAsyncDisposable
{
    private readonly MemoryProvider _provider;
    private readonly string _resource;
    private readonly object _owner;
    private int _closed;

    internal Lease(MemoryProvider provider, string resource, object owner) =>
        (_provider, _resource, _owner) = (provider, resource, owner);

    public Task<ReleaseOutcome> ReleaseAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return Task.FromResult(ReleaseOutcome.AlreadyNotOwned);
        return Task.FromResult(_provider.Release(_resource, _owner));
    }

    public async ValueTask DisposeAsync() =>
        _ = await ReleaseAsync().ConfigureAwait(false);
}

/// <summary>Thread-safe local fixture, not a production provider.</summary>
public sealed class MemoryProvider
{
    private readonly object _gate = new();
    private readonly Dictionary<string, object> _owners = new(StringComparer.Ordinal);

    public IResourceLock GetLock(string resource)
    {
        ArgumentException.ThrowIfNullOrEmpty(resource);
        return new ResourceLock(this, resource);
    }

    internal bool Claim(string resource, object owner)
    {
        lock (_gate) return _owners.TryAdd(resource, owner);
    }

    internal ReleaseOutcome Release(string resource, object owner)
    {
        lock (_gate)
        {
            if (!_owners.TryGetValue(resource, out var current) || !ReferenceEquals(current, owner))
                return ReleaseOutcome.AlreadyNotOwned;
            _owners.Remove(resource);
            return ReleaseOutcome.Released;
        }
    }

    internal void ExpireForTest(string resource)
    {
        lock (_gate) _owners.Remove(resource);
    }

    private sealed class ResourceLock(MemoryProvider provider, string resource) : IResourceLock
    {
        public Task<Lease?> TryAcquireAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var owner = new object(); // A local identity, not a wire token.
            return Task.FromResult(provider.Claim(resource, owner)
                ? new Lease(provider, resource, owner) : null);
        }
    }
}

internal static class Program
{
    private static void Check(bool condition)
    {
        if (!condition) throw new InvalidOperationException("style test failed");
    }

    private static async Task Main()
    {
        var provider = new MemoryProvider();
        var resource = provider.GetLock("example/resource");
        var first = await resource.TryAcquireAsync() ?? throw new Exception("first acquisition failed");
        Check(await resource.TryAcquireAsync() is null);
        provider.ExpireForTest("example/resource");
        var second = await resource.TryAcquireAsync() ?? throw new Exception("second acquisition failed");
        Check(await first.ReleaseAsync() == ReleaseOutcome.AlreadyNotOwned);
        Check(await resource.TryAcquireAsync() is null);
        Check(await second.ReleaseAsync() == ReleaseOutcome.Released);
        Check(await second.ReleaseAsync() == ReleaseOutcome.AlreadyNotOwned);
        try
        {
            await using var held = await resource.TryAcquireAsync() ?? throw new Exception("acquire failed");
            throw new ApplicationException("consumer failure");
        }
        catch (ApplicationException) { }
        await using var final = await resource.TryAcquireAsync();
        Check(final is not null);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        try
        {
            await provider.GetLock("another").TryAcquireAsync(cancelled.Token);
            throw new Exception("cancellation was ignored");
        }
        catch (OperationCanceledException) { }
        Console.WriteLine("PASS dotnet: busy, stale owner, idempotence, async disposal, cancellation");
    }
}
