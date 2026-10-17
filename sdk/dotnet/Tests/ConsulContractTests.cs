using System.Collections.Concurrent;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Dilock;
using Dilock.Consul;
using Xunit;

namespace Dilock.Tests;

public sealed class ConsulContractTests
{
    [ConsulIntegrationFact]
    public async Task LiveConsulContentionRenewalAndLockIndexFence()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_CONSUL_ENDPOINT")!;
        var ns = Environment.GetEnvironmentVariable("DILOCK_CONSUL_NAMESPACE")!;
        var epoch = Environment.GetEnvironmentVariable("DILOCK_CONSUL_EPOCH")!;
        await using var provider = await ConsulProvider.ConnectAsync(new ConsulProviderOptions
        {
            Endpoint = endpoint, Namespace = ns, ExpectedEpoch = epoch,
            Token = Environment.GetEnvironmentVariable("CONSUL_HTTP_TOKEN")
        });
        var handle = provider.Lock("dotnet-live/" + Guid.NewGuid().ToString("N"));
        var options = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(10), RpcTimeout = TimeSpan.FromSeconds(2) };
        var first = Assert.IsAssignableFrom<IConsulLease>(await handle.TryAcquireAsync(options));
        Assert.Null(await handle.TryAcquireAsync(options));
        Assert.True(await first.RenewAsync());
        Assert.Equal(ReleaseOutcome.Released, await first.ReleaseAsync());
        var next = Assert.IsAssignableFrom<IConsulLease>(await handle.TryAcquireAsync(options));
        Assert.True(next.LockIndex > first.LockIndex);
        Assert.Equal(ReleaseOutcome.Released, await next.ReleaseAsync());
    }

    [Fact]
    public async Task ContentionRenewReleaseAndRetainedKeyFenceUseTheSharedContract()
    {
        var handler = new ConsulWireHandler();
        await using var provider = await ConsulProvider.ConnectAsync(new ConsulProviderOptions
        {
            Endpoint = "http://consul.test:8500", Namespace = "tests", ExpectedEpoch = "epoch-1",
            HttpHandlerFactory = () => handler
        });
        var handle = provider.Lock("order/17");
        var options = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(10), RpcTimeout = TimeSpan.FromSeconds(1) };
        var first = Assert.IsAssignableFrom<IConsulLease>(await handle.TryAcquireAsync(options));
        Assert.Equal("epoch-1", first.Epoch);
        Assert.Equal(1, first.LockIndex);
        Assert.Null(await handle.TryAcquireAsync(options));
        Assert.True(await first.RenewAsync());
        Assert.Equal(ReleaseOutcome.Released, await first.ReleaseAsync());

        var next = Assert.IsAssignableFrom<IConsulLease>(await handle.TryAcquireAsync(options));
        Assert.Equal(2, next.LockIndex);
        Assert.Equal(ReleaseOutcome.Released, await next.ReleaseAsync());
    }

    [Fact]
    public async Task TenSecondServerMinimumIsRejectedBeforeHttpIo()
    {
        var handler = new ConsulWireHandler();
        await using var provider = await ConsulProvider.ConnectAsync(new ConsulProviderOptions
        {
            Endpoint = "http://consul.test:8500", Namespace = "tests", ExpectedEpoch = "epoch-1",
            HttpHandlerFactory = () => handler
        });
        var before = handler.RequestCount;
        var error = await Assert.ThrowsAsync<DilockException>(() => provider.Lock("short").TryAcquireAsync(
            new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(9), RpcTimeout = TimeSpan.FromSeconds(1) }));
        Assert.Equal(FailureKind.UnsupportedCapability, error.Kind);
        Assert.Equal(before, handler.RequestCount);
    }

    [Fact]
    public async Task UnknownMetadataFieldIsRejected()
    {
        var handler = new ConsulWireHandler(includeUnknownMetadata: true);
        var error = await Assert.ThrowsAsync<DilockException>(() => ConsulProvider.ConnectAsync(new ConsulProviderOptions
        {
            Endpoint = "http://consul.test:8500", Namespace = "tests", ExpectedEpoch = "epoch-1",
            HttpHandlerFactory = () => handler
        }));
        Assert.Equal(FailureKind.ProtocolMismatch, error.Kind);
    }

    [Fact]
    public async Task LostReleaseAcknowledgementStillDestroysAttemptSession()
    {
        var handler = new ConsulWireHandler();
        await using var provider = await ConsulProvider.ConnectAsync(new ConsulProviderOptions
        {
            Endpoint = "http://consul.test:8500", Namespace = "tests", ExpectedEpoch = "epoch-1",
            HttpHandlerFactory = () => handler
        });
        var lease = Assert.IsAssignableFrom<IConsulLease>(await provider.Lock("order/17").TryAcquireAsync(
            new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(10), RpcTimeout = TimeSpan.FromSeconds(1) }));
        handler.FailNextReleaseAcknowledgement = true;
        Assert.Equal(ReleaseOutcome.Unknown, await lease.ReleaseAsync());
        Assert.Equal(1, handler.DestroyCount);
        Assert.Equal(0, handler.SessionCount);
    }

    private sealed class ConsulWireHandler : HttpMessageHandler
    {
        private readonly bool includeUnknownMetadata;
        private readonly object gate = new();
        private readonly ConcurrentDictionary<string, byte> sessions = new();
        private string owner = "";
        private long lockIndex;
        private int sessionCount;
        private int requests;
        private int destroyCount;
        internal bool FailNextReleaseAcknowledgement { get; set; }
        internal int DestroyCount => Volatile.Read(ref destroyCount);
        internal int SessionCount => sessions.Count;
        internal ConsulWireHandler(bool includeUnknownMetadata = false) => this.includeUnknownMetadata = includeUnknownMetadata;
        internal int RequestCount => Volatile.Read(ref requests);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref requests);
            var path = request.RequestUri!.AbsolutePath;
            var query = request.RequestUri.Query;
            if (path == "/v1/kv/dilock/namespaces/tests/meta" && request.Method == HttpMethod.Get)
            {
                var metadata = new Dictionary<string, object?>
                {
                    ["protocol_major"] = 1, ["identity_scheme"] = "sha256-framed-utf8-v1",
                    ["profile"] = "consul-kv-session-v1", ["coordination_epoch"] = "epoch-1", ["fixed_lease_ms"] = null
                };
                if (includeUnknownMetadata) metadata["future_field"] = "unsupported";
                var metadataJson = JsonSerializer.Serialize(metadata);
                return Json(new[] { new { Key = "dilock/namespaces/tests/meta", Value = Convert.ToBase64String(Encoding.UTF8.GetBytes(metadataJson)) } });
            }
            if (path == "/v1/session/create")
            {
                var id = "session-" + Interlocked.Increment(ref sessionCount);
                sessions[id] = 0;
                return Json(new { ID = id });
            }
            if (path.StartsWith("/v1/session/renew/", StringComparison.Ordinal))
            {
                var id = path[(path.LastIndexOf('/') + 1)..];
                return Json(sessions.ContainsKey(id) ? new[] { new { ID = id } } : Array.Empty<object>());
            }
            if (path.StartsWith("/v1/session/destroy/", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref destroyCount);
                var id = path[(path.LastIndexOf('/') + 1)..];
                sessions.TryRemove(id, out _);
                lock (gate) if (owner == id) owner = "";
                return Json(true);
            }
            if (path == "/v1/kv/dilock/locks/" + TestIdentity())
            {
                lock (gate)
                {
                    if (request.Method == HttpMethod.Get)
                    {
                        if (lockIndex == 0) return JsonStatus(System.Net.HttpStatusCode.NotFound, "");
                        return Json(new[] { new { Key = "dilock/locks/" + TestIdentity(), Session = owner, LockIndex = lockIndex } });
                    }
                    if (query.StartsWith("?acquire=", StringComparison.Ordinal))
                    {
                        var id = Uri.UnescapeDataString(query[9..]);
                        if (!sessions.ContainsKey(id)) return Json(false);
                        if (owner.Length != 0) return Json(owner == id);
                        owner = id;
                        lockIndex++;
                        return Json(true);
                    }
                    if (query.StartsWith("?release=", StringComparison.Ordinal))
                    {
                        if (FailNextReleaseAcknowledgement)
                        {
                            FailNextReleaseAcknowledgement = false;
                            throw new HttpRequestException("injected lost release acknowledgement");
                        }
                        var id = Uri.UnescapeDataString(query[9..]);
                        if (owner != id) return Json(false);
                        owner = "";
                        return Json(true);
                    }
                }
            }
            return JsonStatus(System.Net.HttpStatusCode.NotFound, "false");
        }

        private static string TestIdentity() => ResourceIdentity.Hash("tests", "order/17");
        private static Task<HttpResponseMessage> Json<T>(T value)
            => JsonStatus(System.Net.HttpStatusCode.OK, JsonSerializer.Serialize(value));
        private static Task<HttpResponseMessage> JsonStatus(System.Net.HttpStatusCode status, string body)
            => Task.FromResult(new HttpResponseMessage(status)
            { Content = new StringContent(body, Encoding.UTF8, "application/json") });
    }
}

public sealed class ConsulIntegrationFactAttribute : FactAttribute
{
    public ConsulIntegrationFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_CONSUL_ENDPOINT"))
            || string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_CONSUL_NAMESPACE"))
            || string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_CONSUL_EPOCH")))
            Skip = "Use tools/dev.py integration --sdk dotnet --backend consul with a disposable Consul agent.";
    }
}
