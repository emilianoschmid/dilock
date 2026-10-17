using System.Collections.Concurrent;
using System.Text.Json;
using Dilock;
using Dilock.Etcd;
using Dilock.Consul;
using Dilock.Redis;

using var config = JsonDocument.Parse(File.ReadAllText(args[0]));
var c = config.RootElement;
var backend = c.TryGetProperty("backend", out var configuredBackend) ? configuredBackend.GetString() : "redis";
ILockProvider provider = backend switch
{
    "redis" => await RedisProvider.ConnectAsync(c.GetProperty("endpoint").GetString()!, c.GetProperty("namespace").GetString()!, c.GetProperty("epoch").GetString()!),
    "etcd" => await EtcdProvider.ConnectAsync(c.GetProperty("endpoint").GetString()!, c.GetProperty("namespace").GetString()!, c.GetProperty("epoch").GetString()!),
    "consul" => await ConsulProvider.ConnectAsync(new ConsulProviderOptions
    {
        Endpoint = c.GetProperty("endpoint").GetString()!, Namespace = c.GetProperty("namespace").GetString()!,
        ExpectedEpoch = c.GetProperty("epoch").GetString()!, Token = Environment.GetEnvironmentVariable("CONSUL_HTTP_TOKEN")
    }),
    _ => throw new ArgumentException("backend must be redis, etcd or consul", nameof(backend))
};
await using var ownedProvider = provider;
var leases = new ConcurrentDictionary<string, ILease>();
var pending = new ConcurrentDictionary<string, CancellationTokenSource>();
var tasks = new List<Task>();
var outputGate = new SemaphoreSlim(1);
var seen = new HashSet<string>();
async Task Write(object response) { await outputGate.WaitAsync(); try { Console.WriteLine(JsonSerializer.Serialize(response)); } finally { outputGate.Release(); } }
async Task Process(string line, string id, CancellationTokenSource ct)
{
    try
    {
        using var doc = JsonDocument.Parse(line); var r = doc.RootElement; var op = r.GetProperty("op").GetString();
        if (op == "hello") { await Write(new { id, status = "ok", sdk = "dotnet", version = "0.1.0-dev.1", protocol = 1, capabilities = backend switch { "etcd" => new[] { "etcd", "renewal", "auto_renew", "fencing" }, "consul" => new[] { "consul", "renewal", "auto_renew", "fencing" }, _ => new[] { "redis", "renewal", "auto_renew" } } }); return; }
        if (op is "try_acquire" or "acquire")
        {
            var o = new LeaseOptions { LeaseDuration = TimeSpan.FromMilliseconds(r.TryGetProperty("lease_ms", out var ttl) ? ttl.GetInt64() : 30000), RpcTimeout = TimeSpan.FromMilliseconds(r.TryGetProperty("rpc_ms", out var rpc) ? rpc.GetInt64() : 200), AutoRenew = r.TryGetProperty("auto_renew", out var auto) && auto.GetBoolean() };
            var handle = provider.Lock(r.GetProperty("resource").GetString()!);
            var lease = op == "try_acquire" ? await handle.TryAcquireAsync(o, ct.Token) : await handle.AcquireAsync(TimeSpan.FromMilliseconds(r.GetProperty("wait_ms").GetInt64()), o, ct.Token);
            if (lease == null) await Write(new { id, status = "busy" });
            else
            {
                var local = Guid.NewGuid().ToString("N"); leases[local] = lease;
                if (lease is IEtcdLease etcdLease)
                    await Write(new { id, status = "acquired", lease_id = local,
                        effective_lease_ms = lease.EffectiveLeaseMilliseconds,
                        fence = new { epoch = etcdLease.Epoch, value = etcdLease.CreateRevision.ToString() } });
                else if (lease is IConsulLease consulLease)
                    await Write(new { id, status = "acquired", lease_id = local,
                        effective_lease_ms = lease.EffectiveLeaseMilliseconds,
                        fence = new { epoch = consulLease.Epoch, value = consulLease.LockIndex.ToString() } });
                else
                    await Write(new { id, status = "acquired", lease_id = local,
                        effective_lease_ms = lease.EffectiveLeaseMilliseconds });
            }
        }
        else if (op is "release" or "inspect" or "renew")
        {
            var lease = leases[r.GetProperty("lease_id").GetString()!];
            if (op == "release") { var result = await lease.ReleaseAsync(); await Write(new { id, status = result == ReleaseOutcome.Released ? "released" : result == ReleaseOutcome.AlreadyNotOwned ? "already_not_owned" : "release_unknown" }); }
            else if (op == "inspect") await Write(new { id, status = "ok", state = lease.State.ToString().ToLowerInvariant() });
            else await Write(new { id, status = await lease.RenewAsync(ct.Token) ? "renewed" : "lost" });
        }
        else if (op == "cancel_request") { if (pending.TryGetValue(r.GetProperty("target_id").GetString()!, out var target)) target.Cancel(); await Write(new { id, status = "ok" }); }
        else await Write(new { id, status = "invalid_argument" });
    }
    catch (DilockException e) { await Write(new { id, status = e.Kind switch { FailureKind.AcquisitionUnknown => "acquisition_unknown", FailureKind.BackendUnavailable => "backend_unavailable", FailureKind.ProtocolMismatch => "protocol_mismatch", FailureKind.UnsupportedCapability => "unsupported_capability", FailureKind.WaitTimeout => "timeout", _ => "internal_error" }, remote_outcome_unknown = e.Kind == FailureKind.AcquisitionUnknown }); }
    catch (OperationCanceledException) { await Write(new { id, status = "cancelled", remote_outcome_unknown = false }); }
    catch (Exception e) when (e is ArgumentException or JsonException or KeyNotFoundException or InvalidOperationException) { await Write(new { id, status = "invalid_argument" }); }
    finally { pending.TryRemove(id, out _); ct.Dispose(); }
}
while (await Console.In.ReadLineAsync() is { } line)
{
    if (line.Length > 65536) { await Write(new { id = "", status = "invalid_argument" }); continue; }
    string id; string? op;
    try { using var d = JsonDocument.Parse(line); id = d.RootElement.GetProperty("id").GetString()!; op = d.RootElement.GetProperty("op").GetString(); }
    catch { await Write(new { id = "", status = "invalid_argument" }); continue; }
    if (!seen.Add(id)) { await Write(new { id, status = "invalid_argument" }); continue; }
    if (op == "shutdown") { foreach (var token in pending.Values) token.Cancel(); await Task.WhenAll(tasks); await provider.DisposeAsync(); await Write(new { id, status = "ok", outstanding_cleanup = 0 }); break; }
    var ct = new CancellationTokenSource(); pending[id] = ct; tasks.Add(Process(line, id, ct));
}
foreach (var token in pending.Values) token.Cancel();
await Task.WhenAll(tasks);
