using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Dilock;
using Dilock.Redis;
using StackExchange.Redis;
using Xunit;

namespace Dilock.Tests;

public sealed class RedisLostAckIntegrationTests
{
    [RedisIntegrationFact]
    public async Task CancellationAfterAppliedSetRemainsUnknownAndOwnerCleanupRemovesKey()
    {
        var backendEndpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_ENDPOINT")!;
        var nameSpace = "it-cancel-applied-set-" + Guid.NewGuid().ToString("N");
        var epoch = "cancel-applied-set-" + Guid.NewGuid().ToString("N");
        var resource = "applied-set/" + Guid.NewGuid().ToString("N");
        var key = $"dilock:{{{ResourceIdentity.Hash(nameSpace, resource)}}}:lease";
        var metadataKey = $"dilock:namespace:{nameSpace}:meta";

        await using var admin = await ConnectionMultiplexer.ConnectAsync(new Uri(backendEndpoint).Authority);
        var database = admin.GetDatabase();
        var metadata = $$"""
            {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"{{epoch}}","fixed_lease_ms":null}
            """;
        Assert.True(await database.StringSetAsync(metadataKey, metadata, when: When.NotExists));

        await using var proxy = new RedisAppliedSetCancellationProxy(backendEndpoint);
        using var cancellation = new CancellationTokenSource();
        await using var provider = await RedisProvider.ConnectAsync(new RedisProviderOptions
        {
            Endpoint = proxy.Endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = epoch,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(30),
                    RpcTimeout = TimeSpan.FromSeconds(5),
                    CleanupTimeout = TimeSpan.FromSeconds(2),
                    AutoRenew = false
                }
            }
        });

        var acquisition = provider.Lock(resource).TryAcquireAsync(cancellationToken: cancellation.Token);
        var appliedReply = await proxy.AppliedSetReply.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(Encoding.ASCII.GetBytes("+OK\r\n"), appliedReply);
        Assert.False((await database.StringGetAsync(key)).IsNull,
            "The proxy must hold the SET acknowledgement only after Redis applied the owner key.");

        cancellation.Cancel();
        var unknown = await Assert.ThrowsAsync<DilockException>(() => acquisition);
        Assert.Equal(FailureKind.AcquisitionUnknown, unknown.Kind);

        var cleanupReply = await proxy.FirstCleanupReply.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(Encoding.ASCII.GetBytes(":1\r\n"), cleanupReply);
        await provider.DisposeAsync();

        Assert.True((await database.StringGetAsync(key)).IsNull,
            "Tracked post-SET owner cleanup must remove the lock after the delayed SET acknowledgement arrives.");
        Assert.Equal(1, proxy.SetCount);
        Assert.Equal(2, proxy.CleanupEvalCount);
        Assert.True(await database.KeyDeleteAsync(metadataKey), "The test must remove only its own namespace metadata.");
        Assert.True((await database.StringGetAsync(key)).IsNull);
    }

    [RedisIntegrationFact]
    public async Task AppliedReleaseWithDiscardedRedisAcknowledgementIsUnknownAndNeverReplayed()
    {
        var backendEndpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_ENDPOINT")!;
        var nameSpace = "it-lost-release-ack-" + Guid.NewGuid().ToString("N");
        var epoch = "lost-release-ack-" + Guid.NewGuid().ToString("N");
        var resource = "applied-release/" + Guid.NewGuid().ToString("N");
        var key = $"dilock:{{{ResourceIdentity.Hash(nameSpace, resource)}}}:lease";

        await using var admin = await ConnectionMultiplexer.ConnectAsync(new Uri(backendEndpoint).Authority);
        var database = admin.GetDatabase();
        var metadata = $$"""
            {"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"{{epoch}}","fixed_lease_ms":null}
            """;
        Assert.True(await database.StringSetAsync($"dilock:namespace:{nameSpace}:meta", metadata, when: When.NotExists));

        await using var proxy = new RedisReleaseAckDropProxy(backendEndpoint);
        var cleanupObservations = new List<CleanupObservation>();
        await using var provider = await RedisProvider.ConnectAsync(new RedisProviderOptions
        {
            Endpoint = proxy.Endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = epoch,
            CleanupObserver = cleanupObservations.Add,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(5),
                    RpcTimeout = TimeSpan.FromSeconds(1),
                    CleanupTimeout = TimeSpan.FromSeconds(1),
                    AutoRenew = false
                }
            }
        });

        var lease = await provider.Lock(resource).AcquireAsync();
        Assert.False((await database.StringGetAsync(key)).IsNull, "The acquired owner key must exist before release starts.");
        await lease.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        var discardedReply = await proxy.AcknowledgementDiscarded.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True((await database.StringGetAsync(key)).IsNull,
            "Redis must have deleted the owner key before the proxy discards the EVAL acknowledgement.");
        Assert.Equal(Encoding.ASCII.GetBytes(":1\r\n"), discardedReply);
        Assert.Equal(ReleaseOutcome.Unknown, await lease.ReleaseAsync());
        Assert.Equal(LeaseState.ReleasedUnknown, lease.State);
        var observation = Assert.Single(cleanupObservations);
        Assert.Equal("redis-single-authority-v1", observation.Backend);
        Assert.Equal(ReleaseOutcome.Unknown, observation.Outcome);
        Assert.Equal("lease-dispose", observation.Reason);
        Assert.Equal(1, proxy.ReleaseEvalCount);
    }
}

/// <summary>Holds a successful SET reply until owner-checked cleanup has executed.</summary>
internal sealed class RedisAppliedSetCancellationProxy : IAsyncDisposable
{
    private readonly Uri backend;
    private readonly TcpListener listener;
    private readonly CancellationTokenSource stop = new();
    private readonly ConcurrentDictionary<TcpClient, byte> clients = new();
    private readonly TaskCompletionSource<byte[]> appliedSetReply = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<byte[]> firstCleanupReply = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task acceptLoop;
    private int setCount;
    private int cleanupEvalCount;

    public RedisAppliedSetCancellationProxy(string backendEndpoint)
    {
        backend = new Uri(backendEndpoint);
        listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        Endpoint = $"redis://127.0.0.1:{port}";
        acceptLoop = AcceptAsync();
    }

    public string Endpoint { get; }
    public int SetCount => Volatile.Read(ref setCount);
    public int CleanupEvalCount => Volatile.Read(ref cleanupEvalCount);
    public Task<byte[]> AppliedSetReply => appliedSetReply.Task;
    public Task<byte[]> FirstCleanupReply => firstCleanupReply.Task;

    private async Task AcceptAsync()
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stop.Token).ConfigureAwait(false);
                clients.TryAdd(client, 0);
                _ = HandleAsync(client);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) when (stop.IsCancellationRequested) { }
    }

    private async Task HandleAsync(TcpClient client)
    {
        using (client)
        using (var upstream = new TcpClient())
        {
            try
            {
                await upstream.ConnectAsync(backend.Host, backend.Port > 0 ? backend.Port : 6379, stop.Token).ConfigureAwait(false);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                var commands = new ConcurrentQueue<string>();
                var clientStream = client.GetStream();
                var upstreamStream = upstream.GetStream();
                var outbound = ForwardCommandsAsync(clientStream, upstreamStream, commands, linked.Token);
                var inbound = ForwardRepliesAsync(upstreamStream, clientStream, commands, linked.Token);
                await Task.WhenAny(outbound, inbound).ConfigureAwait(false);
                linked.Cancel();
                client.Close();
                upstream.Close();
                try { await Task.WhenAll(outbound, inbound).ConfigureAwait(false); }
                catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            finally { clients.TryRemove(client, out _); }
        }
    }

    private async Task ForwardCommandsAsync(NetworkStream source, NetworkStream destination,
        ConcurrentQueue<string> commands, CancellationToken cancellationToken)
    {
        var reader = new RedisRespReader(source);
        while (true)
        {
            var frame = await reader.ReadFrameAsync(cancellationToken).ConfigureAwait(false);
            if (frame is null) return;
            var command = RedisRespReader.CommandName(frame.Bytes);
            commands.Enqueue(command);
            if (string.Equals(command, "SET", StringComparison.OrdinalIgnoreCase)) Interlocked.Increment(ref setCount);
            if (string.Equals(command, "EVAL", StringComparison.OrdinalIgnoreCase)
                || string.Equals(command, "EVALSHA", StringComparison.OrdinalIgnoreCase))
                Interlocked.Increment(ref cleanupEvalCount);
            await destination.WriteAsync(frame.Bytes, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ForwardRepliesAsync(NetworkStream source, NetworkStream destination,
        ConcurrentQueue<string> commands, CancellationToken cancellationToken)
    {
        var reader = new RedisRespReader(source);
        byte[]? heldSetReply = null;
        var pendingReplies = new List<(string Command, byte[] Bytes)>();
        while (true)
        {
            var frame = await reader.ReadFrameAsync(cancellationToken).ConfigureAwait(false);
            if (frame is null) return;
            if (frame.IsPush) { await destination.WriteAsync(frame.Bytes, cancellationToken).ConfigureAwait(false); continue; }
            if (!commands.TryDequeue(out var command)) throw new InvalidDataException("Redis replied without a correlated request.");
            if (string.Equals(command, "SET", StringComparison.OrdinalIgnoreCase) && heldSetReply is null)
            {
                heldSetReply = frame.Bytes;
                appliedSetReply.TrySetResult(frame.Bytes);
                continue;
            }
            if (heldSetReply is not null)
            {
                pendingReplies.Add((command, frame.Bytes));
                if ((string.Equals(command, "EVAL", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(command, "EVALSHA", StringComparison.OrdinalIgnoreCase))
                    && frame.Bytes.Length > 0 && frame.Bytes[0] == (byte)':')
                {
                    firstCleanupReply.TrySetResult(frame.Bytes);
                    await destination.WriteAsync(heldSetReply, cancellationToken).ConfigureAwait(false);
                    foreach (var pending in pendingReplies)
                        await destination.WriteAsync(pending.Bytes, cancellationToken).ConfigureAwait(false);
                    heldSetReply = null;
                    pendingReplies.Clear();
                }
                continue;
            }
            await destination.WriteAsync(frame.Bytes, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        stop.Cancel();
        listener.Stop();
        foreach (var client in clients.Keys) client.Close();
        try { await acceptLoop.ConfigureAwait(false); }
        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
        stop.Dispose();
    }
}

/// <summary>Forwards Redis RESP traffic but discards the reply to the first EVAL after Redis executes it.</summary>
internal sealed class RedisReleaseAckDropProxy : IAsyncDisposable
{
    private readonly Uri backend;
    private readonly TcpListener listener;
    private readonly CancellationTokenSource stop = new();
    private readonly ConcurrentDictionary<TcpClient, byte> clients = new();
    private readonly TaskCompletionSource<byte[]> acknowledgementDiscarded = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task acceptLoop;
    private int releaseEvalCount;

    public RedisReleaseAckDropProxy(string backendEndpoint)
    {
        backend = new Uri(backendEndpoint);
        listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        Endpoint = $"redis://127.0.0.1:{port}";
        acceptLoop = AcceptAsync();
    }

    public string Endpoint { get; }
    public int ReleaseEvalCount => Volatile.Read(ref releaseEvalCount);
    public Task<byte[]> AcknowledgementDiscarded => acknowledgementDiscarded.Task;

    private async Task AcceptAsync()
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stop.Token).ConfigureAwait(false);
                clients.TryAdd(client, 0);
                _ = HandleAsync(client);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) when (stop.IsCancellationRequested) { }
    }

    private async Task HandleAsync(TcpClient client)
    {
        using (client)
        using (var upstream = new TcpClient())
        {
            try
            {
                await upstream.ConnectAsync(backend.Host, backend.Port > 0 ? backend.Port : 6379, stop.Token).ConfigureAwait(false);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                var clientStream = client.GetStream();
                var upstreamStream = upstream.GetStream();
                var releaseDispatched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var commands = new ConcurrentQueue<string>();
                var outbound = ForwardCommandsAsync(clientStream, upstreamStream, commands, linked.Token);
                var inbound = ForwardRepliesAsync(upstreamStream, clientStream, client, upstream, commands, releaseDispatched, linked.Token);
                await Task.WhenAny(outbound, inbound).ConfigureAwait(false);
                linked.Cancel();
                client.Close();
                upstream.Close();
                try { await Task.WhenAll(outbound, inbound).ConfigureAwait(false); }
                catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
            finally { clients.TryRemove(client, out _); }
        }
    }

    private async Task ForwardCommandsAsync(NetworkStream source, NetworkStream destination,
        ConcurrentQueue<string> commands, CancellationToken cancellationToken)
    {
        var reader = new RedisRespReader(source);
        while (true)
        {
            var frame = await reader.ReadFrameAsync(cancellationToken).ConfigureAwait(false);
            if (frame is null) return;
            var command = RedisRespReader.CommandName(frame.Bytes);
            commands.Enqueue(command);
            if (string.Equals(command, "EVAL", StringComparison.OrdinalIgnoreCase))
            {
                Interlocked.Increment(ref releaseEvalCount);
            }
            await destination.WriteAsync(frame.Bytes, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ForwardRepliesAsync(NetworkStream source, NetworkStream destination, TcpClient client,
        TcpClient upstream, ConcurrentQueue<string> commands, TaskCompletionSource releaseDispatched,
        CancellationToken cancellationToken)
    {
        var reader = new RedisRespReader(source);
        while (true)
        {
            var frame = await reader.ReadFrameAsync(cancellationToken).ConfigureAwait(false);
            if (frame is null) return;
            if (!frame.IsPush && commands.TryDequeue(out var command)
                && string.Equals(command, "EVAL", StringComparison.OrdinalIgnoreCase))
            {
                acknowledgementDiscarded.TrySetResult(frame.Bytes);
                client.Close();
                upstream.Close();
                return;
            }
            await destination.WriteAsync(frame.Bytes, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        stop.Cancel();
        listener.Stop();
        foreach (var client in clients.Keys) client.Close();
        try { await acceptLoop.ConfigureAwait(false); }
        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
        stop.Dispose();
    }
}

internal sealed class RedisRespReader(Stream stream)
{
    internal sealed record Frame(byte[] Bytes, bool IsPush);

    public async Task<Frame?> ReadFrameAsync(CancellationToken cancellationToken)
    {
        var marker = await ReadByteAsync(null, cancellationToken).ConfigureAwait(false);
        if (marker < 0) return null;
        var bytes = new List<byte>();
        bytes.Add((byte)marker);
        return await ReadFrameBodyAsync((byte)marker, bytes, cancellationToken).ConfigureAwait(false);
    }

    private async Task<Frame> ReadFrameBodyAsync(byte marker, List<byte> bytes, CancellationToken cancellationToken)
    {
        switch (marker)
        {
            case (byte)'+' or (byte)'-' or (byte)':':
            case (byte)',' or (byte)'#' or (byte)'(':
                await ReadLineAsync(bytes, cancellationToken).ConfigureAwait(false);
                return new Frame(bytes.ToArray(), false);
            case (byte)'_':
                await ReadLineAsync(bytes, cancellationToken).ConfigureAwait(false);
                return new Frame(bytes.ToArray(), false);
            case (byte)'$' or (byte)'=' or (byte)'!':
            {
                var length = int.Parse(await ReadLineAsync(bytes, cancellationToken).ConfigureAwait(false),
                    System.Globalization.CultureInfo.InvariantCulture);
                if (length >= 0)
                {
                    await ReadExactAsync(bytes, length + 2, cancellationToken).ConfigureAwait(false);
                }
                return new Frame(bytes.ToArray(), false);
            }
            case (byte)'*' or (byte)'~' or (byte)'>' or (byte)'%':
            {
                var count = int.Parse(await ReadLineAsync(bytes, cancellationToken).ConfigureAwait(false),
                    System.Globalization.CultureInfo.InvariantCulture);
                var isPush = marker == (byte)'>';
                if (count > 0)
                {
                    var children = marker == (byte)'%' ? checked(count * 2) : count;
                    for (var index = 0; index < children; index++)
                    {
                        var childMarker = await ReadByteAsync(bytes, cancellationToken).ConfigureAwait(false);
                        if (childMarker < 0) throw new EndOfStreamException("Redis RESP ended inside an aggregate frame.");
                        var child = await ReadFrameBodyAsync((byte)childMarker, bytes, cancellationToken).ConfigureAwait(false);
                        isPush |= child.IsPush;
                    }
                }
                return new Frame(bytes.ToArray(), isPush);
            }
            case (byte)'|':
            {
                var count = int.Parse(await ReadLineAsync(bytes, cancellationToken).ConfigureAwait(false),
                    System.Globalization.CultureInfo.InvariantCulture);
                for (var index = 0; index < checked(count * 2); index++)
                {
                    var childMarker = await ReadByteAsync(bytes, cancellationToken).ConfigureAwait(false);
                    if (childMarker < 0) throw new EndOfStreamException("Redis RESP ended inside response attributes.");
                    await ReadFrameBodyAsync((byte)childMarker, bytes, cancellationToken).ConfigureAwait(false);
                }
                var responseMarker = await ReadByteAsync(bytes, cancellationToken).ConfigureAwait(false);
                if (responseMarker < 0) throw new EndOfStreamException("Redis RESP ended after response attributes.");
                var response = await ReadFrameBodyAsync((byte)responseMarker, bytes, cancellationToken).ConfigureAwait(false);
                return new Frame(bytes.ToArray(), response.IsPush);
            }
            default:
                throw new InvalidDataException($"Unsupported Redis RESP marker 0x{marker:x2} in test proxy.");
        }
    }

    private async Task<string> ReadLineAsync(List<byte> capture, CancellationToken cancellationToken)
    {
        var line = new List<byte>();
        while (true)
        {
            var value = await ReadByteAsync(capture, cancellationToken).ConfigureAwait(false);
            if (value < 0) throw new EndOfStreamException("Redis RESP ended inside a line.");
            line.Add((byte)value);
            if (line.Count >= 2 && line[^2] == (byte)'\r' && line[^1] == (byte)'\n')
                return Encoding.ASCII.GetString(line.Take(line.Count - 2).ToArray());
        }
    }

    private async Task ReadExactAsync(List<byte> capture, int count, CancellationToken cancellationToken)
    {
        for (var index = 0; index < count; index++)
        {
            var value = await ReadByteAsync(capture, cancellationToken).ConfigureAwait(false);
            if (value < 0) throw new EndOfStreamException("Redis RESP ended inside a bulk value.");
        }
    }

    private async Task<int> ReadByteAsync(List<byte>? capture, CancellationToken cancellationToken)
    {
        var one = new byte[1];
        var count = await stream.ReadAsync(one, cancellationToken).ConfigureAwait(false);
        if (count == 0) return -1;
        capture?.Add(one[0]);
        return one[0];
    }

    public static string CommandName(byte[] frame)
    {
        var index = 0;
        if (frame[index++] != (byte)'*') throw new InvalidDataException("Expected a Redis command array.");
        SkipLine(frame, ref index);
        if (frame[index++] != (byte)'$') throw new InvalidDataException("Expected a bulk-string Redis command name.");
        var length = int.Parse(ReadAsciiLine(frame, ref index), System.Globalization.CultureInfo.InvariantCulture);
        var command = Encoding.ASCII.GetString(frame, index, length);
        return command;
    }

    private static string ReadAsciiLine(byte[] frame, ref int index)
    {
        var start = index;
        while (index + 1 < frame.Length && !(frame[index] == (byte)'\r' && frame[index + 1] == (byte)'\n')) index++;
        var value = Encoding.ASCII.GetString(frame, start, index - start);
        index += 2;
        return value;
    }

    private static void SkipLine(byte[] frame, ref int index) => _ = ReadAsciiLine(frame, ref index);
}
