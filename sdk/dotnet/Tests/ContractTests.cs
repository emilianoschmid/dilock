using System.Security.Cryptography;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Dilock;
using Dilock.Etcd;
using StackExchange.Redis;
using Xunit;

namespace Dilock.Tests;

public sealed class ContractTests
{
    [Fact]
    public void EtcdCleanupRemainingBudgetNeverRestartsTheDeadline()
    {
        var budget = TimeSpan.FromMilliseconds(200);
        var started = System.Diagnostics.Stopwatch.GetTimestamp();

        var remaining = EtcdProvider.RemainingCleanup(budget, started);
        Assert.InRange(remaining, TimeSpan.Zero, budget);
        Assert.Equal(TimeSpan.Zero, EtcdProvider.RemainingCleanup(budget, started - System.Diagnostics.Stopwatch.Frequency));
    }

    [Fact]
    public void EtcdAbandonedAcquisitionPassesOnlyRemainingCleanupTimeToRevoke()
    {
        var budget = TimeSpan.FromSeconds(1);
        var renewalDrainElapsed = TimeSpan.FromMilliseconds(725);

        Assert.Equal(TimeSpan.FromMilliseconds(275), EtcdProvider.RemainingCleanup(budget, renewalDrainElapsed));
        Assert.Equal(TimeSpan.Zero, EtcdProvider.RemainingCleanup(budget, budget));
        Assert.Equal(TimeSpan.Zero, EtcdProvider.RemainingCleanup(budget, budget + TimeSpan.FromMilliseconds(5)));
    }

    [Theory]
    [InlineData(0, true, 0, ReleaseOutcome.Released)]
    [InlineData(0, true, 1, ReleaseOutcome.AlreadyNotOwned)]
    [InlineData(0, false, 0, ReleaseOutcome.AlreadyNotOwned)]
    [InlineData(1, false, 1, ReleaseOutcome.AlreadyNotOwned)]
    [InlineData(1, false, 0, ReleaseOutcome.AlreadyNotOwned)]
    [InlineData(1, false, 2, ReleaseOutcome.Unknown)]
    [InlineData(2, true, 0, ReleaseOutcome.Unknown)]
    [InlineData(2, false, 1, ReleaseOutcome.Unknown)]
    public void EtcdReleaseClassificationRequiresKnownOwnershipAndCleanupAcknowledgement(
        int ownership, bool unlockAcknowledged, int revoke, ReleaseOutcome expected)
        => Assert.Equal(expected, EtcdProvider.ClassifyRelease(
            (EtcdProvider.OwnershipObservation)ownership, unlockAcknowledged,
            (EtcdProvider.RevokeObservation)revoke));

    [Fact]
    public void RedisAcquisitionRpcBudgetsShareTheTotalWaitDeadline()
    {
        var wait = TimeSpan.FromSeconds(1);
        var method = typeof(Dilock.Redis.RedisProvider).Assembly
            .GetType("Dilock.Redis.RedisHandle")!
            .GetMethod("AttemptBudgetAfter", BindingFlags.Static | BindingFlags.NonPublic)!;

        var budget = (TimeSpan)method.Invoke(null, new object[] { TimeSpan.FromSeconds(2), (TimeSpan?)wait, TimeSpan.FromMilliseconds(250) })!;

        Assert.Equal(TimeSpan.FromMilliseconds(750), budget);
        Assert.Equal(TimeSpan.Zero, (TimeSpan)method.Invoke(null,
            new object[] { TimeSpan.FromSeconds(2), (TimeSpan?)wait, TimeSpan.FromSeconds(2) })!);
    }

    [Theory]
    [InlineData(ReleaseOutcome.Released, FailureKind.WaitTimeout)]
    [InlineData(ReleaseOutcome.AlreadyNotOwned, FailureKind.WaitTimeout)]
    [InlineData(ReleaseOutcome.Unknown, FailureKind.AcquisitionUnknown)]
    public void LateRedisAcquisitionPreservesCleanupOutcome(ReleaseOutcome cleanup, FailureKind expected)
        => Assert.Equal(expected, Dilock.Redis.RedisProvider.LateAcquisitionFailure(cleanup));

    [Fact]
    public void RedisCancellationAfterAcknowledgedSetRequiresAcknowledgedCleanup()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.IsAssignableFrom<OperationCanceledException>(Dilock.Redis.RedisProvider.CancelledAcquisitionFailure(
            ReleaseOutcome.Released, cancellation.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(Dilock.Redis.RedisProvider.CancelledAcquisitionFailure(
            ReleaseOutcome.AlreadyNotOwned, cancellation.Token));
        var unknown = Assert.IsType<DilockException>(Dilock.Redis.RedisProvider.CancelledAcquisitionFailure(
            ReleaseOutcome.Unknown, cancellation.Token));
        Assert.Equal(FailureKind.AcquisitionUnknown, unknown.Kind);
    }

    [Fact]
    public void RedisNamespaceMetadataRequiresAStringKey()
    {
        Dilock.Redis.RedisProvider.ValidateMetadataType(RedisType.String);
        Assert.Throws<DilockException>(() => Dilock.Redis.RedisProvider.ValidateMetadataType(RedisType.None));
        Assert.Throws<DilockException>(() => Dilock.Redis.RedisProvider.ValidateMetadataType(RedisType.List));
    }

    [Fact]
    public void RedisMetadataTypeAndReadShareOneValidationBudget()
    {
        var budget = TimeSpan.FromMilliseconds(200);
        var started = Stopwatch.GetTimestamp() - Stopwatch.Frequency / 4;
        var remaining = Dilock.Redis.RedisProvider.RemainingBudget(budget, started);
        Assert.InRange(remaining, TimeSpan.Zero, TimeSpan.FromMilliseconds(200));
        Assert.Equal(TimeSpan.Zero, Dilock.Redis.RedisProvider.RemainingBudget(budget, started - Stopwatch.Frequency));
    }

    [Fact]
    public void AllGoldenIdentityVectorsMatchWithoutChangingTheFixture()
    {
        using var vectors = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "identity.json")));
        foreach (var vector in vectors.RootElement.GetProperty("valid").EnumerateArray())
            Assert.Equal(vector.GetProperty("sha256").GetString(), ResourceIdentity.Hash(vector.GetProperty("namespace").GetString()!, vector.GetProperty("resource").GetString()!));
        foreach (var vector in vectors.RootElement.GetProperty("invalid").EnumerateArray())
        {
            var ns = vector.GetProperty("namespace"); var resource = vector.GetProperty("resource");
            Assert.ThrowsAny<Exception>(() => ResourceIdentity.Hash(ns.GetString()!, resource.GetString()!));
        }
    }

    [Fact]
    public void StrictUtf8AcceptsF1ToF3FourByteScalarsAndRejectsUnpairedSurrogates()
    {
        Assert.Equal(64, ResourceIdentity.Hash("default", "\U00040000").Length);
        Assert.Equal(64, ResourceIdentity.Hash("default", "\U000FFFFF").Length);
        Assert.Throws<ArgumentException>(() => ResourceIdentity.Hash("default", "\uD800"));
        Assert.Throws<ArgumentException>(() => ResourceIdentity.Hash("default", "\uDFFF"));
    }

    [Theory]
    [InlineData("redis-release.lua")]
    [InlineData("redis-renew.lua")]
    public void AdapterLuaIsByteIdenticalToCanonical(string script)
    {
        using var expected = File.OpenRead(Path.Combine(AppContext.BaseDirectory, script));
        using var actual = typeof(Dilock.Redis.RedisProvider).Assembly.GetManifestResourceStream(script)!;
        using var expectedHash = SHA256.Create(); using var actualHash = SHA256.Create();
        Assert.Equal(expectedHash.ComputeHash(expected), actualHash.ComputeHash(actual));
    }

    [Fact]
    public void OptionsRejectInvalidBudgets()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new LeaseOptions { LeaseDuration = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new LeaseOptions { LeaseDuration = TimeSpan.FromMilliseconds(100), RpcTimeout = TimeSpan.FromMilliseconds(100) }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new LeaseOptions { LeaseDuration = TimeSpan.FromDays(2) }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcquireOptions { WaitTimeout = TimeSpan.Zero }.Validate());
    }

    [Fact]
    public void AcquireOptionsExposeSharedDefaultsAndValidateCompletePolicy()
    {
        var options = new AcquireOptions();
        Assert.Equal(TimeSpan.FromSeconds(5), options.WaitTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), options.Lease.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(2), options.Lease.RpcTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), options.Lease.CleanupTimeout);
        options.Validate();
        Assert.Throws<ArgumentNullException>(() => new AcquireOptions { Lease = null! }.Validate());
    }

    [Fact]
    public void PublicAdaptersExposeDefaultedAndFullyConfiguredAcquisitionOverloads()
    {
        foreach (var assembly in new[] { typeof(Dilock.Redis.RedisProvider).Assembly, typeof(Dilock.Etcd.EtcdProvider).Assembly })
        {
            var handle = assembly.GetTypes().Single(type => type.Name is "RedisHandle" or "EtcdHandle");
            Assert.NotNull(handle.GetMethod("AcquireAsync", [typeof(CancellationToken)]));
            Assert.NotNull(handle.GetMethod("AcquireAsync", [typeof(AcquireOptions), typeof(CancellationToken)]));
        }
    }

    [Fact]
    public void EtcdKeepAliveCannotExtendAValidityWindowAfterItsPriorDeadline()
    {
        var frequency = Stopwatch.Frequency;
        var sent = 0L;
        var priorDeadline = frequency;
        var now = frequency + 1;
        var method = typeof(Dilock.Etcd.EtcdProvider).Assembly
            .GetType("Dilock.Etcd.EtcdLease")!
            .GetMethod("TryCalculateValidityDeadline", BindingFlags.Static | BindingFlags.NonPublic)!;
        var args = new object[] { sent, now, priorDeadline, TimeSpan.FromSeconds(10), false, 0L };

        Assert.False((bool)method.Invoke(null, args)!);
    }

    [Fact]
    public void EtcdGrantLockAndVerificationShareOneWaitBudget()
    {
        var method = typeof(Dilock.Etcd.EtcdProvider).Assembly
            .GetType("Dilock.Etcd.EtcdHandle")!
            .GetMethod("RemainingWaitAt", BindingFlags.Static | BindingFlags.NonPublic)!;

        Assert.Equal(TimeSpan.FromSeconds(5), (TimeSpan)method.Invoke(null,
            [TimeSpan.FromSeconds(5), TimeSpan.Zero])!);
        Assert.Equal(TimeSpan.FromSeconds(2), (TimeSpan)method.Invoke(null,
            [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(3)])!);
        Assert.Equal(TimeSpan.Zero, (TimeSpan)method.Invoke(null,
            [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5)])!);
    }

    [Fact]
    public void RedisExpiryTimerUsesRemainingTimeFromTheCommandSend()
    {
        var sent = Stopwatch.GetTimestamp() - Stopwatch.Frequency / 5;
        var method = typeof(Dilock.Redis.RedisProvider).Assembly
            .GetType("Dilock.Redis.RedisLease")!
            .GetMethod("RemainingValidity", BindingFlags.Static | BindingFlags.NonPublic)!;

        var remaining = (TimeSpan)method.Invoke(null,
            new object[] { sent, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100) })!;

        Assert.InRange(remaining, TimeSpan.FromMilliseconds(650), TimeSpan.FromMilliseconds(800));
    }
}
