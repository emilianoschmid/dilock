using System.Text.Json;
using Dilock;
using Dilock.Redis;
using StackExchange.Redis;
using Xunit;

namespace Dilock.Tests;

public sealed class RedisIntegrationTests
{
    [Fact]
    public void NamespaceMetadataRejectsDuplicateTopLevelFieldsButIgnoresNestedExtensions()
    {
        using var duplicate = JsonDocument.Parse("{\"protocol_major\":1,\"protocol_major\":2}");
        using var escapedDuplicate = JsonDocument.Parse("{\"protocol_major\":1,\"protocol\\u005fmajor\":1}");
        using var nestedDuplicate = JsonDocument.Parse("{\"protocol_major\":1,\"extension\":{\"field\":1,\"field\":2}}");
        Assert.False(RedisProvider.HasUniqueTopLevelProperties(duplicate.RootElement));
        Assert.False(RedisProvider.HasUniqueTopLevelProperties(escapedDuplicate.RootElement));
        Assert.True(RedisProvider.HasUniqueTopLevelProperties(nestedDuplicate.RootElement));
    }

    [RedisIntegrationFact]
    public async Task ReleasingOneResourceKeepsSiblingLeaseAndSharedProviderUsable()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_ENDPOINT")!;
        var nameSpace = "it-shared-provider-" + Guid.NewGuid().ToString("N");
        var epoch = "shared-provider-" + Guid.NewGuid().ToString("N");
        await using var admin = await ConnectionMultiplexer.ConnectAsync(
            ConfigurationOptions.Parse(new Uri(endpoint).Authority));
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "redis-single-authority-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        Assert.True(await admin.GetDatabase().StringSetAsync(
            $"dilock:namespace:{nameSpace}:meta", metadata, when: When.NotExists));

        var providerOptions = new RedisProviderOptions
        {
            Endpoint = endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = epoch,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(5),
                    RpcTimeout = TimeSpan.FromSeconds(1),
                    CleanupTimeout = TimeSpan.FromSeconds(1)
                }
            }
        };
        await using var provider = await RedisProvider.ConnectAsync(providerOptions);
        await using var contender = await RedisProvider.ConnectAsync(providerOptions);
        var first = provider.Lock("shared/first");
        var sibling = provider.Lock("shared/sibling");
        var firstContender = contender.Lock("shared/first");
        var siblingContender = contender.Lock("shared/sibling");

        await using (var siblingLease = await sibling.AcquireAsync())
        {
            await using (var firstLease = await first.AcquireAsync())
            {
                Assert.Equal(LeaseState.Held, siblingLease.State);
                Assert.Null(await firstContender.TryAcquireAsync());
            }

            // Scope exit releases only this resource; the same provider remains open.
            await using var firstSuccessor = await firstContender.AcquireAsync();
            Assert.Equal(LeaseState.Held, siblingLease.State);
            Assert.Null(await siblingContender.TryAcquireAsync());
        }

        // Releasing the sibling also leaves the original provider usable.
        await using var siblingSuccessor = await sibling.AcquireAsync();
        Assert.Equal(LeaseState.Held, siblingSuccessor.State);

        // The legacy positional wait overload still inherits the provider's
        // immutable lease defaults when its optional lease policy is omitted.
        await using var positionalDefault = await provider.Lock("shared/positional-default")
            .AcquireAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(TimeSpan.FromSeconds(5).TotalMilliseconds, positionalDefault.EffectiveLeaseMilliseconds);
    }

    [RedisIntegrationFact]
    public async Task ConnectValidatesExistingMetadataAndNeverRepairsIt()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_ENDPOINT")!;
        var nameSpace = "factory-metadata-" + Guid.NewGuid().ToString("N");
        var expectedEpoch = "approved-" + Guid.NewGuid().ToString("N");
        var key = $"dilock:namespace:{nameSpace}:meta";
        var options = new RedisProviderOptions
        {
            Endpoint = endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = expectedEpoch
        };

        await using var admin = await ConnectionMultiplexer.ConnectAsync(
            ConfigurationOptions.Parse(new Uri(endpoint).Authority));
        var database = admin.GetDatabase();

        var missing = await Assert.ThrowsAsync<DilockException>(
            () => RedisProvider.ConnectAsync(options));
        Assert.Equal(FailureKind.ProtocolMismatch, missing.Kind);
        Assert.True((await database.StringGetAsync(key)).IsNull);

        const string preservedListValue = "preserve-metadata-list";
        Assert.Equal(1L, await database.ListLeftPushAsync(key, preservedListValue));
        var wrongType = await Assert.ThrowsAsync<DilockException>(
            () => RedisProvider.ConnectAsync(options));
        Assert.Equal(FailureKind.ProtocolMismatch, wrongType.Kind);
        var preservedList = await database.ListRangeAsync(key);
        Assert.Single(preservedList);
        Assert.Equal(preservedListValue, (string?)preservedList[0]);
        Assert.True(await database.KeyDeleteAsync(key));

        var incompatibleMetadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "redis-single-authority-v1",
            coordination_epoch = "some-other-approved-epoch",
            fixed_lease_ms = (long?)null
        });
        Assert.True(await database.StringSetAsync(key, incompatibleMetadata, when: When.NotExists));

        var mismatch = await Assert.ThrowsAsync<DilockException>(
            () => RedisProvider.ConnectAsync(options));
        Assert.Equal(FailureKind.ProtocolMismatch, mismatch.Kind);
        Assert.Equal(incompatibleMetadata, await database.StringGetAsync(key));

        var matchingMetadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "redis-single-authority-v1",
            coordination_epoch = expectedEpoch,
            fixed_lease_ms = (long?)null
        });
        Assert.True(await database.StringSetAsync(key, matchingMetadata, when: When.Exists));

        await using var provider = await RedisProvider.ConnectAsync(options);
        var lease = await provider.Lock("factory-validation/smoke").AcquireAsync();
        Assert.Equal(ReleaseOutcome.Released, await lease.ReleaseAsync());
    }
}

public sealed class RedisIntegrationFactAttribute : FactAttribute
{
    public RedisIntegrationFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_REDIS_ENDPOINT")))
            Skip = "Set DILOCK_REDIS_ENDPOINT to run this live Redis integration test.";
    }
}
