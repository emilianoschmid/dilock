using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Dilock;
using Dilock.Etcd;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace Dilock.Tests;

public sealed class EtcdIntegrationTests
{
    [Fact]
    public void NamespaceMetadataRejectsDuplicateTopLevelFieldsButIgnoresNestedExtensions()
    {
        using var duplicate = JsonDocument.Parse("{\"coordination_epoch\":\"old\",\"coordination_epoch\":\"new\"}");
        using var nestedDuplicate = JsonDocument.Parse("{\"coordination_epoch\":\"epoch\",\"extension\":{\"field\":1,\"field\":2}}");
        Assert.False(EtcdProvider.HasUniqueTopLevelProperties(duplicate.RootElement));
        Assert.True(EtcdProvider.HasUniqueTopLevelProperties(nestedDuplicate.RootElement));
    }

    [EtcdIntegrationFact]
    public async Task AwaitUsingReleasesOnNormalAndExceptionalExitAndExplicitReleaseIsIdempotent()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var nameSpace = "it-await-using-" + Guid.NewGuid().ToString("N");
        var epoch = "await-using-" + Guid.NewGuid().ToString("N");
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var admin = new KV.KVClient(adminChannel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await admin.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{nameSpace}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        var options = new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = epoch,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(3),
                    RpcTimeout = TimeSpan.FromMilliseconds(250),
                    CleanupTimeout = TimeSpan.FromSeconds(1)
                }
            }
        };
        await using var ownerProvider = await EtcdProvider.ConnectAsync(options);
        await using var contenderProvider = await EtcdProvider.ConnectAsync(options);
        var owner = ownerProvider.Lock("await-using/resource");
        var contender = contenderProvider.Lock("await-using/resource");

        await using (var lease = await owner.AcquireAsync())
            Assert.Equal(LeaseState.Held, lease.State);
        var afterNormalExit = await contender.AcquireAsync();
        Assert.Equal(ReleaseOutcome.Released, await afterNormalExit.ReleaseAsync());

        var bodyFailure = new InvalidOperationException("primary protected-work failure");
        var observedFailure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var lease = await owner.AcquireAsync();
            Assert.Equal(LeaseState.Held, lease.State);
            throw bodyFailure;
        });
        Assert.Same(bodyFailure, observedFailure);
        var afterExceptionalExit = await contender.AcquireAsync();
        Assert.Equal(ReleaseOutcome.Released, await afterExceptionalExit.ReleaseAsync());

        await using (var lease = await owner.AcquireAsync())
        {
            Assert.Equal(ReleaseOutcome.Released, await lease.ReleaseAsync());
            Assert.Equal(ReleaseOutcome.Released, await lease.ReleaseAsync());
        }
        var afterExplicitReleaseAndDispose = await contender.AcquireAsync();
        Assert.Equal(ReleaseOutcome.Released, await afterExplicitReleaseAndDispose.ReleaseAsync());
    }

    [EtcdDefaultNamespaceIntegrationFact]
    public async Task TwoLocksShareDefaultProviderAndReleaseWithAwaitUsing()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        const string epoch = "api-ergonomics-default-20261005";
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(adminChannel);
        var key = ByteString.CopyFromUtf8("/dilock/namespaces/default/meta");
        var read = await kv.RangeAsync(new RangeRequest { Key = key }, deadline: DateTime.UtcNow.AddSeconds(2));
        if (read.Count == 0)
        {
            var metadata = JsonSerializer.Serialize(new
            {
                protocol_major = 1,
                identity_scheme = "sha256-framed-utf8-v1",
                profile = "etcd-native-lock-v1",
                coordination_epoch = epoch,
                fixed_lease_ms = (long?)null
            });
            await kv.PutAsync(new PutRequest { Key = key, Value = ByteString.CopyFromUtf8(metadata) }, deadline: DateTime.UtcNow.AddSeconds(2));
        }
        else
        {
            using var document = JsonDocument.Parse(read.Kvs.Single().Value.ToByteArray());
            Assert.Equal(epoch, document.RootElement.GetProperty("coordination_epoch").GetString());
        }

        await using var provider = await EtcdProvider.ConnectAsync(endpoint, epoch);
        var daily = provider.Lock("await-using/daily");
        var weekly = provider.Lock("await-using/weekly");
        await using (var dailyLease = await daily.AcquireAsync())
        await using (var weeklyLease = await weekly.AcquireAsync())
        {
            Assert.Equal(TimeSpan.FromSeconds(30).TotalMilliseconds, dailyLease.EffectiveLeaseMilliseconds);
            Assert.Equal(LeaseState.Held, dailyLease.State);
            Assert.Equal(LeaseState.Held, weeklyLease.State);
        }

        var successor = await daily.AcquireAsync(new AcquireOptions
        {
            WaitTimeout = TimeSpan.FromSeconds(2),
            Lease = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(3), RpcTimeout = TimeSpan.FromMilliseconds(250) }
        });
        Assert.Equal(ReleaseOutcome.Released, await successor.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task ReleasingOneResourceKeepsSiblingLeaseAndSharedProviderUsable()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var nameSpace = "it-shared-provider-" + Guid.NewGuid().ToString("N");
        var epoch = "shared-provider-" + Guid.NewGuid().ToString("N");
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(adminChannel);
        var key = ByteString.CopyFromUtf8($"/dilock/namespaces/{nameSpace}/meta");
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest { Key = key, Value = ByteString.CopyFromUtf8(metadata) }, deadline: DateTime.UtcNow.AddSeconds(2));

        var options = new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = nameSpace,
            ExpectedEpoch = epoch,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(3),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(5),
                    RpcTimeout = TimeSpan.FromSeconds(1),
                    CleanupTimeout = TimeSpan.FromSeconds(1)
                }
            }
        };
        await using var provider = await EtcdProvider.ConnectAsync(options);
        var first = provider.Lock("shared-provider/first/" + Guid.NewGuid().ToString("N"));
        var sibling = provider.Lock("shared-provider/sibling/" + Guid.NewGuid().ToString("N"));
        await using (var siblingLease = await sibling.AcquireAsync())
        {
            await using (var firstLease = await first.AcquireAsync())
            {
                Assert.Equal(LeaseState.Held, siblingLease.State);
                Assert.True(((IEtcdLease)firstLease).CreateRevision > 0);
            }

            // The first scope has released its remote lock while the sibling is still held.
            await using var firstSuccessor = await first.AcquireAsync();
            Assert.Equal(LeaseState.Held, siblingLease.State);
            Assert.True(((IEtcdLease)firstSuccessor).CreateRevision > ((IEtcdLease)siblingLease).CreateRevision);
        }

        // The same channel/provider can acquire again after the sibling scope exits.
        await using var siblingSuccessor = await sibling.AcquireAsync();
        Assert.Equal(LeaseState.Held, siblingSuccessor.State);
    }

    [EtcdIntegrationFact]
    public async Task DefaultAndConfiguredScopesReuseOneProviderAndReleaseOnScopeExit()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var ns = "it-default-" + Guid.NewGuid().ToString("N");
        var epoch = "local-integration-" + Guid.NewGuid().ToString("N");
        using var channel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(channel);
        var key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta");
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest { Key = key, Value = ByteString.CopyFromUtf8(metadata) }, deadline: DateTime.UtcNow.AddSeconds(3));

        var providerOptions = new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = ns,
            ExpectedEpoch = epoch,
            MetadataTimeout = TimeSpan.FromSeconds(2),
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(3), RpcTimeout = TimeSpan.FromMilliseconds(250) }
            }
        };
        await using var provider = await EtcdProvider.ConnectAsync(providerOptions);
        var daily = provider.Lock("scope/daily");
        var weekly = provider.Lock("scope/weekly");

        await using (var waitOnlyDefault = await daily.AcquireAsync(TimeSpan.FromSeconds(2)))
            Assert.Equal(TimeSpan.FromSeconds(3).TotalMilliseconds, waitOnlyDefault.EffectiveLeaseMilliseconds);

        await using (var dailyLease = await daily.AcquireAsync())
        await using (var weeklyLease = await weekly.AcquireAsync(new AcquireOptions
        {
            WaitTimeout = TimeSpan.FromSeconds(2),
            Lease = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(3), RpcTimeout = TimeSpan.FromMilliseconds(250) }
        }))
        {
            Assert.Equal(LeaseState.Held, dailyLease.State);
            Assert.Equal(LeaseState.Held, weeklyLease.State);
            Assert.True(((IEtcdLease)dailyLease).CreateRevision > 0);
            Assert.True(((IEtcdLease)weeklyLease).CreateRevision > 0);
        }

        var releasedDaily = await daily.AcquireAsync(new AcquireOptions
        {
            WaitTimeout = TimeSpan.FromSeconds(2),
            Lease = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(3), RpcTimeout = TimeSpan.FromMilliseconds(250) }
        });
        Assert.Equal(LeaseState.Held, releasedDaily.State);
        Assert.Equal(ReleaseOutcome.Released, await releasedDaily.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task ReleaseUsesOneBudgetAcrossDelayedUnlockAndRevokeAcknowledgements()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var ns = "it-cleanup-" + Guid.NewGuid().ToString("N");
        var epoch = "local-cleanup-" + Guid.NewGuid().ToString("N");
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(adminChannel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        var delayedAcks = new DelayedCleanupAcknowledgements(TimeSpan.FromMilliseconds(450));
        await using var provider = await EtcdProvider.ConnectAsync(new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = ns,
            ExpectedEpoch = epoch,
            HttpHandlerFactory = () => delayedAcks,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(3),
                    RpcTimeout = TimeSpan.FromMilliseconds(500),
                    CleanupTimeout = TimeSpan.FromMilliseconds(500)
                }
            }
        });
        var lease = await provider.Lock("cleanup-deadline").AcquireAsync();

        var started = Stopwatch.GetTimestamp();
        var outcome = await lease.ReleaseAsync();
        var elapsed = Stopwatch.GetElapsedTime(started);

        Assert.Equal(ReleaseOutcome.Released, outcome);
        Assert.True(elapsed < TimeSpan.FromMilliseconds(750), $"Cleanup took {elapsed}; its single budget is 500 ms.");
        Assert.Contains(delayedAcks.Paths, path => path.EndsWith("/Unlock", StringComparison.Ordinal));
        Assert.Contains(delayedAcks.Paths, path => path.EndsWith("/LeaseRevoke", StringComparison.Ordinal));
    }

    [EtcdIntegrationFact]
    public async Task AppliedUnlockWithWithheldAcknowledgementIsNotRetriedAndLeaseRevokeCleansSuccessor()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var ns = "it-lost-unlock-ack-" + Guid.NewGuid().ToString("N");
        var epoch = "local-lost-unlock-ack-" + Guid.NewGuid().ToString("N");
        var cleanupObservations = new List<CleanupObservation>();
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(adminChannel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        var fault = new WithholdUnlockAcknowledgementHandler();
        await using var ownerProvider = await EtcdProvider.ConnectAsync(new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = ns,
            ExpectedEpoch = epoch,
            HttpHandlerFactory = () => fault,
            CleanupObserver = cleanupObservations.Add,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(3),
                    RpcTimeout = TimeSpan.FromMilliseconds(500),
                    CleanupTimeout = TimeSpan.FromSeconds(2)
                }
            }
        });
        await using var successorProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        var owner = await ownerProvider.Lock("lost-unlock-ack").AcquireAsync();

        // The handler first receives the complete successful etcd Unlock response,
        // then withholds it from gRPC. This models a response lost after application.
        await owner.DisposeAsync();
        Assert.Equal(ReleaseOutcome.AlreadyNotOwned, await owner.ReleaseAsync());
        Assert.Equal(LeaseState.Released, owner.State);
        var observation = Assert.Single(cleanupObservations);
        Assert.Equal("etcd-native-lock-v1", observation.Backend);
        Assert.Equal(ReleaseOutcome.AlreadyNotOwned, observation.Outcome);
        Assert.Equal("lease-dispose", observation.Reason);
        Assert.Equal(1, fault.AppliedUnlockResponses);
        Assert.Equal(1, fault.WithheldUnlockResponses);
        Assert.Equal(1, fault.UnlockRequests);
        Assert.Equal(1, fault.RevokeRequests);
        Assert.Equal(1, fault.AcknowledgedRevokes);

        var successor = await successorProvider.Lock("lost-unlock-ack").AcquireAsync(TimeSpan.FromSeconds(2), new LeaseOptions
        {
            LeaseDuration = TimeSpan.FromSeconds(3),
            RpcTimeout = TimeSpan.FromMilliseconds(500),
            CleanupTimeout = TimeSpan.FromSeconds(2)
        });
        Assert.True(((IEtcdLease)successor).CreateRevision > ((IEtcdLease)owner).CreateRevision);
        Assert.Equal(ReleaseOutcome.Released, await successor.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task AppliedUnlockAndRevokeWithWithheldAcknowledgementsRemainUnknownWithoutRetry()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var ns = "it-lost-cleanup-acks-" + Guid.NewGuid().ToString("N");
        var epoch = "local-lost-cleanup-acks-" + Guid.NewGuid().ToString("N");
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(adminChannel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        var fault = new WithholdUnlockAcknowledgementHandler(withholdRevokeAcknowledgement: true);
        await using var ownerProvider = await EtcdProvider.ConnectAsync(new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = ns,
            ExpectedEpoch = epoch,
            HttpHandlerFactory = () => fault,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(3),
                    RpcTimeout = TimeSpan.FromMilliseconds(500),
                    CleanupTimeout = TimeSpan.FromSeconds(2)
                }
            }
        });
        await using var successorProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        var owner = await ownerProvider.Lock("lost-cleanup-acks").AcquireAsync();

        // Both successful etcd cleanup responses are consumed by the handler
        // and withheld before the gRPC client receives either acknowledgement.
        var outcome = await owner.ReleaseAsync();
        var repeatedOutcome = await owner.ReleaseAsync();
        Assert.Equal(ReleaseOutcome.Unknown, outcome);
        Assert.Equal(outcome, repeatedOutcome);
        Assert.Equal(LeaseState.ReleasedUnknown, owner.State);
        Assert.Equal(1, fault.AppliedUnlockResponses);
        Assert.Equal(1, fault.WithheldUnlockResponses);
        Assert.Equal(1, fault.UnlockRequests);
        Assert.Equal(1, fault.RevokeRequests);
        Assert.Equal(1, fault.AppliedRevokeResponses);
        Assert.Equal(1, fault.WithheldRevokeResponses);
        Assert.Equal(0, fault.AcknowledgedRevokes);

        var successor = await successorProvider.Lock("lost-cleanup-acks").AcquireAsync(TimeSpan.FromSeconds(2), new LeaseOptions
        {
            LeaseDuration = TimeSpan.FromSeconds(3),
            RpcTimeout = TimeSpan.FromMilliseconds(500),
            CleanupTimeout = TimeSpan.FromSeconds(2)
        });
        Assert.True(((IEtcdLease)successor).CreateRevision > ((IEtcdLease)owner).CreateRevision);
        Assert.Equal(ReleaseOutcome.Released, await successor.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task NativeQueueUsesDistinctLeasesAndStaleReleaseCannotUnlockSuccessor()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;

        var ns = "it-" + Guid.NewGuid().ToString("N");
        const string epoch = "local-integration-epoch";
        using var channel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(channel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        await using var firstProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        await using var nextProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        var options = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(3), RpcTimeout = TimeSpan.FromMilliseconds(250), CleanupTimeout = TimeSpan.FromSeconds(2) };
        var unsupported = await Assert.ThrowsAsync<DilockException>(() => firstProvider.Lock("try-resource").TryAcquireAsync(options));
        Assert.Equal(FailureKind.UnsupportedCapability, unsupported.Kind);
        var first = Assert.IsAssignableFrom<IEtcdLease>(await firstProvider.Lock("queued-resource").AcquireAsync(TimeSpan.FromSeconds(2), options));
        Assert.True(first.CreateRevision > 0);
        Assert.Equal(epoch, first.Epoch);
        var waitFailure = await Assert.ThrowsAsync<DilockException>(() => nextProvider.Lock("queued-resource").AcquireAsync(TimeSpan.FromMilliseconds(250), options));
        Assert.Equal(FailureKind.WaitTimeout, waitFailure.Kind);

        Assert.Equal(ReleaseOutcome.Released, await first.ReleaseAsync());
        var successor = Assert.IsAssignableFrom<IEtcdLease>(await nextProvider.Lock("queued-resource").AcquireAsync(TimeSpan.FromSeconds(2), options));
        Assert.True(successor.CreateRevision > first.CreateRevision);
        Assert.Equal(ReleaseOutcome.Released, await first.ReleaseAsync());
        Assert.Equal(LeaseState.Held, successor.State);
        Assert.Equal(ReleaseOutcome.Released, await successor.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task NaturallyExpiredLeaseIsNotReportedAsReleased()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;
        var ns = "it-expired-release-" + Guid.NewGuid().ToString("N");
        var epoch = "expired-release-" + Guid.NewGuid().ToString("N");
        using var adminChannel = GrpcChannel.ForAddress(endpoint);
        var admin = new KV.KVClient(adminChannel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await admin.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        await using var provider = await EtcdProvider.ConnectAsync(new EtcdProviderOptions
        {
            Endpoint = endpoint,
            Namespace = ns,
            ExpectedEpoch = epoch,
            DefaultAcquisition = new AcquireOptions
            {
                WaitTimeout = TimeSpan.FromSeconds(2),
                Lease = new LeaseOptions
                {
                    LeaseDuration = TimeSpan.FromSeconds(1),
                    AutoRenew = false,
                    RpcTimeout = TimeSpan.FromMilliseconds(500),
                    CleanupTimeout = TimeSpan.FromSeconds(2)
                }
            }
        });
        var lease = await provider.Lock("natural-expiry").AcquireAsync();

        await Task.Delay(TimeSpan.FromSeconds(2.5));

        Assert.Equal(ReleaseOutcome.AlreadyNotOwned, await lease.ReleaseAsync());
    }

    [EtcdIntegrationFact]
    public async Task KeepAliveMaintainsLockWaiterBeyondRequestedTtl()
    {
        var endpoint = Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")!;

        var ns = "it-" + Guid.NewGuid().ToString("N");
        const string epoch = "local-integration-epoch";
        using var channel = GrpcChannel.ForAddress(endpoint);
        var kv = new KV.KVClient(channel);
        var metadata = JsonSerializer.Serialize(new
        {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "etcd-native-lock-v1",
            coordination_epoch = epoch,
            fixed_lease_ms = (long?)null
        });
        await kv.PutAsync(new PutRequest
        {
            Key = ByteString.CopyFromUtf8($"/dilock/namespaces/{ns}/meta"),
            Value = ByteString.CopyFromUtf8(metadata)
        }, deadline: DateTime.UtcNow.AddSeconds(3));

        await using var ownerProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        await using var cancelledProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        await using var waiterProvider = await EtcdProvider.ConnectAsync(endpoint, ns, epoch);
        var settings = new LeaseOptions { LeaseDuration = TimeSpan.FromSeconds(1), RpcTimeout = TimeSpan.FromMilliseconds(250), CleanupTimeout = TimeSpan.FromSeconds(2), AutoRenew = true };
        var owner = await ownerProvider.Lock("long-wait").AcquireAsync(TimeSpan.FromSeconds(2), settings);
        using var cancellation = new CancellationTokenSource();
        var cancelledWait = cancelledProvider.Lock("long-wait").AcquireAsync(TimeSpan.FromSeconds(8), settings, cancellation.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(400));
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelledWait);
        var pending = waiterProvider.Lock("long-wait").AcquireAsync(TimeSpan.FromSeconds(6), settings);
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        Assert.Equal(LeaseState.Held, owner.State);
        await owner.ReleaseAsync();
        var next = await pending.WaitAsync(TimeSpan.FromSeconds(4));
        Assert.Equal(ReleaseOutcome.Released, await next.ReleaseAsync());
    }
}

internal sealed class DelayedCleanupAcknowledgements(TimeSpan delay) : DelegatingHandler(new SocketsHttpHandler())
{
    private readonly ConcurrentQueue<string> paths = new();
    public IReadOnlyCollection<string> Paths => paths.ToArray();

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri?.AbsolutePath ?? string.Empty;
        if (path.EndsWith("/Unlock", StringComparison.Ordinal) || path.EndsWith("/LeaseRevoke", StringComparison.Ordinal))
            paths.Enqueue(path);
        var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (path.EndsWith("/Unlock", StringComparison.Ordinal) || path.EndsWith("/LeaseRevoke", StringComparison.Ordinal))
        {
            try { await Task.Delay(delay, cancellationToken).ConfigureAwait(false); }
            catch { response.Dispose(); throw; }
        }
        return response;
    }
}

internal sealed class WithholdUnlockAcknowledgementHandler(bool withholdRevokeAcknowledgement = false) : DelegatingHandler(new SocketsHttpHandler())
{
    private int appliedUnlockResponses;
    private int withheldUnlockResponses;
    private int unlockRequests;
    private int revokeRequests;
    private int acknowledgedRevokes;
    private int appliedRevokeResponses;
    private int withheldRevokeResponses;

    public int AppliedUnlockResponses => Volatile.Read(ref appliedUnlockResponses);
    public int WithheldUnlockResponses => Volatile.Read(ref withheldUnlockResponses);
    public int UnlockRequests => Volatile.Read(ref unlockRequests);
    public int RevokeRequests => Volatile.Read(ref revokeRequests);
    public int AcknowledgedRevokes => Volatile.Read(ref acknowledgedRevokes);
    public int AppliedRevokeResponses => Volatile.Read(ref appliedRevokeResponses);
    public int WithheldRevokeResponses => Volatile.Read(ref withheldRevokeResponses);

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri?.AbsolutePath ?? string.Empty;
        if (path.EndsWith("/Unlock", StringComparison.Ordinal)) Interlocked.Increment(ref unlockRequests);
        if (path.EndsWith("/LeaseRevoke", StringComparison.Ordinal)) Interlocked.Increment(ref revokeRequests);

        var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (path.EndsWith("/Unlock", StringComparison.Ordinal))
        {
            var grpcStatus = await ReadGrpcStatusAndRestoreBody(response, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode || grpcStatus != "0") return response;
            Interlocked.Increment(ref appliedUnlockResponses);
            Interlocked.Increment(ref withheldUnlockResponses);
            response.Dispose();
            throw new HttpRequestException("Injected lost Unlock acknowledgement after etcd returned success.");
        }
        if (path.EndsWith("/LeaseRevoke", StringComparison.Ordinal))
        {
            var grpcStatus = await ReadGrpcStatusAndRestoreBody(response, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode || grpcStatus != "0") return response;
            Interlocked.Increment(ref appliedRevokeResponses);
            if (withholdRevokeAcknowledgement && Interlocked.Exchange(ref withheldRevokeResponses, 1) == 0)
            {
                response.Dispose();
                throw new HttpRequestException("Injected lost LeaseRevoke acknowledgement after etcd returned success.");
            }
            Interlocked.Increment(ref acknowledgedRevokes);
        }
        return response;
    }

    private static async Task<string?> ReadGrpcStatusAndRestoreBody(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var original = response.Content;
        var headers = original.Headers.ToArray();
        var body = await original.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        string? grpcStatus = response.TrailingHeaders.TryGetValues("grpc-status", out var values) ? values.SingleOrDefault() : null;
        var replacement = new ByteArrayContent(body);
        foreach (var header in headers) replacement.Headers.TryAddWithoutValidation(header.Key, header.Value);
        response.Content = replacement;
        return grpcStatus;
    }
}

public sealed class EtcdIntegrationFactAttribute : FactAttribute
{
    public EtcdIntegrationFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")))
            Skip = "Set DILOCK_ETCD_ENDPOINT to a disposable local etcd v3 endpoint.";
    }
}

public sealed class EtcdDefaultNamespaceIntegrationFactAttribute : FactAttribute
{
    public EtcdDefaultNamespaceIntegrationFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DILOCK_ETCD_ENDPOINT")))
            Skip = "Set DILOCK_ETCD_ENDPOINT to a disposable local etcd v3 endpoint.";
        else if (Environment.GetEnvironmentVariable("DILOCK_API_ERGONOMICS_DEFAULT_FIXTURE") != "1")
            Skip = "Set DILOCK_API_ERGONOMICS_DEFAULT_FIXTURE=1 for the isolated default-namespace API fixture.";
    }
}
