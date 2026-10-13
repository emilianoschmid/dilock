using Dilock;
using Dilock.Redis;

var endpoint = Environment.GetEnvironmentVariable("DILOCK_REDIS_URL") ?? "redis://127.0.0.1:6379";
var epoch = Environment.GetEnvironmentVariable("DILOCK_EPOCH")
    ?? throw new InvalidOperationException("Set DILOCK_EPOCH to the deployment-approved namespace epoch.");
var configured = args.Contains("--configured", StringComparer.Ordinal);
var sharedProvider = args.Contains("--shared-provider", StringComparer.Ordinal);

var providerOptions = new RedisProviderOptions
{
    Endpoint = endpoint,
    Namespace = Environment.GetEnvironmentVariable("DILOCK_NAMESPACE") ?? "default",
    ExpectedEpoch = epoch,
    ConnectTimeout = TimeSpan.FromSeconds(2),
    DefaultAcquisition = new AcquireOptions
    {
        WaitTimeout = TimeSpan.FromSeconds(10),
        Lease = new LeaseOptions
        {
            LeaseDuration = TimeSpan.FromSeconds(60),
            RpcTimeout = TimeSpan.FromSeconds(2),
            CleanupTimeout = TimeSpan.FromSeconds(2),
            AutoRenew = false,
            RequireFencing = false
        }
    },
    CleanupObserver = observation =>
        Console.Error.WriteLine($"cleanup outcome={observation.Outcome}; reason={observation.Reason}")
};

await using var provider = configured
    ? await RedisProvider.ConnectAsync(providerOptions)
    : await RedisProvider.ConnectAsync(endpoint, epoch);

var daily = provider.Lock("reports/daily");
if (sharedProvider)
{
    var weekly = provider.Lock("reports/weekly");
    await using (var dailyLease = await daily.AcquireAsync())
    await using (var weeklyLease = await weekly.AcquireAsync())
    {
        await RunShortReportsAsync();
        await RequireReleasedAsync(weeklyLease, "Redis weekly lease");
        await RequireReleasedAsync(dailyLease, "Redis daily lease");
        Console.WriteLine("Two distinct resources completed and both releases were acknowledged.");
    }
}
else
{
    await using var lease = configured
        ? await daily.AcquireAsync(providerOptions.DefaultAcquisition)
        : await daily.AcquireAsync();
    await RunShortReportsAsync();
    await RequireReleasedAsync(lease, "Redis lease");
    Console.WriteLine("One short protected Redis operation completed and release was acknowledged.");
}

static Task RunShortReportsAsync() => Task.CompletedTask;

static async Task RequireReleasedAsync(ILease lease, string name)
{
    var outcome = await lease.ReleaseAsync();
    if (outcome != ReleaseOutcome.Released)
        throw new InvalidOperationException($"{name} release was not acknowledged as Released: {outcome}");
}
