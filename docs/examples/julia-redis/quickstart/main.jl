using Dilock
using Redis

connection = RedisConnection(host=get(ENV, "DILOCK_REDIS_HOST", "127.0.0.1"),
                             port=parse(Int, get(ENV, "DILOCK_REDIS_PORT", "6379")))
namespace = get(ENV, "DILOCK_NAMESPACE", "default")
epoch = get(ENV, "DILOCK_COORDINATION_EPOCH", "local-redis-epoch")
provider = Provider(connection, namespace, epoch)
try
    # Handles are resource identities and reuse the same caller-owned connection.
    daily = Dilock.lock(provider, "reports/daily")
    with_lease(daily) do lease
        println("Default: perform short protected work")
    end
    weekly = Dilock.lock(provider, "reports/weekly")
    with_lease(weekly; wait_ms=3_000, lease_ms=20_000) do lease
        println("Configured: perform short protected work")
    end
finally
    disconnect(connection)
end
