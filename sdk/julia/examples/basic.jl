using Dilock
using Redis

connection = RedisConnection(host=get(ENV, "DILOCK_REDIS_HOST", "127.0.0.1"),
                             port=parse(Int, get(ENV, "DILOCK_REDIS_PORT", "6379")))
provider = Provider(connection, "default", "deployment-epoch-from-namespace-provisioning")
try
    handle = Dilock.lock(provider, "camera/123/reindex")
    with_lease(handle) do lease
        println("perform short protected work")
    end
finally
    disconnect(connection)
end
