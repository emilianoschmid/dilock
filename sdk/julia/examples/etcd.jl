using Dilock

provider = EtcdProvider(ENV["DILOCK_ETCD_ENDPOINT"], "billing",
                        ENV["DILOCK_COORDINATION_EPOCH"];
                        token=get(ENV, "DILOCK_ETCD_TOKEN", nothing))
handle = Dilock.lock(provider, "invoice/42")

with_lease(handle; wait_ms=5_000, lease_ms=30_000, auto_renew=true) do lease
    println(fence(lease))
    # Check the fence atomically at the protected resource before writing.
end
