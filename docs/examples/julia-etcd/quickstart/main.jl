using Dilock

provider = EtcdProvider(ENV["DILOCK_ETCD_ENDPOINT"], "billing",
                        ENV["DILOCK_COORDINATION_EPOCH"];
                        token=get(ENV, "DILOCK_ETCD_TOKEN", nothing))
handle = Dilock.lock(provider, "invoice/42")

with_lease(handle) do lease
    println("Default etcd fence: ", fence(lease))
    # Check the fence atomically at the protected resource before writing.
end

configured = Dilock.lock(provider, "invoice/43")
with_lease(configured; wait_ms=3_000, lease_ms=20_000, auto_renew=true) do lease
    println("Configured etcd fence: ", fence(lease))
end
