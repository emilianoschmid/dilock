using Dilock

provider = ConsulProvider(
    endpoint=get(ENV, "CONSUL_HTTP_ADDR", "http://127.0.0.1:8500"),
    namespace="reports",
    epoch=ENV["DILOCK_COORDINATION_EPOCH"],
    token=get(ENV, "CONSUL_HTTP_TOKEN", nothing),
)

# Run this once as an explicit operator-controlled setup step before normal use.
provision_consul_namespace(provider)
connect_consul(provider)
daily = lock(provider, "daily")

try
    with_lease(daily; wait_ms=5_000, lease_ms=30_000) do lease
        fence_token = fence(lease)
        # Atomically compare/register fence_token at the protected resource.
        renew!(lease) # renewal is explicit in this Julia preview
    end
finally
    close(provider)
end
