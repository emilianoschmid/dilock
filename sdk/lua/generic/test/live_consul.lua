local consul = require "dilock.consul"
local endpoint = os.getenv("DILOCK_CONSUL_ENDPOINT")
local namespace = os.getenv("DILOCK_CONSUL_NAMESPACE")
local epoch = os.getenv("DILOCK_CONSUL_EPOCH")
if not endpoint or not namespace or not epoch then
    print("Lua 5.1 Consul live lifecycle: SKIP (configure endpoint and provisioned namespace/epoch)")
    return
end
local provider, status, detail = consul.new({ endpoint = endpoint, namespace = namespace,
    epoch = epoch, token = os.getenv("DILOCK_CONSUL_TOKEN"), rpc_timeout_ms = 1000,
    cleanup_timeout_ms = 1000, poll_interval_ms = 50, default_wait_ms = 3000,
    default_lease_ms = 10000 })
assert(provider, tostring(status) .. ": " .. tostring(detail))
local lock = assert(provider:lock("lua-51-consul-live/" .. tostring(os.time())))
local first = assert(lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
assert(first.fence.coordination_epoch == epoch)
assert(first.fence.lock_index:match("^[1-9][0-9]*$"))
assert(first:is_valid())
local busy, busy_status = lock:try_acquire(10000)
assert(not busy and busy_status == "busy", tostring(busy_status))
assert(first:renew())
local first_index = first.fence.lock_index
assert(first:release() == "released")
local next_lease = assert(lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
local next_index = next_lease.fence.lock_index
assert(#next_index > #first_index or (#next_index == #first_index and next_index > first_index))
assert(next_lease:release() == "released")
assert(provider:close())
print("Lua 5.1 Consul live lifecycle: PASS")
