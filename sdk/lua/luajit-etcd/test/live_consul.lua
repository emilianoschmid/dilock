local consul = require "dilock_consul_luajit"
local endpoint = os.getenv("DILOCK_CONSUL_ENDPOINT")
local namespace = os.getenv("DILOCK_CONSUL_NAMESPACE")
local epoch = os.getenv("DILOCK_CONSUL_EPOCH")
local library_path = os.getenv("DILOCK_C_LIBRARY")
if not endpoint or not namespace or not epoch or not library_path then
    print("LuaJIT Consul live lifecycle: SKIP (configure Consul endpoint, provisioned namespace/epoch and C ABI library)")
    return
end
local provider, category, detail = consul.new({
    endpoint = endpoint, namespace = namespace, epoch = epoch,
    token = os.getenv("DILOCK_CONSUL_TOKEN"), library_path = library_path,
    rpc_timeout_ms = 1000, cleanup_timeout_ms = 1000,
    default_wait_ms = 3000, default_lease_ms = 10000,
})
assert(provider, tostring(category) .. ": " .. tostring(detail))
local lock = assert(provider:lock("lua-jit-consul-live/" .. tostring(os.time())))
local first = assert(lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
assert(first.fence.coordination_epoch == epoch)
assert(first.fence.lock_index:match("^[1-9][0-9]*$"))
assert(first:is_valid() == true)
local busy, busy_status = lock:try_acquire({ lease_ms = 10000 })
assert(not busy and busy_status == "busy", tostring(busy_status))
assert(first:renew() == true)
local previous_index = first.fence.lock_index
assert(first:release() == "released")
local second = assert(lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
assert(second.fence.coordination_epoch == epoch)
assert(second.fence.lock_index ~= previous_index)
assert(second:release() == "released")
assert(provider:close())
print("LuaJIT Consul live lifecycle: PASS")
