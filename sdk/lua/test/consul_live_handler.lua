-- Run only against a disposable Consul namespace provisioned with this epoch.
local consul = require "dilock_consul"
local endpoint = assert(os.getenv("DILOCK_CONSUL_ENDPOINT"), "Consul endpoint required")
local namespace = assert(os.getenv("DILOCK_CONSUL_NAMESPACE"), "pre-provisioned Consul namespace required")
local epoch = assert(os.getenv("DILOCK_CONSUL_EPOCH"), "approved epoch required")
local token = os.getenv("DILOCK_CONSUL_TOKEN")
if token == "" then token = nil end
local config = { endpoint = endpoint, namespace = namespace, epoch = epoch, token = token,
    rpc_timeout_ms = 1500, cleanup_timeout_ms = 1500, default_lease_ms = 10000 }
local first_provider, err = consul.new(config)
assert(first_provider, tostring(err))
local second_provider = assert(consul.new(config))
local resource = "openresty-consul-live/" .. ngx.worker.pid() .. "/" .. ngx.now()
local first_lock = assert(first_provider:lock(resource))
local second_lock = assert(second_provider:lock(resource))
local first = assert(first_lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
assert(first.fence.coordination_epoch == epoch)
assert(first.fence.lock_index:match("^[1-9][0-9]*$"))
local busy, busy_status = second_lock:try_acquire(10000)
assert(not busy and busy_status == "busy", tostring(busy_status))
assert(first:renew())
local previous_index = first.fence.lock_index
assert(first:release() == "released")
local successor = assert(second_lock:acquire({ wait_ms = 3000, lease_ms = 10000 }))
assert(successor.fence.coordination_epoch == epoch)
assert(#successor.fence.lock_index > #previous_index or
    (#successor.fence.lock_index == #previous_index and successor.fence.lock_index > previous_index))
assert(successor:release() == "released")
assert(first_provider:close())
assert(second_provider:close())
ngx.say("PASS: OpenResty Consul contention, renewal, fencing, release and successor handoff")
