local consul = require "dilock_consul_luajit"
local provider, status, detail = consul.new({
    endpoint = os.getenv("DILOCK_CONSUL_ENDPOINT") or "http://127.0.0.1:8500",
    namespace = os.getenv("DILOCK_CONSUL_NAMESPACE") or "billing",
    epoch = assert(os.getenv("DILOCK_CONSUL_EPOCH")),
    token = os.getenv("DILOCK_CONSUL_TOKEN"),
    library_path = os.getenv("DILOCK_C_LIBRARY"),
})
assert(provider, tostring(status) .. ": " .. tostring(detail))
local lock = assert(provider:lock("invoice:42"))
local result, outcome, failure = lock:with_lock(function(lease)
    assert(lease:is_valid())
    return update_invoice(lease.fence.coordination_epoch, lease.fence.lock_index)
end)
assert(outcome == nil, tostring(outcome) .. ": " .. tostring(failure))
assert(provider:close())
