local dilock = require "dilock_etcd_luajit"

local provider, category, detail = dilock.new({
    endpoint = assert(os.getenv("DILOCK_ETCD_ENDPOINT"), "DILOCK_ETCD_ENDPOINT is required"),
    namespace = assert(os.getenv("DILOCK_ETCD_NAMESPACE"), "DILOCK_ETCD_NAMESPACE is required"),
    epoch = assert(os.getenv("DILOCK_ETCD_EPOCH"), "DILOCK_ETCD_EPOCH is required"),
    library_path = assert(os.getenv("DILOCK_C_LIBRARY"), "DILOCK_C_LIBRARY is required"),
    rpc_timeout_ms = 2000,
    cleanup_timeout_ms = 2000,
    default_wait_ms = 5000,
    default_lease_ms = 30000,
    auto_renew = true,
})
assert(provider, tostring(category) .. ": " .. tostring(detail))

local handle, lock_category, lock_detail = provider:lock("invoice:42")
assert(handle, tostring(lock_category) .. ": " .. tostring(lock_detail))

local result, status, operation_detail = handle:with_lock(function(lease)
    assert(lease:is_valid(), "new etcd lease must be valid")
    local fence = lease.fence
    assert(fence.coordination_epoch == os.getenv("DILOCK_ETCD_EPOCH"))
    assert(fence.create_revision:match("^[1-9][0-9]*$"))
    -- The protected resource must atomically reject stale epoch/revision pairs.
    return "invoice work completed at fence " .. fence.coordination_epoch .. ":" .. fence.create_revision
end)
assert(result and status == nil, tostring(status) .. ": " .. tostring(operation_detail))
assert(provider:close())

print(result)
