local dilock = require "dilock_etcd_luajit"

local endpoint = assert(os.getenv("DILOCK_ETCD_ENDPOINT"), "DILOCK_ETCD_ENDPOINT is required")
local namespace = assert(os.getenv("DILOCK_ETCD_NAMESPACE"), "DILOCK_ETCD_NAMESPACE is required")
local epoch = assert(os.getenv("DILOCK_ETCD_EPOCH"), "DILOCK_ETCD_EPOCH is required")
local library_path = assert(os.getenv("DILOCK_C_LIBRARY"), "DILOCK_C_LIBRARY is required")

local missing_provider, missing_status, missing_detail = dilock.new({
    endpoint = endpoint,
    namespace = namespace .. "-missing",
    epoch = epoch,
    library_path = library_path,
    rpc_timeout_ms = 500,
    cleanup_timeout_ms = 1000,
})
assert(not missing_provider and missing_status == "protocol_mismatch",
    "unprovisioned metadata must fail without creating a provider (status="
        .. tostring(missing_status) .. ", detail=" .. tostring(missing_detail) .. ")")

local provider, category, detail = dilock.new({
    endpoint = endpoint,
    namespace = namespace,
    epoch = epoch,
    library_path = library_path,
    rpc_timeout_ms = 500,
    cleanup_timeout_ms = 1000,
    safety_margin_ms = 25,
    default_wait_ms = 2000,
    default_lease_ms = 1500,
    auto_renew = true,
})
assert(provider, tostring(category) .. ": " .. tostring(detail))

local function release_and_close(lease)
    if lease then
        local outcome, release_detail = lease:release()
        assert(outcome == "released", tostring(outcome) .. ": " .. tostring(release_detail))
    end
    assert(provider:close())
end

local lock = assert(provider:lock("lua-jit-live"))
local lease, acquire_error, acquire_detail = lock:acquire({ wait_ms = 2000, lease_ms = 1500 })
assert(lease, tostring(acquire_error) .. ": " .. tostring(acquire_detail))
assert(lease.fence.coordination_epoch == epoch, "fence epoch must match provisioned namespace metadata")
assert(lease.fence.create_revision:match("^[1-9][0-9]*$"), "fence revision must be an exact positive decimal string")
assert(lease:is_valid() == true, "newly acquired lease must be valid")
local closed, close_error = provider:close()
assert(not closed and close_error:find("provider_busy", 1, true), "provider must reject close while lease is active")

-- Keep the critical section beyond the requested TTL to check native keepalive.
os.execute("sleep 2")
assert(lease:is_valid() == true, "auto-renewed lease must remain valid after its requested TTL")
local first_revision = lease.fence.create_revision
local outcome, release_detail = lease:release()
assert(outcome == "released", tostring(outcome) .. ": " .. tostring(release_detail))
assert(lease:is_valid() == false, "released lease must no longer be valid")

local second = assert(lock:acquire({ wait_ms = 2000, lease_ms = 1500 }))
assert(second.fence.coordination_epoch == epoch, "successor must retain the same coordination epoch")
assert(second.fence.create_revision ~= first_revision, "successor lock must have a distinct etcd revision")
local second_outcome, second_detail = second:release()
assert(second_outcome == "released", tostring(second_outcome) .. ": " .. tostring(second_detail))

local scoped_result, scoped_status, scoped_detail = lock:with_lock(function(scoped_lease)
    assert(scoped_lease:is_valid() == true, "with_lock must expose a live native lease")
    assert(scoped_lease.fence.coordination_epoch == epoch,
        "with_lock must preserve the verified epoch fence")
    return "scoped-work-complete"
end, { wait_ms = 2000, lease_ms = 1500 })
assert(scoped_result == "scoped-work-complete" and scoped_status == nil,
    tostring(scoped_status) .. ": " .. tostring(scoped_detail))
assert(provider:close())

print("LuaJIT installed-package etcd lifecycle: PASS")
