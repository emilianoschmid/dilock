-- Blocking native calls run only in ngx.run_worker_thread's isolated Lua VM.
local B = require "dilock_etcd_ffi"
local ffi, lib = B.ffi, B.lib
local M = {}
local function ptr(ctype, hi, lo) return B.pointer_from(ctype, hi, lo) end
local function errbox() return ffi.new("dilock_error[1]") end
local function discard_lease_after_acquire_failure(lease, cause_status, cause_message)
    -- An accessor can fail after native acquisition already succeeded. Preserve
    -- that original error, but do not report it as an ordinary failure if the
    -- bounded owner-specific cleanup is itself unacknowledged.
    local cleanup_error, outcome = errbox(), ffi.new("dilock_release_outcome[1]")
    local cleanup_status = lib.dilock_etcd_lease_release(lease, outcome, cleanup_error)
    lib.dilock_etcd_lease_destroy(lease)
    if cleanup_status ~= 0 or (outcome[0] ~= 0 and outcome[0] ~= 1) then
        local cleanup_message = B.message(cleanup_error[0])
        return 6, "acquisition succeeded but lease details failed (" .. tostring(cause_status)
            .. ": " .. tostring(cause_message) .. "); bounded lease cleanup is unconfirmed ("
            .. tostring(cleanup_status) .. ", outcome " .. tostring(outcome[0]) .. "): "
            .. cleanup_message
    end
    return cause_status, cause_message
end

function M.abi_version()
    return tonumber(lib.dilock_c_abi_version())
end

function M.create_cancellation()
    local err = errbox()
    local cancellation = lib.dilock_cancellation_create(err)
    if cancellation == nil then
        return tonumber(err[0].code), B.message(err[0])
    end
    local hi, lo = B.address_pair(cancellation)
    return 0, "", hi, lo
end

function M.request_cancellation(cancel_hi, cancel_lo)
    lib.dilock_cancellation_request(ptr("dilock_cancellation *", cancel_hi, cancel_lo))
    return true
end

function M.destroy_cancellation(cancel_hi, cancel_lo)
    lib.dilock_cancellation_destroy(ptr("dilock_cancellation *", cancel_hi, cancel_lo))
    return true
end

function M.lease_valid(lease_hi, lease_lo)
    return lib.dilock_etcd_lease_is_valid(ptr("dilock_etcd_lease *", lease_hi, lease_lo)) == 1
end

function M.create_provider(config)
    local err, options = errbox(), ffi.new("dilock_etcd_options[1]")
    lib.dilock_etcd_options_init(options)
    options[0].endpoint, options[0].name_space = config.endpoint, config.namespace
    options[0].coordination_epoch = config.epoch
    options[0].tls_root_certificates = config.tls_root_certificates
    options[0].tls_client_certificate = config.tls_client_certificate
    options[0].tls_client_private_key = config.tls_client_private_key
    options[0].rpc_timeout_ms, options[0].cleanup_timeout_ms = config.rpc_timeout_ms, config.cleanup_timeout_ms
    options[0].safety_margin_ms, options[0].default_wait_ms = config.safety_margin_ms, config.default_wait_ms
    options[0].default_lease_ms, options[0].auto_renew = config.default_lease_ms, config.auto_renew and 1 or 0
    local out = ffi.new("dilock_etcd_provider *[1]")
    local status = lib.dilock_etcd_provider_create(options, out, err)
    if status ~= 0 then return status, B.message(err[0]) end
    local hi, lo = B.address_pair(out[0])
    return 0, "", hi, lo
end

function M.create_lock(provider_hi, provider_lo, resource)
    local provider, err = ptr("dilock_etcd_provider *", provider_hi, provider_lo), errbox()
    local out = ffi.new("dilock_etcd_lock *[1]")
    local status = lib.dilock_etcd_lock_create(provider, resource, out, err)
    if status ~= 0 then return status, B.message(err[0]) end
    local hi, lo = B.address_pair(out[0])
    return 0, "", hi, lo
end

function M.acquire(provider_hi, provider_lo, lock_hi, lock_lo,
        cancel_hi, cancel_lo, wait_ms, lease_ms)
    -- Provider and lock remain owned by the caller; only a successful lease
    -- pointer is transferred back over the run_worker_thread primitive ABI.
    local provider = ptr("dilock_etcd_provider *", provider_hi, provider_lo)
    local lock = ptr("dilock_etcd_lock *", lock_hi, lock_lo)
    local cancel = ptr("dilock_cancellation *", cancel_hi, cancel_lo)
    local err, out = errbox(), ffi.new("dilock_etcd_lease *[1]")
    local status = lib.dilock_etcd_acquire_with_cancellation(lock, wait_ms, lease_ms, cancel, out, err)
    if status ~= 0 then return status, B.message(err[0]) end
    local lease = out[0]
    local fence, epoch, ttl = ffi.new("dilock_etcd_fence[1]"), ffi.new("uint8_t[1025]"), ffi.new("int64_t[1]")
    status = lib.dilock_etcd_lease_fence(lease, fence, ffi.cast("char *", epoch), 1025, err)
    if status == 0 then status = lib.dilock_etcd_lease_effective_ttl_ms(lease, ttl, err) end
    if status ~= 0 then
        local cause_status, cause_message = status, B.message(err[0])
        return discard_lease_after_acquire_failure(lease, cause_status, cause_message)
    end
    local hi, lo = B.address_pair(lease)
    return 0, "", hi, lo, B.decimal_i64(fence[0].create_revision),
        ffi.string(epoch, fence[0].coordination_epoch_bytes), B.decimal_i64(ttl[0])
end

function M.release(lease_hi, lease_lo)
    local lease, err = ptr("dilock_etcd_lease *", lease_hi, lease_lo), errbox()
    local outcome = ffi.new("dilock_release_outcome[1]")
    local status = lib.dilock_etcd_lease_release(lease, outcome, err)
    local message = B.message(err[0])
    lib.dilock_etcd_lease_destroy(lease)
    return status, tonumber(outcome[0]), message
end
function M.discard_acquired(lease_hi, lease_lo) return M.release(lease_hi, lease_lo) end

function M.close_provider(provider_hi, provider_lo, lock_pairs)
    local provider = ptr("dilock_etcd_provider *", provider_hi, provider_lo)
    for _, pair in ipairs(lock_pairs or {}) do
        lib.dilock_etcd_lock_destroy(ptr("dilock_etcd_lock *", pair[1], pair[2]))
    end
    lib.dilock_etcd_provider_destroy(provider)
    return true, "closed"
end
return M
