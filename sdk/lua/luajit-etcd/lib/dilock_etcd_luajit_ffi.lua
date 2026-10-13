-- Thin LuaJIT FFI binding to Dilock C ABI v1; lock semantics stay in C++.
local ffi = require "ffi"

ffi.cdef[=[
typedef struct dilock_error { int32_t code; char message[384]; } dilock_error;
typedef struct dilock_etcd_provider dilock_etcd_provider;
typedef struct dilock_etcd_lock dilock_etcd_lock;
typedef struct dilock_etcd_lease dilock_etcd_lease;
typedef void (*dilock_cleanup_callback)(int32_t backend, int32_t outcome,
    const char *reason, void *user_data);
typedef struct dilock_etcd_options {
    const char *endpoint;
    const char *name_space;
    const char *coordination_epoch;
    const char *tls_root_certificates;
    const char *tls_client_certificate;
    const char *tls_client_private_key;
    dilock_cleanup_callback cleanup_callback;
    void *cleanup_user_data;
    int64_t rpc_timeout_ms;
    int64_t cleanup_timeout_ms;
    int64_t safety_margin_ms;
    int64_t default_wait_ms;
    int64_t default_lease_ms;
    int32_t auto_renew;
} dilock_etcd_options;
typedef struct dilock_etcd_fence { int64_t create_revision; size_t coordination_epoch_bytes; } dilock_etcd_fence;
typedef int32_t dilock_release_outcome;
uint32_t dilock_c_abi_version(void);
void dilock_etcd_options_init(dilock_etcd_options *options);
int32_t dilock_etcd_provider_create(const dilock_etcd_options *options,
    dilock_etcd_provider **out, dilock_error *error);
void dilock_etcd_provider_destroy(dilock_etcd_provider *provider);
int32_t dilock_etcd_lock_create(dilock_etcd_provider *provider, const char *resource,
    dilock_etcd_lock **out, dilock_error *error);
void dilock_etcd_lock_destroy(dilock_etcd_lock *lock);
int32_t dilock_etcd_acquire(dilock_etcd_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_etcd_lease **out, dilock_error *error);
int32_t dilock_etcd_lease_is_valid(const dilock_etcd_lease *lease);
int32_t dilock_etcd_lease_effective_ttl_ms(const dilock_etcd_lease *lease,
    int64_t *effective_ttl_ms, dilock_error *error);
int32_t dilock_etcd_lease_fence(const dilock_etcd_lease *lease, dilock_etcd_fence *fence,
    char *coordination_epoch, size_t epoch_capacity, dilock_error *error);
int32_t dilock_etcd_lease_release(dilock_etcd_lease *lease,
    dilock_release_outcome *outcome, dilock_error *error);
void dilock_etcd_lease_destroy(dilock_etcd_lease *lease);
]=]

local status_names = {
    [0] = "ok", [1] = "busy", [2] = "invalid_argument", [3] = "cancelled",
    [4] = "wait_timeout", [5] = "backend_unavailable", [6] = "acquisition_unknown",
    [7] = "protocol_mismatch", [8] = "lease_lost", [9] = "unsupported_capability",
    [10] = "internal",
}

local function status_name(status)
    return status_names[tonumber(status)] or "backend_error"
end

local function error_message(error)
    local value = ffi.string(error[0].message)
    return value ~= "" and value or nil
end

local function decimal_i64(value)
    local number = ffi.cast("int64_t", value)
    if number < 0 then error("etcd create_revision must be nonnegative", 2) end
    if number == 0 then return "0" end
    local divisor = ffi.new("int64_t", 10)
    local output = ""
    while number > 0 do
        local digit = tonumber(number % divisor)
        output = string.char(48 + digit) .. output
        number = ffi.cast("int64_t", number / divisor)
    end
    return output
end

local M = { decimal_i64 = decimal_i64 }

function M.load(path)
    local ok, lib = pcall(ffi.load, path or "dilock_c")
    if not ok then return nil, "backend_unavailable", tostring(lib) end
    local api = {}
    function api:abi_version() return tonumber(lib.dilock_c_abi_version()) end

    function api:provider_create(config)
        local options = ffi.new("dilock_etcd_options[1]")
        lib.dilock_etcd_options_init(options)
        options[0].endpoint = config.endpoint
        options[0].name_space = config.namespace
        options[0].coordination_epoch = config.epoch
        options[0].tls_root_certificates = config.tls_root_certificates
        options[0].tls_client_certificate = config.tls_client_certificate
        options[0].tls_client_private_key = config.tls_client_private_key
        options[0].rpc_timeout_ms = config.rpc_timeout_ms
        options[0].cleanup_timeout_ms = config.cleanup_timeout_ms
        options[0].safety_margin_ms = config.safety_margin_ms
        options[0].default_wait_ms = config.default_wait_ms
        options[0].default_lease_ms = config.default_lease_ms
        options[0].auto_renew = config.auto_renew and 1 or 0
        local out, error = ffi.new("dilock_etcd_provider *[1]"), ffi.new("dilock_error[1]")
        local status = lib.dilock_etcd_provider_create(options, out, error)
        if status ~= 0 then return nil, status_name(status), error_message(error) end
        return out[0]
    end

    function api:provider_destroy(provider) lib.dilock_etcd_provider_destroy(provider) end

    function api:lock_create(provider, resource)
        local out, error = ffi.new("dilock_etcd_lock *[1]"), ffi.new("dilock_error[1]")
        local status = lib.dilock_etcd_lock_create(provider, resource, out, error)
        if status ~= 0 then return nil, status_name(status), error_message(error) end
        return out[0]
    end

    function api:lock_destroy(lock) lib.dilock_etcd_lock_destroy(lock) end

    function api:acquire(lock, wait_ms, lease_ms)
        local out, error = ffi.new("dilock_etcd_lease *[1]"), ffi.new("dilock_error[1]")
        local status = lib.dilock_etcd_acquire(lock, wait_ms, lease_ms, out, error)
        if status ~= 0 then return nil, status_name(status), error_message(error) end
        return out[0]
    end

    function api:lease_info(lease)
        local fence, epoch = ffi.new("dilock_etcd_fence[1]"), ffi.new("char[1025]")
        local error = ffi.new("dilock_error[1]")
        local status = lib.dilock_etcd_lease_fence(lease, fence, epoch, 1025, error)
        if status ~= 0 then return nil, status_name(status), error_message(error) end
        if fence[0].coordination_epoch_bytes > 1024 then
            return nil, "protocol_mismatch", "native coordination epoch exceeds the C ABI buffer contract"
        end
        local ttl = ffi.new("int64_t[1]")
        status = lib.dilock_etcd_lease_effective_ttl_ms(lease, ttl, error)
        if status ~= 0 then return nil, status_name(status), error_message(error) end
        return {
            create_revision = decimal_i64(fence[0].create_revision),
            coordination_epoch = ffi.string(epoch, fence[0].coordination_epoch_bytes),
            effective_ttl_ms = tonumber(ttl[0]),
        }
    end

    function api:lease_valid(lease)
        return lib.dilock_etcd_lease_is_valid(lease) == 1
    end

    function api:lease_release(lease)
        local outcome, error = ffi.new("dilock_release_outcome[1]"), ffi.new("dilock_error[1]")
        local status = lib.dilock_etcd_lease_release(lease, outcome, error)
        if status ~= 0 then return "unknown", status_name(status), error_message(error) end
        local names = { [0] = "released", [1] = "already_not_owned", [2] = "unknown" }
        local result = names[tonumber(outcome[0])]
        if not result then return "unknown", "protocol_mismatch", "native C ABI returned an invalid release outcome" end
        return result
    end

    function api:lease_destroy(lease) lib.dilock_etcd_lease_destroy(lease) end
    return api
end

return M
