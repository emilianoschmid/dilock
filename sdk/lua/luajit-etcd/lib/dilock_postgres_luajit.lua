-- Synchronous LuaJIT FFI facade for Dilock's PostgreSQL C ABI route.
-- The PostgreSQL session/advisory-lock semantics remain in the native backend.
local ffi = require "ffi"
local M = {}

ffi.cdef[=[
typedef struct dilock_error { int32_t code; char message[384]; } dilock_error;
typedef struct dilock_postgres_provider dilock_postgres_provider;
typedef struct dilock_postgres_lock dilock_postgres_lock;
typedef struct dilock_postgres_lease dilock_postgres_lease;
typedef struct dilock_cancellation dilock_cancellation;
typedef void (*dilock_cleanup_callback)(int32_t backend, int32_t outcome,
    const char *reason, void *user_data);
typedef struct dilock_postgres_options {
    const char *connection_string;
    const char *name_space;
    const char *coordination_epoch;
    dilock_cleanup_callback cleanup_callback;
    void *cleanup_user_data;
    int64_t rpc_timeout_ms;
    int64_t cleanup_timeout_ms;
    int64_t poll_interval_ms;
    int64_t default_wait_ms;
    int64_t default_lease_ms;
} dilock_postgres_options;
typedef int32_t dilock_release_outcome;

uint32_t dilock_c_abi_version(void);
void dilock_postgres_options_init(dilock_postgres_options *options);
int32_t dilock_postgres_provision_namespace(const dilock_postgres_options *options, dilock_error *error);
int32_t dilock_postgres_provider_create(const dilock_postgres_options *options,
    dilock_postgres_provider **out, dilock_error *error);
void dilock_postgres_provider_destroy(dilock_postgres_provider *provider);
int32_t dilock_postgres_lock_create(dilock_postgres_provider *provider, const char *resource,
    dilock_postgres_lock **out, dilock_error *error);
void dilock_postgres_lock_destroy(dilock_postgres_lock *lock);
int32_t dilock_postgres_acquire(dilock_postgres_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_postgres_lease **out, dilock_error *error);
int32_t dilock_postgres_acquire_default(dilock_postgres_lock *lock,
    dilock_postgres_lease **out, dilock_error *error);
int32_t dilock_postgres_try_acquire(dilock_postgres_lock *lock,
    dilock_postgres_lease **out, dilock_error *error);
int32_t dilock_postgres_lease_is_valid(const dilock_postgres_lease *lease);
int32_t dilock_postgres_lease_renew(dilock_postgres_lease *lease, int32_t *renewed,
    dilock_error *error);
int32_t dilock_postgres_lease_release(dilock_postgres_lease *lease,
    dilock_release_outcome *outcome, dilock_error *error);
void dilock_postgres_lease_destroy(dilock_postgres_lease *lease);
]=]

local status_names = {
    [0] = "ok", [1] = "busy", [2] = "invalid_argument", [3] = "cancelled",
    [4] = "wait_timeout", [5] = "backend_unavailable", [6] = "acquisition_unknown",
    [7] = "protocol_mismatch", [8] = "lease_lost", [9] = "unsupported_capability",
    [10] = "internal",
}
local release_names = { [0] = "released", [1] = "already_not_owned", [2] = "unknown" }

local function status_name(status)
    return status_names[tonumber(status)] or "backend_error"
end

local function error_message(error)
    local value = ffi.string(error[0].message)
    return value ~= "" and value or nil
end

local function result(status, error)
    if tonumber(status) ~= 0 then return nil, status_name(status), error_message(error) end
    return true
end

local function validate_config(config)
    if type(config) ~= "table" then return nil, "invalid_config", "configuration must be a table" end
    local connection = config.connection_string or config.dsn
    if type(connection) ~= "string" or connection == "" or connection:find("%z") then
        return nil, "invalid_config", "connection_string must be a nonempty PostgreSQL connection string"
    end
    if type(config.epoch) ~= "string" or config.epoch == "" or config.epoch:find("%z") then
        return nil, "invalid_config", "epoch must be a nonempty string without NUL"
    end
    local namespace = config.namespace or "default"
    if type(namespace) ~= "string" or namespace == "" or namespace:find("%z") then
        return nil, "invalid_config", "namespace must be a nonempty string without NUL"
    end
    return connection, namespace
end

local function options(api, config)
    local connection, namespace, detail = validate_config(config)
    if not connection then return nil, namespace, detail end
    local value = ffi.new("dilock_postgres_options[1]")
    api.lib.dilock_postgres_options_init(value)
    value[0].connection_string = connection
    value[0].name_space = namespace
    value[0].coordination_epoch = config.epoch
    for _, key in ipairs({ "rpc_timeout_ms", "cleanup_timeout_ms", "poll_interval_ms",
                           "default_wait_ms", "default_lease_ms" }) do
        if config[key] ~= nil then
            if type(config[key]) ~= "number" or config[key] % 1 ~= 0 or config[key] < 1 then
                return nil, "invalid_config", key .. " must be a positive integer"
            end
            value[0][key] = config[key]
        end
    end
    return value
end

local Api, Provider, Handle, Lease = {}, {}, {}, {}
Api.__index, Provider.__index, Handle.__index, Lease.__index = Api, Provider, Handle, Lease

function Api:abi_version()
    return tonumber(self.lib.dilock_c_abi_version())
end

function Api:provision(config)
    local value, category, detail = options(self, config)
    if not value then return nil, category, detail end
    local error = ffi.new("dilock_error[1]")
    return result(self.lib.dilock_postgres_provision_namespace(value, error), error)
end

function Api:new(config)
    local value, category, detail = options(self, config)
    if not value then return nil, category, detail end
    local out, error = ffi.new("dilock_postgres_provider *[1]"), ffi.new("dilock_error[1]")
    local status = self.lib.dilock_postgres_provider_create(value, out, error)
    if tonumber(status) ~= 0 then return nil, status_name(status), error_message(error) end
    local provider = setmetatable({
        api = self, ptr = out[0], leases = {}, closed = false,
        default_wait_ms = config.default_wait_ms or 5000,
        default_lease_ms = config.default_lease_ms or 30000,
    }, Provider)
    return provider
end

function Provider:lock(resource)
    if self.closed then return nil, "provider_closed" end
    if type(resource) ~= "string" or resource == "" or resource:find("%z") then
        return nil, "invalid_resource"
    end
    local out, error = ffi.new("dilock_postgres_lock *[1]"), ffi.new("dilock_error[1]")
    local status = self.api.lib.dilock_postgres_lock_create(self.ptr, resource, out, error)
    if tonumber(status) ~= 0 then return nil, status_name(status), error_message(error) end
    return setmetatable({ provider = self, ptr = out[0] }, Handle)
end

function Provider:close()
    if self.closed then return true end
    for _ in pairs(self.leases) do return nil, "provider_busy" end
    self.api.lib.dilock_postgres_provider_destroy(self.ptr)
    self.ptr, self.closed = nil, true
    return true
end

function Handle:_acquire(method, wait_ms, lease_ms)
    if self.provider.closed then return nil, "provider_closed" end
    local out, error = ffi.new("dilock_postgres_lease *[1]"), ffi.new("dilock_error[1]")
    local status
    if method == "default" then
        status = self.provider.api.lib.dilock_postgres_acquire_default(self.ptr, out, error)
    elseif method == "try" then
        status = self.provider.api.lib.dilock_postgres_try_acquire(self.ptr, out, error)
    else
        status = self.provider.api.lib.dilock_postgres_acquire(self.ptr, wait_ms, lease_ms, out, error)
    end
    if tonumber(status) ~= 0 then return nil, status_name(status), error_message(error) end
    local lease = setmetatable({ provider = self.provider, ptr = out[0], released = false }, Lease)
    self.provider.leases[lease] = true
    return lease
end

function Handle:acquire(options_value)
    if options_value == nil then return self:_acquire("default") end
    if type(options_value) ~= "table" then return nil, "invalid_options" end
    local wait_ms = options_value.wait_ms or self.provider.default_wait_ms
    local lease_ms = options_value.lease_ms or self.provider.default_lease_ms
    if type(wait_ms) ~= "number" or wait_ms % 1 ~= 0 or wait_ms < 1
        or type(lease_ms) ~= "number" or lease_ms % 1 ~= 0 or lease_ms < 1 then
        return nil, "invalid_options"
    end
    return self:_acquire("explicit", wait_ms, lease_ms)
end

function Handle:try_acquire()
    return self:_acquire("try")
end

function Handle:with_lock(fn, options_value)
    if type(fn) ~= "function" then return nil, "invalid_callback" end
    local lease, category, detail = self:acquire(options_value)
    if not lease then return nil, category, detail end
    local ok, a, b, c = pcall(fn, lease)
    local outcome, release_category, release_detail = lease:release()
    if not ok then error(a, 0) end
    if outcome ~= "released" then return nil, "release_unknown", release_category or release_detail end
    return a, b, c
end

function Handle:destroy()
    if self.ptr ~= nil then
        self.provider.api.lib.dilock_postgres_lock_destroy(self.ptr)
        self.ptr = nil
    end
end

function Lease:is_valid()
    return not self.released and self.ptr ~= nil
        and self.provider.api.lib.dilock_postgres_lease_is_valid(self.ptr) == 1
end

function Lease:renew()
    if self.released then return false, "lease_lost" end
    local renewed, error = ffi.new("int32_t[1]"), ffi.new("dilock_error[1]")
    local status = self.provider.api.lib.dilock_postgres_lease_renew(self.ptr, renewed, error)
    if tonumber(status) ~= 0 then return nil, status_name(status), error_message(error) end
    return renewed[0] == 1
end

function Lease:release()
    if self.released then return self.outcome end
    local outcome, error = ffi.new("dilock_release_outcome[1]"), ffi.new("dilock_error[1]")
    local status = self.provider.api.lib.dilock_postgres_lease_release(self.ptr, outcome, error)
    local name = tonumber(status) == 0 and release_names[tonumber(outcome[0])] or "unknown"
    if not name then name = "unknown" end
    self.released, self.outcome = true, name
    self.provider.leases[self] = nil
    self.release_category, self.release_detail = status_name(status), error_message(error)
    return name, self.release_category, self.release_detail
end

function Lease:destroy()
    if self.ptr ~= nil then
        self.provider.api.lib.dilock_postgres_lease_destroy(self.ptr)
        self.ptr = nil
    end
end

function M.load(path)
    local ok, lib = pcall(ffi.load, path or "dilock_c")
    if not ok then return nil, "backend_unavailable", tostring(lib) end
    return setmetatable({ lib = lib }, Api)
end

function M.new(config)
    local api, category, detail = M.load(config and config.library_path)
    if not api then return nil, category, detail end
    return api:new(config)
end

return M
