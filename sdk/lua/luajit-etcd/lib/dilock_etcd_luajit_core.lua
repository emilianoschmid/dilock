-- Runtime-independent owner and lifetime rules for the standalone LuaJIT bridge.
local M, Provider, Lock, Lease = {}, {}, {}, {}
Provider.__index, Lock.__index, Lease.__index = Provider, Lock, Lease

local MAX_MS = 86400000
local function integer(value, minimum, maximum)
    return type(value) == "number" and value == value and value % 1 == 0
        and value >= minimum and value <= maximum
end

local function configured(value, fallback)
    if value == nil then return fallback end
    return value
end

local function valid_namespace(value)
    return type(value) == "string" and #value >= 1 and #value <= 64
        and value:match("^[a-z0-9][a-z0-9_.%-]*$") ~= nil
end

local function validate(config)
    if type(config) ~= "table" then return nil, "invalid_config" end
    if config.library_path ~= nil and (type(config.library_path) ~= "string" or config.library_path == "") then
        return nil, "invalid_library_path"
    end
    local namespace = configured(config.namespace, "default")
    local endpoint = configured(config.endpoint, "127.0.0.1:2379")
    local epoch = config.epoch
    local rpc = configured(config.rpc_timeout_ms, 2000)
    local cleanup = configured(config.cleanup_timeout_ms, 2000)
    local margin = configured(config.safety_margin_ms, 25)
    local wait = configured(config.default_wait_ms, 5000)
    local lease = configured(config.default_lease_ms, 30000)
    if not valid_namespace(namespace) then return nil, "invalid_namespace" end
    if type(endpoint) ~= "string" or #endpoint == 0 or endpoint:find("%z") then
        return nil, "invalid_endpoint"
    end
    if type(epoch) ~= "string" or #epoch == 0 or #epoch > 1024 or epoch:find("%z") then
        return nil, "invalid_epoch"
    end
    if not integer(rpc, 1, MAX_MS) then return nil, "invalid_rpc_timeout_ms" end
    if not integer(cleanup, 1, MAX_MS) then return nil, "invalid_cleanup_timeout_ms" end
    if not integer(margin, 0, MAX_MS) then return nil, "invalid_safety_margin_ms" end
    if not integer(wait, 1, MAX_MS) then return nil, "invalid_default_wait_ms" end
    if not integer(lease, 1, MAX_MS) or lease <= rpc + margin then
        return nil, "invalid_default_lease_ms"
    end
    if config.auto_renew ~= nil and type(config.auto_renew) ~= "boolean" then
        return nil, "invalid_auto_renew"
    end
    for _, name in ipairs({ "tls_root_certificates", "tls_client_certificate", "tls_client_private_key" }) do
        local value = config[name]
        if value ~= nil and (type(value) ~= "string" or value == "" or value:find("%z")) then
            return nil, "invalid_" .. name
        end
    end
    if (config.tls_client_certificate == nil) ~= (config.tls_client_private_key == nil) then
        return nil, "invalid_tls_client_identity"
    end
    return {
        namespace = namespace, endpoint = endpoint, epoch = epoch,
        rpc_timeout_ms = rpc, cleanup_timeout_ms = cleanup,
        safety_margin_ms = margin, default_wait_ms = wait,
        default_lease_ms = lease, auto_renew = config.auto_renew == true,
        tls_root_certificates = config.tls_root_certificates,
        tls_client_certificate = config.tls_client_certificate,
        tls_client_private_key = config.tls_client_private_key,
    }
end

M.validate = validate

local function api_call(api, name, ...)
    local ok, a, b, c = pcall(api[name], api, ...)
    if not ok then return nil, "internal", tostring(a) end
    return a, b, c
end

function M.new(config, api)
    local normalized, err = validate(config)
    if not normalized then return nil, err end
    if type(api) ~= "table" then return nil, "invalid_native_abi" end
    local version, status, message = api_call(api, "abi_version")
    if version == nil then return nil, status or "backend_unavailable", message end
    if version ~= 1 then return nil, "unsupported_c_abi_version" end
    local native, create_status, create_message = api_call(api, "provider_create", normalized)
    if not native then return nil, create_status or "backend_unavailable", create_message end
    return setmetatable({
        _api = api, _native = native, _config = normalized,
        _locks = {}, _active_leases = 0, _closed = false,
    }, Provider)
end

function Provider:lock(resource)
    if self._closed then return nil, "provider_closed" end
    if type(resource) ~= "string" or #resource == 0 or #resource > 1024 or resource:find("%z") then
        return nil, "invalid_resource"
    end
    local existing = self._locks[resource]
    if existing then return existing end
    local native, status, message = api_call(self._api, "lock_create", self._native, resource)
    if not native then return nil, status or "backend_unavailable", message end
    local lock = setmetatable({ provider = self, resource = resource, _native = native }, Lock)
    self._locks[resource] = lock
    return lock
end

local function release_unreturned_lease(provider, native)
    local outcome, status, message = api_call(provider._api, "lease_release", native)
    api_call(provider._api, "lease_destroy", native)
    if outcome == "released" or outcome == "already_not_owned" then
        return true
    end
    return nil, "acquisition_unknown", message or status or "lease cleanup was not acknowledged"
end

function Lock:try_acquire()
    return nil, "unsupported_capability: etcd native Lock has no safe try-acquire operation"
end

function Lock:acquire(options)
    local provider = self.provider
    if provider._closed then return nil, "provider_closed" end
    if options == nil then options = {} end
    if type(options) ~= "table" then return nil, "invalid_options" end
    local wait = configured(options.wait_ms, provider._config.default_wait_ms)
    local lease_ms = configured(options.lease_ms, provider._config.default_lease_ms)
    if not integer(wait, 1, MAX_MS) then return nil, "invalid_wait_ms" end
    if not integer(lease_ms, 1, MAX_MS)
        or lease_ms <= provider._config.rpc_timeout_ms + provider._config.safety_margin_ms then
        return nil, "invalid_lease_ms"
    end
    -- The C ABI call blocks this LuaJIT thread. Its native Lock deadline is
    -- bounded by wait_ms, followed by the provider's bounded revoke cleanup.
    local native, status, message = api_call(provider._api, "acquire", self._native, wait, lease_ms)
    if not native then return nil, status or "backend_unavailable", message end
    local info, info_status, info_message = api_call(provider._api, "lease_info", native)
    if not info then
        local _, cleanup_status, cleanup_message = release_unreturned_lease(provider, native)
        if cleanup_status then return nil, cleanup_status, cleanup_message end
        return nil, info_status or "protocol_mismatch", info_message
    end
    if type(info) ~= "table" or type(info.create_revision) ~= "string"
        or not info.create_revision:match("^[0-9]+$") or info.create_revision == "0"
        or info.coordination_epoch ~= provider._config.epoch
        or not integer(info.effective_ttl_ms, 1, MAX_MS) then
        local _, cleanup_status, cleanup_message = release_unreturned_lease(provider, native)
        if cleanup_status then return nil, cleanup_status, cleanup_message end
        return nil, "protocol_mismatch", "native C ABI returned invalid lease evidence"
    end
    provider._active_leases = provider._active_leases + 1
    return setmetatable({
        provider = provider, _native = native,
        fence = { create_revision = info.create_revision,
            coordination_epoch = info.coordination_epoch },
        effective_ttl_ms = info.effective_ttl_ms,
        _release_outcome = nil, _release_error = nil,
    }, Lease)
end

--- Run a synchronous callback while holding the etcd lease and release it
-- before returning. The callback's first return value is returned after an
-- acknowledged release. Acquisition and callback execution block this LuaJIT
-- thread; Lua coroutine cancellation cannot interrupt native calls.
-- If the callback raises and release is acknowledged, its exact error value
-- is rethrown. If both callback and release fail, a structured error table
-- preserves the callback cause and exposes cleanup_outcome/cleanup_error.
function Lock:with_lock(fn, options)
    if type(fn) ~= "function" then return nil, "invalid_callback" end
    local lease, acquire_status, acquire_detail = self:acquire(options)
    if not lease then return nil, acquire_status, acquire_detail end

    local ok, result = xpcall(function() return fn(lease) end, function(value) return value end)
    local outcome, release_detail = lease:release()
    if outcome ~= "released" then
        if not ok then
            local wrapped = setmetatable({
                kind = "dilock_with_lock_error",
                cause = result,
                cleanup_outcome = outcome,
                cleanup_error = release_detail,
            }, { __tostring = function(value) return tostring(value.cause) end })
            error(wrapped, 0)
        end
        return nil, outcome, release_detail
    end
    if not ok then error(result, 0) end
    return result
end

function Lease:is_valid()
    if self._release_outcome then return false end
    local valid, status, message = api_call(self.provider._api, "lease_valid", self._native)
    if valid == nil then return nil, status, message end
    return valid == true
end

function Lease:release()
    if self._release_outcome then return self._release_outcome, self._release_error end
    local outcome, status, message = api_call(self.provider._api, "lease_release", self._native)
    if outcome ~= "released" and outcome ~= "already_not_owned" and outcome ~= "unknown" then
        outcome, message = "unknown", message or status or "native release returned an invalid result"
    end
    local destroyed, destroy_status, destroy_message = api_call(
        self.provider._api, "lease_destroy", self._native)
    self._native = nil
    self._release_outcome = outcome
    self._release_error = message or (destroyed == nil and (destroy_message or destroy_status) or nil)
    self.provider._active_leases = math.max(0, self.provider._active_leases - 1)
    return self._release_outcome, self._release_error
end

function Provider:close()
    if self._closed then return true end
    if self._active_leases > 0 then return nil, "provider_busy: release active leases before close" end
    for _, lock in pairs(self._locks) do
        api_call(self._api, "lock_destroy", lock._native)
        lock._native = nil
    end
    self._locks = {}
    api_call(self._api, "provider_destroy", self._native)
    self._native = nil
    self._closed = true
    return true
end

return M
