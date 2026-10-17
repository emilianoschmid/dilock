-- LuaJIT facade over the shared Dilock C ABI Consul engine.
local native = require "dilock_etcd_luajit_ffi"
local M, Provider, Lock, Lease = {}, {}, {}, {}
Provider.__index, Lock.__index, Lease.__index = Provider, Lock, Lease
local MAX_MS = 86400000

local function integer(value, low, high)
    return type(value) == "number" and value == value and value % 1 == 0 and value >= low and value <= high
end
local function configured(value, fallback) if value == nil then return fallback end; return value end
local function valid_namespace(value)
    return type(value) == "string" and #value >= 1 and #value <= 64 and value:match("^[a-z0-9][a-z0-9_.%-]*$") ~= nil
end
local function validate(config)
    if type(config) ~= "table" then return nil, "invalid_config" end
    local namespace = configured(config.namespace, "default")
    local endpoint = configured(config.endpoint, "http://127.0.0.1:8500")
    local epoch, token = config.epoch, config.token
    local rpc, cleanup = configured(config.rpc_timeout_ms, 2000), configured(config.cleanup_timeout_ms, 2000)
    local poll, wait, lease = configured(config.poll_interval_ms, 100), configured(config.default_wait_ms, 5000), configured(config.default_lease_ms, 30000)
    if not valid_namespace(namespace) then return nil, "invalid_namespace" end
    if type(endpoint) ~= "string" or not endpoint:match("^https?://[^/]+/?$") or endpoint:find("%z") then return nil, "invalid_endpoint" end
    if type(epoch) ~= "string" or #epoch < 1 or #epoch > 1024 or epoch:find("%z") then return nil, "invalid_epoch" end
    if token ~= nil and (type(token) ~= "string" or token == "" or token:find("[%c]")) then return nil, "invalid_token" end
    if not integer(rpc, 1, MAX_MS) then return nil, "invalid_rpc_timeout_ms" end
    if not integer(cleanup, 1, MAX_MS) then return nil, "invalid_cleanup_timeout_ms" end
    if not integer(poll, 1, MAX_MS) then return nil, "invalid_poll_interval_ms" end
    if not integer(wait, 1, MAX_MS) then return nil, "invalid_default_wait_ms" end
    if not integer(lease, 10000, MAX_MS) or lease <= rpc + math.max(100, math.ceil(lease * 0.01)) then return nil, "invalid_default_lease_ms" end
    if config.library_path ~= nil and (type(config.library_path) ~= "string" or config.library_path == "") then return nil, "invalid_library_path" end
    return { endpoint = endpoint, namespace = namespace, epoch = epoch, token = token,
        rpc_timeout_ms = rpc, cleanup_timeout_ms = cleanup, poll_interval_ms = poll,
        default_wait_ms = wait, default_lease_ms = lease }
end
M.validate = validate

local function call(api, name, ...)
    local ok, a, b, c = pcall(api[name], api, ...)
    if not ok then return nil, "internal", tostring(a) end
    return a, b, c
end
function M.provision(config)
    local normalized, err = validate(config)
    if not normalized then return nil, err end
    local api, status, detail = native.load_consul(config.library_path)
    if not api then return nil, status, detail end
    if api:abi_version() ~= 1 then return nil, "unsupported_c_abi_version" end
    return call(api, "provision", normalized)
end
function M.new(config)
    local normalized, err = validate(config)
    if not normalized then return nil, err end
    local api, status, detail = native.load_consul(config.library_path)
    if not api then return nil, status, detail end
    if api:abi_version() ~= 1 then return nil, "unsupported_c_abi_version" end
    local handle, create_status, create_detail = call(api, "provider_create", normalized)
    if not handle then return nil, create_status, create_detail end
    return setmetatable({ _api=api, _native=handle, _config=normalized, _locks={}, _active=0, _closed=false }, Provider)
end
function Provider:lock(resource)
    if self._closed then return nil, "provider_closed" end
    if type(resource) ~= "string" or #resource < 1 or #resource > 1024 or resource:find("%z") then return nil, "invalid_resource" end
    if self._locks[resource] then return self._locks[resource] end
    local handle, status, detail = call(self._api, "lock_create", self._native, resource)
    if not handle then return nil, status, detail end
    local lock = setmetatable({ provider=self, resource=resource, _native=handle }, Lock)
    self._locks[resource] = lock
    return lock
end
local function acquire(lock, mode, options)
    local provider = lock.provider
    if provider._closed then return nil, "provider_closed" end
    options = options or {}
    if type(options) ~= "table" then return nil, "invalid_options" end
    local wait, ttl = configured(options.wait_ms, provider._config.default_wait_ms), configured(options.lease_ms, provider._config.default_lease_ms)
    if not integer(ttl, 10000, MAX_MS) or ttl <= provider._config.rpc_timeout_ms + math.max(100, math.ceil(ttl * 0.01)) then return nil, "invalid_lease_ms" end
    if mode ~= "try" and not integer(wait, 1, MAX_MS) then return nil, "invalid_wait_ms" end
    local lease, status, detail
    if mode == "try" then lease, status, detail = call(provider._api, "try_acquire", lock._native, ttl)
    else lease, status, detail = call(provider._api, "acquire", lock._native, wait, ttl) end
    if not lease then return nil, status, detail end
    local info, info_status, info_detail = call(provider._api, "lease_info", lease)
    if not info then
        local outcome = call(provider._api, "lease_release", lease)
        call(provider._api, "lease_destroy", lease)
        if outcome ~= "released" and outcome ~= "already_not_owned" then return nil, "acquisition_unknown", "fence read failed and lease cleanup was not acknowledged" end
        return nil, info_status or "protocol_mismatch", info_detail
    end
    if type(info.lock_index) ~= "string" or not info.lock_index:match("^[0-9]+$") or info.lock_index == "0" or info.coordination_epoch ~= provider._config.epoch or not integer(info.effective_ttl_ms, 1, MAX_MS) then
        call(provider._api, "lease_release", lease); call(provider._api, "lease_destroy", lease)
        return nil, "protocol_mismatch", "native ABI returned invalid Consul lease evidence"
    end
    provider._active = provider._active + 1
    return setmetatable({ provider=provider, _native=lease, fence={lock_index=info.lock_index, coordination_epoch=info.coordination_epoch}, effective_ttl_ms=info.effective_ttl_ms }, Lease)
end
function Lock:acquire(options) return acquire(self, "wait", options) end
function Lock:try_acquire(options) return acquire(self, "try", options) end
function Lease:is_valid()
    if self._released then return false end
    return call(self.provider._api, "lease_valid", self._native)
end
function Lease:renew()
    if self._released then return false, "lease_released" end
    local renewed, status, detail = call(self.provider._api, "lease_renew", self._native)
    if renewed ~= true then self._lost = true end
    return renewed, status, detail
end
function Lease:release()
    if self._released then return self._outcome, self._detail end
    local outcome, status, detail = call(self.provider._api, "lease_release", self._native)
    if outcome ~= "released" and outcome ~= "already_not_owned" and outcome ~= "unknown" then outcome, detail = "unknown", detail or status end
    local _, destroy_status, destroy_detail = call(self.provider._api, "lease_destroy", self._native)
    self._native, self._released, self._outcome = nil, true, outcome
    self._detail = detail or destroy_detail or destroy_status
    self.provider._active = math.max(0, self.provider._active - 1)
    return outcome, self._detail
end
function Lock:with_lock(fn, options)
    if type(fn) ~= "function" then return nil, "invalid_callback" end
    local lease, status, detail = self:acquire(options)
    if not lease then return nil, status, detail end
    local ok, value = xpcall(function() return fn(lease) end, function(err) return err end)
    local outcome, release_detail = lease:release()
    if outcome ~= "released" then return nil, outcome, release_detail end
    if not ok then error(value, 0) end
    return value
end
function Provider:close()
    if self._closed then return true end
    if self._active > 0 then return nil, "provider_busy" end
    for _, lock in pairs(self._locks) do call(self._api, "lock_destroy", lock._native) end
    self._locks = {}
    call(self._api, "provider_destroy", self._native)
    self._native, self._closed = nil, true
    return true
end
return M
