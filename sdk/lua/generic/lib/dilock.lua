local redis = require "redis"
local json = require "dkjson"
local digest = require "openssl.digest"
local random = require "openssl.rand"
local socket = require "socket"
local system = require "system"

local M = {}
local Provider, Handle, Lease = {}, {}, {}
Provider.__index, Handle.__index, Lease.__index = Provider, Handle, Lease

local RELEASE = [[
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
]]

local function valid_utf8(value)
    local i, n = 1, #value
    while i <= n do
        local a = value:byte(i)
        if a <= 0x7f then i = i + 1
        elseif a >= 0xc2 and a <= 0xdf then
            local b = value:byte(i + 1)
            if not b or b < 0x80 or b > 0xbf then return false end
            i = i + 2
        elseif a == 0xe0 then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0xa0 or b > 0xbf or c < 0x80 or c > 0xbf then return false end
            i = i + 3
        elseif (a >= 0xe1 and a <= 0xec) or (a >= 0xee and a <= 0xef) then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0x80 or b > 0xbf or c < 0x80 or c > 0xbf then return false end
            i = i + 3
        elseif a == 0xed then
            local b, c = value:byte(i + 1, i + 2)
            if not b or not c or b < 0x80 or b > 0xbf or c < 0x80 or c > 0x9f then return false end
            i = i + 3
        elseif a == 0xf0 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x90 or b > 0xbf or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        elseif a >= 0xf1 and a <= 0xf3 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x80 or b > 0xbf
                or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        elseif a == 0xf4 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x80 or b > 0x8f or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        else return false end
    end
    return true
end

local function u32be(value)
    return string.char(math.floor(value / 16777216) % 256, math.floor(value / 65536) % 256,
        math.floor(value / 256) % 256, value % 256)
end

local function identity(namespace, resource)
    if type(namespace) ~= "string" or #namespace < 1 or #namespace > 64
        or not namespace:match("^[a-z0-9][a-z0-9_.%-]*$") then
        return nil, "invalid_namespace"
    end
    if type(resource) ~= "string" or #resource < 1 or #resource > 1024
        or resource:find("%z") or not valid_utf8(resource) then
        return nil, "invalid_resource"
    end
    local hash = digest.new("sha256")
    hash:update("dilock.lock.identity\0" .. u32be(#namespace) .. namespace .. u32be(#resource) .. resource)
    local raw = hash:final()
    return (raw:gsub(".", function(byte) return string.format("%02x", byte:byte()) end))
end

M.resource_id = identity

local function close(client)
    if client and client.network and client.network.socket then
        pcall(client.network.socket.close, client.network.socket)
    end
end

local function connect(provider)
    local ok, client = pcall(redis.connect, {
        host = provider.host,
        port = provider.port,
        timeout = provider.rpc_timeout_ms / 1000,
        tcp_nodelay = true,
    })
    -- Lua Redis clients commonly report transport failures as `nil, err`
    -- instead of throwing. pcall only captures thrown errors, so check both
    -- return channels before treating a connection as usable.
    if not ok or not client then return nil, "backend_unavailable" end
    if provider.password then
        local auth_ok, reply
        if provider.username then
            auth_ok, reply = pcall(client.auth, client, provider.username, provider.password)
        else
            auth_ok, reply = pcall(client.auth, client, provider.password)
        end
        if not auth_ok or reply ~= true then close(client); return nil, "backend_unavailable" end
    end
    return client
end

local function cleanup_owner(provider, key, owner)
    local client = connect(provider)
    if not client then return false end
    local ok, result = pcall(client.eval, client, RELEASE, 1, key, owner)
    close(client)
    return ok and (result == 0 or result == 1)
end

local function validate_metadata(provider, client)
    local key = "dilock:namespace:" .. provider.namespace .. ":meta"
    local type_ok, key_type, type_err = pcall(client.type, client, key)
    if not type_ok or type_err ~= nil then return nil, "backend_unavailable" end
    if key_type ~= "string" then return nil, "protocol_mismatch" end
    local get_ok, raw, get_err = pcall(client.get, client, key)
    if not get_ok or get_err ~= nil then return nil, "backend_unavailable" end
    if type(raw) ~= "string" or #raw > 4096 then return nil, "protocol_mismatch" end
    local decoded, _, err = json.decode(raw, 1, json.null)
    if err or type(decoded) ~= "table"
        or decoded.protocol_major ~= 1
        or decoded.identity_scheme ~= "sha256-framed-utf8-v1"
        or decoded.profile ~= "redis-single-authority-v1"
        or decoded.coordination_epoch ~= provider.epoch
        or decoded.fixed_lease_ms ~= json.null then
        return nil, "protocol_mismatch"
    end
    return true
end

function M.new(config)
    if type(config) ~= "table" then return nil, "invalid_config" end
    if config.cleanup_observer ~= nil and type(config.cleanup_observer) ~= "function" then
        return nil, "invalid_config"
    end
    local namespace = config.namespace or "default"
    local _, err = identity(namespace, "validation")
    if err then return nil, err end
    local epoch = config.epoch
    if type(epoch) ~= "string" or #epoch < 1 or #epoch > 1024 or epoch:find("%z") or not valid_utf8(epoch) then
        return nil, "invalid_epoch"
    end
    if config.tls then return nil, "unsupported" end
    local port = config.port or 6379
    local timeout = config.rpc_timeout_ms or 2000
    if type(port) ~= "number" or port % 1 ~= 0 or port < 1 or port > 65535 then return nil, "invalid_config" end
    if type(timeout) ~= "number" or timeout % 1 ~= 0 or timeout < 1 or timeout > 60000 then return nil, "invalid_config" end
    if config.username ~= nil and (type(config.username) ~= "string" or #config.username == 0) then return nil, "invalid_config" end
    if config.password ~= nil and (type(config.password) ~= "string" or #config.password == 0) then return nil, "invalid_config" end
    if config.username and not config.password then return nil, "invalid_config" end
    return setmetatable({
        host = config.host or "127.0.0.1", port = port,
        namespace = namespace, epoch = epoch,
        username = config.username, password = config.password,
        rpc_timeout_ms = timeout,
        cleanup_observer = config.cleanup_observer,
    }, Provider)
end

function Provider:lock(resource)
    local id, err = identity(self.namespace, resource)
    if not id then return nil, err end
    return setmetatable({ provider = self, key = "dilock:{" .. id .. "}:lease" }, Handle)
end

function Handle:try_acquire(ttl_ms)
    ttl_ms = ttl_ms or 30000
    local provider = self.provider
    if type(ttl_ms) ~= "number" or ttl_ms % 1 ~= 0 or ttl_ms < 1 or ttl_ms > 86400000 then
        return nil, "invalid_ttl"
    end
    local client, err = connect(provider)
    if not client then return nil, err end
    local meta_ok, meta_err = validate_metadata(provider, client)
    if not meta_ok then close(client); return nil, meta_err end

    local random_ok, bytes = pcall(random.bytes, 32)
    if not random_ok or type(bytes) ~= "string" or #bytes ~= 32 then close(client); return nil, "backend_unavailable" end
    local owner = "dilock/1/" .. (bytes:gsub(".", function(byte) return string.format("%02x", byte:byte()) end))
    local sent_at = system.monotime()
    local set_ok, result, set_err = pcall(client.set, client, self.key, owner, "NX", "PX", ttl_ms)
    close(client)
    if not set_ok or set_err ~= nil then
        cleanup_owner(provider, self.key, owner)
        return nil, "acquisition_unknown"
    end
    if result == nil then
        local classifier, conn_err = connect(provider)
        if not classifier then return nil, conn_err end
        local type_ok, lock_type, type_err = pcall(classifier.type, classifier, self.key)
        close(classifier)
        if not type_ok or type_err ~= nil then return nil, "backend_unavailable" end
        if lock_type ~= "string" and lock_type ~= "none" then return nil, "protocol_mismatch" end
        return nil, "busy"
    end
    if result ~= true then
        cleanup_owner(provider, self.key, owner)
        return nil, "acquisition_unknown"
    end
    local elapsed_ms = math.ceil((system.monotime() - sent_at) * 1000)
    local margin = math.min(100, math.max(1, math.ceil(ttl_ms * 0.01)))
    if elapsed_ms >= ttl_ms - margin then
        cleanup_owner(provider, self.key, owner)
        return nil, "acquisition_unknown"
    end
    return setmetatable({ provider = provider, key = self.key, owner = owner }, Lease)
end

function Lease:release()
    if self.release_outcome then return self.release_outcome, self.release_error end
    local client, err = connect(self.provider)
    if not client then
        self.release_outcome, self.release_error = "unknown", err
        return self.release_outcome, self.release_error
    end
    local ok, result, eval_err = pcall(client.eval, client, RELEASE, 1, self.key, self.owner)
    close(client)
    if not ok or eval_err ~= nil then
        self.release_outcome, self.release_error = "unknown", "backend_unavailable"
    elseif result == 1 then
        self.release_outcome = "released"
    elseif result == 0 then
        self.release_outcome = "already_not_owned"
    else
        self.release_outcome, self.release_error = "unknown", "protocol_mismatch"
    end
    return self.release_outcome, self.release_error
end

function Handle:with_lock(fn, ttl_ms)
    if type(fn) ~= "function" then return nil, "invalid_callback" end
    local lease, err = self:try_acquire(ttl_ms)
    if not lease then return nil, err end
    local ok, result = xpcall(function() return fn(lease) end, function(value) return value end)
    local outcome, release_err = lease:release()
    if outcome ~= "released" then
        local observer_err
        if self.provider.cleanup_observer then
            local observed, err = pcall(self.provider.cleanup_observer, {
                outcome = outcome,
                error = release_err,
                callback_error = not ok and result or nil,
            })
            if not observed then observer_err = err end
        end
        if not ok then
            -- Keep the callback's exact error value as `cause`. The wrapper is
            -- used only when cleanup is uncertain or ownership was lost, so
            -- callers on the normal cleanup path still receive the original error.
            local wrapped = setmetatable({
                kind = "dilock_with_lock_error",
                cause = result,
                cleanup_outcome = outcome,
                cleanup_error = release_err,
                observer_error = observer_err,
            }, { __tostring = function(value) return tostring(value.cause) end })
            error(wrapped, 0)
        end
        return nil, outcome, release_err, observer_err
    end
    if not ok then error(result, 0) end
    return result
end

return M
