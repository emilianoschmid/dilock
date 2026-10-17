local redis = require "resty.redis"
local sha256 = require "resty.sha256"
local resty_random = require "resty.random"
local resty_string = require "resty.string"
local cjson = require "cjson.safe"
local monotonic_time = require "resty.core.time".monotonic_time

local _M = {}
local Provider = {}
Provider.__index = Provider
local Handle = {}
Handle.__index = Handle
local Lease = {}
Lease.__index = Lease

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
            if not b or not c or b < 0x80 or b > 0x9f or c < 0x80 or c > 0xbf then return false end
            i = i + 3
        elseif a == 0xf0 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x90 or b > 0xbf or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
            i = i + 4
        elseif a >= 0xf1 and a <= 0xf3 then
            local b, c, d = value:byte(i + 1, i + 3)
            if not b or not c or not d or b < 0x80 or b > 0xbf or c < 0x80 or c > 0xbf or d < 0x80 or d > 0xbf then return false end
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
    if type(namespace) ~= "string" or not namespace:match("^[a-z0-9][a-z0-9_.%-]*$") or #namespace > 64 then
        return nil, "invalid_namespace"
    end
    if type(resource) ~= "string" or #resource == 0 or #resource > 1024 or resource:find("%z") or not valid_utf8(resource) then
        return nil, "invalid_resource"
    end
    local digest = sha256:new()
    digest:update("dilock.lock.identity\0" .. u32be(#namespace) .. namespace .. u32be(#resource) .. resource)
    return resty_string.to_hex(digest:final())
end

-- Shared by backend adapters so every facade derives the same resource key.
_M.resource_id = identity

local function redis_connection(self)
    local red, err = redis:new()
    if not red then return nil, err end
    red:set_timeouts(self.connect_timeout, self.send_timeout, self.read_timeout)
    local opts = { ssl = self.tls, ssl_verify = self.verify_tls, server_name = self.server_name }
    local ok
    ok, err = red:connect(self.host, self.port, opts)
    if not ok then red:close(); return nil, err end
    if self.password then
        ok, err = red:auth(self.username or "default", self.password)
        if not ok then red:close(); return nil, err end
    end
    return red
end

local function pack(...)
    return { n = select("#", ...), ... }
end

function _M.new(config)
    config = config or {}
    if type(config) ~= "table" then return nil, "invalid_config" end
    local namespace = config.namespace or "default"
    local _, err = identity(namespace, "validation")
    if err then return nil, err end
    if type(config.epoch) ~= "string" or #config.epoch == 0 or #config.epoch > 1024
        or config.epoch:find("%z") or not valid_utf8(config.epoch) then return nil, "invalid_epoch" end
    local host = config.host or "127.0.0.1"
    local port = config.port or 6379
    local connect_timeout = config.connect_timeout or 1000
    local send_timeout = config.send_timeout or 1000
    local read_timeout = config.read_timeout or 1000
    local function valid_timeout(value)
        return type(value) == "number" and value % 1 == 0 and value >= 1 and value <= 120000
    end
    if type(host) ~= "string" or #host == 0 or #host > 255 or host:find("[%z%s]")
        or type(port) ~= "number" or port % 1 ~= 0 or port < 1 or port > 65535
        or not valid_timeout(connect_timeout) or not valid_timeout(send_timeout)
        or not valid_timeout(read_timeout) then
        return nil, "invalid_config"
    end
    if config.tls ~= nil and type(config.tls) ~= "boolean" then return nil, "invalid_config" end
    if config.username ~= nil and (type(config.username) ~= "string" or #config.username == 0)
        or config.password ~= nil and (type(config.password) ~= "string" or #config.password == 0)
        or config.username ~= nil and config.password == nil then
        return nil, "invalid_config"
    end
    if config.server_name ~= nil and (type(config.server_name) ~= "string"
        or #config.server_name == 0 or #config.server_name > 255 or config.server_name:find("[%z%s]")) then
        return nil, "invalid_config"
    end
    local tls = config.tls == true
    local verify_tls = config.verify_tls
    if verify_tls == nil then verify_tls = tls end
    if type(verify_tls) ~= "boolean" then return nil, "invalid_config" end
    return setmetatable({
        namespace = namespace, epoch = config.epoch,
        host = host, port = port,
        password = config.password, username = config.username,
        tls = tls, verify_tls = verify_tls,
        server_name = config.server_name,
        connect_timeout = connect_timeout,
        send_timeout = send_timeout,
        read_timeout = read_timeout,
    }, Provider)
end

function Provider:lock(resource)
    local id, err = identity(self.namespace, resource)
    if not id then return nil, err end
    return setmetatable({ provider = self, key = "dilock:{" .. id .. "}:lease" }, Handle)
end

function Handle:try_acquire(ttl_ms)
    ttl_ms = ttl_ms or 30000
    if type(ttl_ms) ~= "number" or ttl_ms % 1 ~= 0 or ttl_ms < 1 or ttl_ms > 86400000 then return nil, "invalid_ttl" end
    local provider = self.provider
    local red, err = redis_connection(provider)
    if not red then return nil, "backend_unavailable" end
    local metadata_key = "dilock:namespace:" .. provider.namespace .. ":meta"
    -- Metadata is provisioned as one Redis string. Detect a wrong-type key as
    -- a protocol/configuration mismatch instead of disguising it as an outage.
    local type_ok, metadata_type, type_err = pcall(red.type, red, metadata_key)
    if not type_ok or metadata_type == nil or type_err then red:close(); return nil, "backend_unavailable" end
    if metadata_type ~= "string" then red:close(); return nil, "protocol_mismatch" end
    local ok, meta_raw, meta_err = pcall(red.get, red, metadata_key)
    if not ok or meta_err then red:close(); return nil, "backend_unavailable" end
    if not meta_raw or meta_raw == ngx.null then red:close(); return nil, "protocol_mismatch" end
    local meta = cjson.decode(meta_raw)
    if type(meta) ~= "table" or meta.protocol_major ~= 1 or meta.identity_scheme ~= "sha256-framed-utf8-v1"
        or meta.profile ~= "redis-single-authority-v1" or meta.coordination_epoch ~= provider.epoch
        or meta.fixed_lease_ms ~= cjson.null then
        red:close(); return nil, "protocol_mismatch"
    end
    local random = resty_random.bytes(32, true)
    if not random then red:close(); return nil, "backend_unavailable:secure_random" end
    local owner = "dilock/1/" .. resty_string.to_hex(random)
    local sent_at = monotonic_time()
    local sent, result, set_err = pcall(red.set, red, self.key, owner, "NX", "PX", ttl_ms)
    if not sent or result == nil then
        red:close()
        local cleanup, cleanup_err = redis_connection(provider)
        if cleanup then
            pcall(cleanup.eval, cleanup, RELEASE, 1, self.key, owner)
            cleanup:close()
        end
        return nil, "acquisition_unknown"
    end
    if result == ngx.null then
        -- SET NX only identifies that a value occupied the key at the command's
        -- linearization point. Check its type so unrelated keys are not hidden
        -- as ordinary contention.
        local type_ok, key_type, type_err = pcall(red.type, red, self.key)
        red:close()
        if not type_ok or key_type == nil or type_err then return nil, "backend_unavailable" end
        if key_type ~= "string" and key_type ~= "none" then return nil, "protocol_mismatch" end
        return nil, "busy"
    end
    red:close()
    if result ~= "OK" then
        local cleanup = redis_connection(provider)
        if cleanup then pcall(cleanup.eval, cleanup, RELEASE, 1, self.key, owner); cleanup:close() end
        return nil, "acquisition_unknown"
    end
    -- Treat an acknowledgement too close to lease expiry as ambiguous. The
    -- owner-checked cleanup is safe even if the key has already been replaced.
    local safety_margin_ms = math.min(100, math.max(1, math.ceil(ttl_ms * 0.01)))
    if (monotonic_time() - sent_at) * 1000 >= ttl_ms - safety_margin_ms then
        local cleanup = redis_connection(provider)
        if cleanup then pcall(cleanup.eval, cleanup, RELEASE, 1, self.key, owner); cleanup:close() end
        return nil, "acquisition_unknown"
    end
    return setmetatable({ provider = provider, key = self.key, owner = owner }, Lease)
end

function Lease:release()
    if self.release_outcome then return self.release_outcome, self.release_error end
    local red, err = redis_connection(self.provider)
    if not red then
        self.release_outcome, self.release_error = "unknown", err
        return self.release_outcome, self.release_error
    end
    local ok, result, eval_err = pcall(red.eval, red, RELEASE, 1, self.key, self.owner)
    red:close()
    if not ok then
        self.release_outcome = "unknown"
        self.release_error = result
    elseif result ~= 0 and result ~= 1 then
        self.release_outcome = "unknown"
        self.release_error = eval_err or result
    elseif result == 1 then
        self.release_outcome = "released"
    else
        self.release_outcome = "already_not_owned"
    end
    return self.release_outcome, self.release_error
end

--- Runs a callback while this resource is held, then awaits owner-checked cleanup.
--- Returns callback values only after Redis confirms release. On callback failure,
--- returns nil, the traceback, release outcome, release error, and lease; cleanup
--- uncertainty never replaces the callback failure. A successful callback returns
--- nil, "lease_lost" or "release_unknown" when cleanup cannot confirm a safe scope.
--- This synchronous helper is for an OpenResty request context with cosockets.
function Handle:with_lock(fn, ttl_ms)
    if type(fn) ~= "function" then return nil, "callback_required" end
    local lease, err = self:try_acquire(ttl_ms)
    if not lease then return nil, err end

    local callback = pack(xpcall(function()
        return pack(fn(lease))
    end, debug.traceback))

    local released, release_err
    local release_ok, outcome, detail = pcall(lease.release, lease)
    if release_ok then
        released, release_err = outcome, detail
    else
        released, release_err = "unknown", outcome
    end

    if not callback[1] then
        return nil, callback[2], released, release_err, lease
    end
    if released == "unknown" then
        return nil, "release_unknown", release_err, lease
    end
    if released ~= "released" then
        return nil, "lease_lost", released, release_err, lease
    end

    local values = callback[2]
    return unpack(values, 1, values.n)
end

return _M
