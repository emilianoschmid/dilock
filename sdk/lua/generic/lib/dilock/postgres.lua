-- Experimental synchronous LuaSQL PostgreSQL advisory-session adapter.
local json = require "dkjson"
local socket = require "socket"
local system = require "system"
local identity = require "dilock.identity"
local luasql = require "luasql.postgres"

local M, Provider, Handle, Lease = {}, {}, {}, {}
Provider.__index, Handle.__index, Lease.__index = Provider, Handle, Lease
local PROFILE, SCHEME, MAX_MS = "postgres-advisory-session-v1", "sha256-framed-utf8-v1", 86400000
local function stop(category, detail) return nil, category, detail end
local function close(value)
    local kind = type(value)
    if value and (kind == "table" or kind == "userdata") and value.close then
        pcall(value.close, value)
    end
end
local function valid_ms(value) return type(value) == "number" and value % 1 == 0 and value > 0 and value <= MAX_MS end
local function quote(value) return "'" .. value:gsub("'", "''") .. "'" end

-- Lua 5.1 numbers cannot represent all int64 keys exactly; keep the key decimal.
local function decimal_compare(a, b)
    a, b = a:gsub("^0+", ""), b:gsub("^0+", "")
    if a == "" then a = "0" end; if b == "" then b = "0" end
    return #a == #b and (a == b and 0 or (a < b and -1 or 1)) or (#a < #b and -1 or 1)
end
local function decimal_sub(a, b)
    local out, borrow = {}, 0
    for i = math.max(#a, #b), 1, -1 do
        local da, db = tonumber(a:sub(i, i)) or 0, tonumber(b:sub(i, i)) or 0
        local value = da - borrow - db
        if value < 0 then value, borrow = value + 10, 1 else borrow = 0 end
        out[#out + 1] = tostring(value)
    end
    local value = table.concat(out):reverse():gsub("^0+", "")
    return value == "" and "0" or value
end
local function hex_to_decimal(hex)
    local digits = { 0 }
    for i = 1, #hex do
        local nibble, carry = tonumber(hex:sub(i, i), 16), tonumber(hex:sub(i, i), 16)
        for j = #digits, 1, -1 do
            local value = digits[j] * 16 + carry
            digits[j], carry = value % 10, math.floor(value / 10)
        end
        while carry > 0 do table.insert(digits, 1, carry % 10); carry = math.floor(carry / 10) end
    end
    local text = table.concat(digits):gsub("^0+", "")
    return text == "" and "0" or text
end
local function advisory_key(resource_id)
    local unsigned = hex_to_decimal(resource_id:sub(1, 16))
    if decimal_compare(unsigned, "9223372036854775808") >= 0 then
        return "-" .. decimal_sub("18446744073709551616", unsigned)
    end
    return unsigned
end
M.advisory_key = advisory_key

local function connect(config)
    local factory = luasql.postgres or luasql
    local ok, env = pcall(factory)
    if not ok then return nil, "backend_unavailable", env end
    local connected, connection = pcall(env.connect, env, config.database, config.user, config.password, config.host, config.port)
    if not connected or not connection then close(env); return nil, "backend_unavailable", connection end
    return env, connection
end
local function scalar(connection, sql)
    local ok, cursor = pcall(connection.execute, connection, sql)
    if not ok or not cursor then return nil, "backend_unavailable", cursor end
    local fetched, row = pcall(cursor.fetch, cursor, {}, "a")
    close(cursor)
    if not fetched or type(row) ~= "table" then return nil, "protocol_mismatch" end
    return row[1] or row["pg_try_advisory_lock"] or row["pg_advisory_unlock"] or row["?column?"]
end
local function bool_value(value) return value == true or value == "t" or value == "true" or value == "1" end
local function metadata(provider, connection)
    local ok, cursor = pcall(connection.execute, connection,
        "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms "
        .. "FROM dilock_namespace_meta WHERE namespace = " .. quote(provider.namespace))
    if not ok or not cursor then return stop("backend_unavailable", cursor) end
    local fetched, row = pcall(cursor.fetch, cursor, {}, "a"); close(cursor)
    if not fetched or type(row) ~= "table" then return stop("protocol_mismatch") end
    local major, scheme, profile, epoch, fixed = row.protocol_major or row[1], row.identity_scheme or row[2],
        row.profile or row[3], row.coordination_epoch or row[4], row.fixed_lease_ms or row[5]
    if tostring(major) ~= "1" or scheme ~= SCHEME or profile ~= PROFILE or epoch ~= provider.epoch
        or (fixed ~= nil and fixed ~= "") then return stop("protocol_mismatch") end
    return true
end
local function open_session(provider)
    local env, connection, detail = connect(provider.config)
    if not env then return nil, connection, detail end
    local ok, result = pcall(connection.execute, connection, "SET statement_timeout = " .. tostring(provider.rpc_timeout_ms))
    close(result)
    if not ok then close(connection); close(env); return nil, "backend_unavailable" end
    return { env = env, connection = connection }
end

function M.provision(config)
    if type(config) ~= "table" or type(config.database) ~= "string" then return stop("invalid_config") end
    local namespace, epoch = config.namespace or "default", config.epoch
    local _, err = identity.resource_id(namespace, "validation")
    if err then return stop(err) end
    if type(epoch) ~= "string" or epoch == "" or epoch:find("%z") then return stop("invalid_epoch") end
    local env, connection, detail = connect(config)
    if not env then return stop(connection, detail) end
    local ok, result = pcall(function()
        local cursor = connection:execute("BEGIN"); close(cursor)
        cursor = connection:execute("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))"); close(cursor)
        cursor = connection:execute("CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)"); close(cursor)
        cursor = connection:execute("INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (" .. quote(namespace) .. ", 1, " .. quote(SCHEME) .. ", " .. quote(PROFILE) .. ", " .. quote(epoch) .. ", NULL) ON CONFLICT (namespace) DO NOTHING"); close(cursor)
        cursor = connection:execute("COMMIT"); close(cursor)
    end)
    if not ok then
        pcall(connection.execute, connection, "ROLLBACK")
        close(result); close(connection); close(env); return stop("backend_unavailable")
    end
    local provider = setmetatable({ config = config, namespace = namespace, epoch = epoch, rpc_timeout_ms = config.rpc_timeout_ms or 2000 }, Provider)
    local valid, category, validation_detail = metadata(provider, connection)
    close(connection); close(env)
    if not valid then return stop(category, validation_detail) end
    return true
end

function M.new(config)
    if type(config) ~= "table" or type(config.database) ~= "string" or config.database == "" then return stop("invalid_config") end
    local namespace, epoch = config.namespace or "default", config.epoch
    local _, err = identity.resource_id(namespace, "validation")
    if err then return stop(err) end
    if type(epoch) ~= "string" or epoch == "" or epoch:find("%z") then return stop("invalid_epoch") end
    local provider = setmetatable({ config = config, namespace = namespace, epoch = epoch,
        rpc_timeout_ms = config.rpc_timeout_ms or 2000, cleanup_timeout_ms = config.cleanup_timeout_ms or 2000,
        poll_interval_ms = config.poll_interval_ms or 25, default_wait_ms = config.default_wait_ms or 5000,
        default_lease_ms = config.default_lease_ms or 30000, leases = {}, closed = false }, Provider)
    for _, value in ipairs({ provider.rpc_timeout_ms, provider.cleanup_timeout_ms, provider.poll_interval_ms, provider.default_wait_ms, provider.default_lease_ms }) do
        if not valid_ms(value) then return stop("invalid_config") end
    end
    local session, category, detail = open_session(provider)
    if not session then return stop(category, detail) end
    local valid, metadata_category, metadata_detail = metadata(provider, session.connection)
    close(session.connection); close(session.env)
    if not valid then return stop(metadata_category, metadata_detail) end
    return provider
end

function Provider:lock(resource)
    if self.closed then return stop("provider_closed") end
    local id, err = identity.resource_id(self.namespace, resource)
    if not id then return stop(err) end
    return setmetatable({ provider = self, resource = resource, key = advisory_key(id) }, Handle)
end
function Handle:try_acquire(lease_ms)
    local provider = self.provider; lease_ms = lease_ms or provider.default_lease_ms
    if provider.closed then return stop("provider_closed") end
    if not valid_ms(lease_ms) then return stop("invalid_lease_ms") end
    local session, category, detail = open_session(provider)
    if not session then return stop(category, detail) end
    local value, query_category, query_detail = scalar(session.connection, "SELECT pg_try_advisory_lock(" .. self.key .. "::bigint)")
    if value == nil then close(session.connection); close(session.env); return stop(query_category or "acquisition_unknown", query_detail) end
    if not bool_value(value) then close(session.connection); close(session.env); return stop("busy") end
    local lease = setmetatable({ provider = provider, handle = self, session = session, state = "held", outcome = nil }, Lease)
    provider.leases[#provider.leases + 1] = lease; return lease
end
function Handle:acquire(options)
    options = options or { wait_ms = self.provider.default_wait_ms, lease_ms = self.provider.default_lease_ms }
    if type(options) ~= "table" then return stop("invalid_options") end
    local wait_ms = options.wait_ms or self.provider.default_wait_ms
    if not valid_ms(wait_ms) then return stop("invalid_wait_ms") end
    local deadline = system.monotime() + wait_ms / 1000
    repeat
        local lease, category, detail = self:try_acquire(options.lease_ms)
        if lease then return lease end
        if category ~= "busy" then return stop(category, detail) end
        if system.monotime() < deadline then socket.sleep(math.min(self.provider.poll_interval_ms / 1000, deadline - system.monotime())) end
    until system.monotime() >= deadline
    return stop("wait_timeout")
end
function Lease:is_valid() return self.state == "held" end
function Lease:renew()
    if not self:is_valid() then self.state = "lost"; return false, "lease_lost" end
    local value, category, detail = scalar(self.session.connection, "SELECT 1")
    if not value then self.state = "lost"; return false, category, detail end
    return true
end
function Lease:release()
    if self.outcome then return self.outcome end
    if self.state ~= "held" then self.outcome = "unknown"; return self.outcome end
    local value, category = scalar(self.session.connection, "SELECT pg_advisory_unlock(" .. self.handle.key .. "::bigint)")
    if value == nil then self.outcome = "unknown" else self.outcome = bool_value(value) and "released" or "already_not_owned" end
    close(self.session.connection); close(self.session.env); self.state = "released"; return self.outcome, category
end
function Provider:close()
    if self.closed then return true end
    for _, lease in ipairs(self.leases) do lease:release() end
    self.closed = true; return true
end

return M
