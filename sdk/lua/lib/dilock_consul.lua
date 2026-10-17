-- OpenResty Consul KV/session adapter using request-scoped cosocket HTTP.
local json = require "cjson.safe"
local http = require "resty.http"
local monotonic_time = require "resty.core.time".monotonic_time
local dilock = require "dilock"

local M, Provider, Handle, Lease = {}, {}, {}, {}
Provider.__index, Handle.__index, Lease.__index = Provider, Handle, Lease
local PROFILE, MAX_MS = "consul-kv-session-v1", 86400000
local function now() return monotonic_time() end
local function stop(category, detail) return nil, category, detail end
local function valid_token(value)
    if value == nil then return true end
    if type(value) ~= "string" or value == "" then return false end
    for i = 1, #value do local byte = value:byte(i); if byte < 32 or byte > 126 then return false end end
    return true
end
local function decimal_index(text)
    local fields = {}
    for value in text:gmatch('"LockIndex"%s*:%s*(%d+)%s*[,}]') do fields[#fields + 1] = value end
    if #fields ~= 1 or (fields[1] ~= "0" and not fields[1]:match("^[1-9][0-9]*$")) then return nil end
    return fields[1]
end
M._decimal_index = decimal_index

local function request(provider, method, path, body, mutation, timeout_ms)
    local headers = { Accept = "application/json", ["Content-Length"] = tostring(#(body or "")) }
    if body ~= nil then headers["Content-Type"] = "application/json" end
    if provider.token then headers["X-Consul-Token"] = provider.token end
    if not ngx or not ngx.get_phase or not ngx.socket then return nil, "openresty_request_context_required" end
    local phase = ngx.get_phase()
    if phase ~= "rewrite" and phase ~= "access" and phase ~= "content" and phase ~= "timer" then
        return nil, "openresty_cosocket_phase_unsupported"
    end
    local made, client = pcall(http.new)
    if not made or not client then return nil, mutation and "acquisition_unknown" or "backend_unavailable", client end
    local timeout = timeout_ms or provider.rpc_timeout_ms
    local timed, timeout_ok, timeout_err = pcall(client.set_timeouts, client, timeout, timeout, timeout)
    if not timed or timeout_ok == false or (timeout_ok == nil and timeout_err ~= nil) then
        return nil, mutation and "acquisition_unknown" or "backend_unavailable", timed and timeout_err or timeout_ok
    end
    local ok, response, err = pcall(client.request_uri, client, provider.endpoint .. path, {
        method = method, headers = headers, body = body,
        ssl_verify = provider.verify_tls,
        ssl_server_name = provider.server_name,
        keepalive = false,
    })
    if not ok or not response then
        return nil, mutation and "acquisition_unknown" or "backend_unavailable", ok and err or response
    end
    local code = response.status
    local payload = response.body or ""
    if code == 404 then return nil, "not_found", code end
    if code < 200 or code >= 300 then return nil, mutation and code >= 500 and "acquisition_unknown" or "backend_unavailable", code end
    return payload, nil, code, response.headers
end

local function decode(payload)
    if type(payload) ~= "string" then return nil end
    return json.decode(payload)
end
local function b64decode(value)
    if type(value) ~= "string" or #value == 0 or #value % 4 ~= 0 then return nil end
    local alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    local reverse = {}
    for i = 1, #alphabet do reverse[alphabet:sub(i, i)] = i - 1 end
    local out = {}
    for i = 1, #value, 4 do
        local a, b = reverse[value:sub(i, i)], reverse[value:sub(i + 1, i + 1)]
        local cchar, dchar = value:sub(i + 2, i + 2), value:sub(i + 3, i + 3)
        local c, d = cchar == "=" and 0 or reverse[cchar], dchar == "=" and 0 or reverse[dchar]
        if not a or not b or not c or not d or (cchar == "=" and dchar ~= "=") then return nil end
        out[#out + 1] = string.char(a * 4 + math.floor(b / 16))
        if cchar ~= "=" then out[#out + 1] = string.char((b % 16) * 16 + math.floor(c / 4)) end
        if dchar ~= "=" then out[#out + 1] = string.char((c % 4) * 64 + d) end
    end
    return table.concat(out)
end
local function metadata(provider)
    local key = "dilock/namespaces/" .. provider.namespace .. "/meta"
    local payload, err = request(provider, "GET", "/v1/kv/" .. key .. "?consistent=true")
    if not payload then return nil, err == "not_found" and "protocol_mismatch" or err end
    local rows = decode(payload)
    if type(rows) ~= "table" or #rows ~= 1 or rows[1].Key ~= key then return nil, "protocol_mismatch" end
    local raw, meta = b64decode(rows[1].Value), nil
    if not raw or #raw > 8192 or raw:find("%z") then return nil, "protocol_mismatch" end
    meta = decode(raw)
    if type(meta) ~= "table" or meta.protocol_major ~= 1 or meta.identity_scheme ~= "sha256-framed-utf8-v1"
        or meta.profile ~= PROFILE or meta.coordination_epoch ~= provider.epoch or meta.fixed_lease_ms ~= json.null then
        return nil, "protocol_mismatch"
    end
    for _, name in ipairs({ "protocol_major", "identity_scheme", "profile", "coordination_epoch", "fixed_lease_ms" }) do
        local _, count = raw:gsub('"' .. name .. '"%s*:', "")
        if count ~= 1 then return nil, "protocol_mismatch" end
    end
    local field_count = 0
    for _ in pairs(meta) do field_count = field_count + 1 end
    if field_count ~= 5 then return nil, "protocol_mismatch" end
    return true
end

function M.provision(config)
    if type(config) ~= "table" then return stop("invalid_config") end
    local namespace, epoch = config.namespace or "default", config.epoch
    local _, err = dilock.resource_id(namespace, "validation")
    if err then return stop(err) end
    local _, epoch_error = dilock.resource_id(namespace, epoch)
    if epoch_error then return stop("invalid_epoch") end
    local endpoint = config.endpoint or "http://127.0.0.1:8500"
    if type(endpoint) ~= "string" or not endpoint:match("^https?://[^/]+/?$") or endpoint:find("[%s?#@]") then return stop("invalid_endpoint") end
    if not valid_token(config.token) then return stop("invalid_token") end
    if config.verify_tls ~= nil and type(config.verify_tls) ~= "boolean" then return stop("invalid_verify_tls") end
    if config.server_name ~= nil and (type(config.server_name) ~= "string" or config.server_name == "" or config.server_name:find("[%s%c]")) then return stop("invalid_server_name") end
    local rpc_timeout_ms, cleanup_timeout_ms = config.rpc_timeout_ms or 2000, config.cleanup_timeout_ms or 2000
    if type(rpc_timeout_ms) ~= "number" or rpc_timeout_ms % 1 ~= 0 or rpc_timeout_ms < 1 or rpc_timeout_ms > MAX_MS
        or type(cleanup_timeout_ms) ~= "number" or cleanup_timeout_ms % 1 ~= 0 or cleanup_timeout_ms < 1 or cleanup_timeout_ms > MAX_MS then
        return stop("invalid_config")
    end
    endpoint = endpoint:gsub("/$", "")
    local provider = { endpoint = endpoint, namespace = namespace, epoch = epoch, token = config.token,
        verify_tls = config.verify_tls ~= false, server_name = config.server_name,
        rpc_timeout_ms = rpc_timeout_ms, cleanup_timeout_ms = cleanup_timeout_ms }
    local value = json.encode({ protocol_major = 1, identity_scheme = "sha256-framed-utf8-v1", profile = PROFILE,
        coordination_epoch = epoch, fixed_lease_ms = json.null })
    -- Base64 encode the metadata bytes without relying on an optional module.
    local alphabet, encoded = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", {}
    for i = 1, #value, 3 do
        local a, b, c = value:byte(i, i + 2); b, c = b or 0, c or 0
        local n = a * 65536 + b * 256 + c
        encoded[#encoded + 1] = alphabet:sub(math.floor(n / 262144) % 64 + 1, math.floor(n / 262144) % 64 + 1)
        encoded[#encoded + 1] = alphabet:sub(math.floor(n / 4096) % 64 + 1, math.floor(n / 4096) % 64 + 1)
        encoded[#encoded + 1] = i + 1 <= #value and alphabet:sub(math.floor(n / 64) % 64 + 1, math.floor(n / 64) % 64 + 1) or "="
        encoded[#encoded + 1] = i + 2 <= #value and alphabet:sub(n % 64 + 1, n % 64 + 1) or "="
    end
    body = json.encode(table.concat(encoded))
    local path = "/v1/kv/dilock/namespaces/" .. namespace .. "/meta?cas=0"
    local response, status = request(provider, "PUT", path, body, true)
    if not response then return stop(status) end
    local created = decode(response)
    if created == true then return true end
    local ok, category = metadata(provider)
    if not ok then return stop(category or "protocol_mismatch") end
    return true
end

function M.new(config)
    if type(config) ~= "table" then return stop("invalid_config") end
    local namespace, epoch = config.namespace or "default", config.epoch
    local _, err = dilock.resource_id(namespace, "validation")
    if err then return stop(err) end
    local _, epoch_error = dilock.resource_id(namespace, epoch)
    if epoch_error then return stop("invalid_epoch") end
    local endpoint = config.endpoint or "http://127.0.0.1:8500"
    if type(endpoint) ~= "string" or not endpoint:match("^https?://[^/]+/?$") or endpoint:find("[%s?#@]") then return stop("invalid_endpoint") end
    if not valid_token(config.token) then return stop("invalid_token") end
    local timeout, lease, wait = config.rpc_timeout_ms or 2000, config.default_lease_ms or 30000, config.default_wait_ms or 5000
    local poll, cleanup = config.poll_interval_ms or 100, config.cleanup_timeout_ms or 2000
    for _, value in ipairs({ timeout, lease, wait, poll, cleanup }) do
        if type(value) ~= "number" or value % 1 ~= 0 or value < 1 or value > MAX_MS then return stop("invalid_config") end
    end
    if lease < 10000 or lease <= timeout + math.max(100, math.ceil(lease * 0.01)) then return stop("invalid_default_lease_ms") end
    if config.verify_tls ~= nil and type(config.verify_tls) ~= "boolean" then return stop("invalid_verify_tls") end
    if config.server_name ~= nil and (type(config.server_name) ~= "string" or config.server_name == "" or config.server_name:find("[%s%c]")) then return stop("invalid_server_name") end
    local provider = setmetatable({ endpoint = endpoint:gsub("/$", ""), namespace = namespace, epoch = epoch,
        token = config.token, rpc_timeout_ms = timeout, cleanup_timeout_ms = cleanup, poll_interval_ms = poll,
        verify_tls = config.verify_tls ~= false, server_name = config.server_name,
        default_wait_ms = wait, default_lease_ms = lease, leases = {}, closed = false }, Provider)
    local ok, category = metadata(provider)
    if not ok then return stop(category) end
    return provider
end
function Provider:lock(resource)
    if self.closed then return stop("provider_closed") end
    local id, err = dilock.resource_id(self.namespace, resource)
    if not id then return stop(err) end
    return setmetatable({ provider = self, key = "dilock/locks/" .. id }, Handle)
end
local function session_create(provider, ttl)
    -- Omit Checks; Consul defaults the optional check list to empty. Empty Lua
    -- tables otherwise serialize as JSON objects with cjson-safe.
    local body = json.encode({ Name = "dilock lease", Behavior = "release", LockDelay = "0s", TTL = tostring(math.ceil(ttl / 1000)) .. "s" })
    local sent = now()
    local raw, status = request(provider, "PUT", "/v1/session/create", body, true)
    if not raw then return nil, status end
    local value = decode(raw)
    if type(value) ~= "table" or type(value.ID) ~= "string" or value.ID == "" then return nil, "acquisition_unknown" end
    return { id = value.ID, ttl_ms = math.ceil(ttl / 1000) * 1000,
        deadline = sent + math.ceil(ttl / 1000) - math.max(0.1, math.ceil(ttl / 1000) * 0.01) }
end
local function destroy(provider, session, timeout)
    local raw = request(provider, "PUT", "/v1/session/destroy/" .. session.id, "", true, timeout or provider.cleanup_timeout_ms)
    return raw and decode(raw) == true
end
local function owner(provider, key, timeout)
    local raw, status = request(provider, "GET", "/v1/kv/" .. key .. "?consistent=true", nil, false, timeout)
    if status == "not_found" then return nil end
    if not raw then return nil, status end
    local rows = decode(raw)
    if type(rows) ~= "table" or #rows ~= 1 or rows[1].Key ~= key then return nil, "protocol_mismatch" end
    local index = decimal_index(raw)
    if not index then return nil, "protocol_mismatch" end
    return { session = rows[1].Session or "", index = index }
end
function Handle:try_acquire(ttl)
    local provider = self.provider
    ttl = ttl or provider.default_lease_ms
    if provider.closed then return stop("provider_closed") end
    if type(ttl) ~= "number" or ttl % 1 ~= 0 or ttl < 10000 or ttl > MAX_MS
        or ttl <= provider.rpc_timeout_ms + math.max(100, math.ceil(ttl * 0.01)) then return stop("invalid_lease_ms") end
    local session, status = session_create(provider, ttl)
    if not session then return stop(status) end
    if now() >= session.deadline then destroy(provider, session); return stop("acquisition_unknown") end
    local raw, request_status = request(provider, "PUT", "/v1/kv/" .. self.key .. "?acquire=" .. session.id, "", true)
    if not raw then destroy(provider, session); return stop(request_status) end
    local acquired = decode(raw)
    if acquired == false then destroy(provider, session); return stop("busy") end
    if acquired ~= true then destroy(provider, session); return stop("acquisition_unknown") end
    local observed, owner_status = owner(provider, self.key)
    if not observed or observed.session ~= session.id or observed.index == "0" or now() >= session.deadline then
        destroy(provider, session)
        return stop(owner_status or "acquisition_unknown")
    end
    local lease = setmetatable({ provider = provider, key = self.key, session = session, fence = {
        coordination_epoch = provider.epoch, lock_index = observed.index }, effective_ttl_ms = math.floor((session.deadline - now()) * 1000),
        outcome = nil, state = "held" }, Lease)
    provider.leases[#provider.leases + 1] = lease
    return lease
end
function Handle:acquire(options)
    options = options or {}
    if type(options) ~= "table" then return stop("invalid_options") end
    local wait, ttl = options.wait_ms or self.provider.default_wait_ms, options.lease_ms or self.provider.default_lease_ms
    if type(wait) ~= "number" or wait % 1 ~= 0 or wait < 1 or wait > MAX_MS then return stop("invalid_wait_ms") end
    local deadline = now() + wait / 1000
    repeat
        local lease, status, detail = self:try_acquire(ttl)
        if lease then
            if now() < deadline then return lease end
            local outcome = lease:release()
            return stop(outcome == "unknown" and "acquisition_unknown" or "wait_timeout", detail)
        end
        if status ~= "busy" then return stop(status, detail) end
        if now() < deadline then ngx.sleep(math.min(self.provider.poll_interval_ms / 1000, deadline - now())) end
    until now() >= deadline
    return stop("wait_timeout")
end
function Lease:is_valid() return self.state == "held" and now() < self.session.deadline end
function Lease:renew()
    if not self:is_valid() then self.state = "lost"; return false, "lease_lost" end
    local old_deadline = self.session.deadline
    local sent = now()
    local raw, status = request(self.provider, "PUT", "/v1/session/renew/" .. self.session.id, "", true)
    if not raw then self.state = "lost"; return false, status end
    local rows = decode(raw)
    local deadline = sent + self.session.ttl_ms / 1000 - math.max(0.1, self.session.ttl_ms / 100000)
    if type(rows) ~= "table" or #rows ~= 1 or rows[1].ID ~= self.session.id or now() >= deadline then self.state = "lost"; return false, "lease_lost" end
    local observed, owner_status = owner(self.provider, self.key)
    if not observed or observed.session ~= self.session.id or observed.index ~= self.fence.lock_index
        or now() >= old_deadline or now() >= deadline then
        self.state = "lost"
        return false, owner_status or "lease_lost"
    end
    self.session.deadline = deadline
    self.effective_ttl_ms = math.floor((deadline - now()) * 1000)
    return true
end
function Lease:release()
    if self.outcome then return self.outcome end
    local deadline = now() + self.provider.cleanup_timeout_ms / 1000
    local function remain() local n = math.floor((deadline - now()) * 1000); if n < 1 then error("cleanup timeout") end; return n end
    local outcome = "unknown"
    local ok, result = pcall(function()
        local current, owner_status = owner(self.provider, self.key, remain())
        if not current and owner_status then error(owner_status) end
        if not current or current.session ~= self.session.id or current.index ~= self.fence.lock_index then
            return "already_not_owned"
        end
        local raw = request(self.provider, "PUT", "/v1/kv/" .. self.key .. "?release=" .. self.session.id, "", true, remain())
        local released = raw and decode(raw) == true
        local ownership_gone = released
        if not released then
            local after, after_status = owner(self.provider, self.key, remain())
            if not after and after_status then error(after_status) end
            ownership_gone = not after or after.session ~= self.session.id or after.index ~= self.fence.lock_index
        end
        return released and "released" or (ownership_gone and "already_not_owned" or "unknown")
    end)
    if ok then outcome = result end
    local cleanup_ok, destroyed = pcall(destroy, self.provider, self.session, self.provider.cleanup_timeout_ms)
    if not cleanup_ok or destroyed ~= true then outcome = "unknown" end
    self.outcome, self.state = outcome, "released"
    return outcome
end
function Handle:with_lock(fn, options)
    if type(fn) ~= "function" then return stop("invalid_callback") end
    local lease, status, detail = self:acquire(options)
    if not lease then return nil, status, detail end
    local ok, value = xpcall(function() return fn(lease) end, function(e) return e end)
    local outcome = lease:release()
    if outcome ~= "released" then
        if not ok then return nil, value, outcome, "Consul release was not acknowledged" end
        return stop(outcome, "Consul release was not acknowledged")
    end
    if not ok then error(value, 0) end
    return value
end
function Provider:close()
    if self.closed then return true end
    local uncertain = false
    for _, lease in ipairs(self.leases) do if lease:release() == "unknown" then uncertain = true end end
    self.closed = true
    if uncertain then return stop("release_unknown") end
    return true
end
return M
