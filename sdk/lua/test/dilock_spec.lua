-- Focused contract tests for the OpenResty adapter. Run with the bundled LuaJIT:
-- luajit sdk/lua/test/dilock_spec.lua
local null = {}
local clock = 10
local state = {}
local connections = 0

ngx = { null = null, log = function() end, ERR = 3 }
package.preload["resty.core.time"] = function()
    return { monotonic_time = function() return clock end }
end
package.preload["resty.redis"] = function()
    return { new = function()
        connections = connections + 1
        local conn = {}
        function conn:set_timeouts() return true end
        function conn:connect(_, _, options) state.connect_options = options; return true end
        function conn:auth() return true end
        function conn:get()
            state.metadata_get_calls = (state.metadata_get_calls or 0) + 1
            if state.metadata_get_error then return nil, "injected metadata GET failure" end
            return "{}"
        end
        function conn:set()
            if state.set_delay then clock = clock + state.set_delay end
            if state.set_result ~= nil then return state.set_result end
            return "OK"
        end
        function conn:type(key)
            if key == "dilock:namespace:default:meta" then
                if state.metadata_type_error then return nil, "injected metadata TYPE failure" end
                return state.metadata_type or "string"
            end
            if state.type_error then return nil, "injected TYPE failure" end
            return state.key_type or "string"
        end
        function conn:eval()
            state.eval_calls = (state.eval_calls or 0) + 1
            if state.eval_error then error("injected eval failure") end
            return state.eval_result or 1
        end
        function conn:close() state.closed = (state.closed or 0) + 1; return true end
        return conn
    end }
end
package.preload["resty.sha256"] = function()
    return { new = function()
        return { update = function() end, final = function() return "digest" end }
    end }
end
package.preload["resty.random"] = function()
    return { bytes = function() return string.rep("x", 32) end }
end
package.preload["resty.string"] = function()
    return { to_hex = function(value) return value == "digest" and string.rep("a", 64) or string.rep("b", #value * 2) end }
end
package.preload["cjson.safe"] = function()
    return { null = null, decode = function()
        return { protocol_major = 1, identity_scheme = "sha256-framed-utf8-v1",
            profile = "redis-single-authority-v1", coordination_epoch = "epoch", fixed_lease_ms = null }
    end }
end

local dilock = require "sdk.lua.lib.dilock"
local function eq(actual, expected, message)
    if actual ~= expected then error((message or "assertion") .. ": expected " .. tostring(expected) .. ", got " .. tostring(actual)) end
end
local function provider()
    local result, err = dilock.new({ epoch = "epoch" })
    eq(err, nil, "provider creation")
    return result
end
local function reset()
    clock = 10
    state = {}
end

reset()
local non_table_provider, non_table_err = dilock.new(true)
eq(non_table_provider, nil, "non-table provider configuration must be rejected")
eq(non_table_err, "invalid_config")
for _, config in ipairs({
    { host = 42 }, { host = "" }, { host = "redis host" },
    { port = 0 }, { port = 65536 }, { port = 6379.5 },
    { connect_timeout = 0 }, { connect_timeout = "1000" }, { connect_timeout = 120001 },
    { send_timeout = -1 }, { read_timeout = 1.5 },
    { tls = "true" }, { username = "dilock-user" },
    { password = "" }, { username = 7, password = "secret" },
    { server_name = "" }, { server_name = "redis host" },
}) do
    config.epoch = "epoch"
    local invalid_provider, invalid_err = dilock.new(config)
    eq(invalid_provider, nil, "invalid Redis configuration must be rejected before I/O")
    eq(invalid_err, "invalid_config")
end

reset()
local bad, err = dilock.new({ epoch = "a\0b" })
eq(bad, nil, "NUL epoch rejected")
eq(err, "invalid_epoch")

reset()
local tls_provider = assert(dilock.new({ epoch = "epoch", tls = true }))
lock = assert(tls_provider:lock("verified-tls-default"))
lease, outcome = lock:try_acquire()
eq(outcome, nil, "TLS acquisition uses the fake acknowledged backend")
eq(state.connect_options.ssl, true, "TLS is enabled")
eq(state.connect_options.ssl_verify, true, "TLS certificate verification defaults on")

reset()
tls_provider = assert(dilock.new({ epoch = "epoch", tls = true, verify_tls = false }))
lock = assert(tls_provider:lock("explicit-tls-opt-out"))
lease, outcome = lock:try_acquire()
eq(state.connect_options.ssl_verify, false, "TLS verification can only be disabled explicitly")
bad, err = dilock.new({ epoch = "\255" })
eq(bad, nil, "invalid UTF-8 epoch rejected")
eq(err, "invalid_epoch")

reset()
local lock = assert(provider():lock("resource"))
state.set_result = null
state.key_type = "hash"
local lease, outcome = lock:try_acquire()
eq(lease, nil)
eq(outcome, "protocol_mismatch", "wrong-type key is not contention")

reset()
lock = assert(provider():lock("resource"))
state.metadata_type = "hash"
lease, outcome = lock:try_acquire()
eq(lease, nil)
eq(outcome, "protocol_mismatch", "wrong-type namespace metadata is not a backend outage")
eq(state.metadata_get_calls, nil, "wrong-type metadata is rejected before GET")

reset()
lock = assert(provider():lock("resource"))
state.metadata_type_error = true
lease, outcome = lock:try_acquire()
eq(lease, nil)
eq(outcome, "backend_unavailable", "metadata TYPE errors are not protocol mismatches")
eq(state.metadata_get_calls, nil, "metadata TYPE failure does not issue GET")

reset()
lock = assert(provider():lock("resource"))
state.set_result = null
state.key_type = "string"
lease, outcome = lock:try_acquire()
eq(lease, nil)
eq(outcome, "busy", "string key is ordinary contention")

reset()
lock = assert(provider():lock("resource"))
state.set_result = null
state.type_error = true
lease, outcome = lock:try_acquire()
eq(lease, nil)
eq(outcome, "backend_unavailable", "TYPE backend errors are not contention")

reset()
lock = assert(provider():lock("resource"))
state.set_delay = 29.9
lease, outcome = lock:try_acquire(30000)
eq(lease, nil, "late acknowledgement must not create a held lease")
eq(outcome, "acquisition_unknown")
eq(state.eval_calls, 1, "late owner gets best-effort owner-checked cleanup")

reset()
lock = assert(provider():lock("resource"))
lease = assert(lock:try_acquire())
state.eval_error = true
local created_before_release = connections
local first, first_err = lease:release()
local after_first_release = connections
local second, second_err = lease:release()
eq(first, "unknown", "release failure is unknown")
if not tostring(first_err):find("injected eval failure", 1, true) then error("release error is returned") end
eq(second, first, "release outcome is cached exactly")
eq(second_err, first_err, "release error is cached exactly")
eq(after_first_release, created_before_release + 1, "first release uses one connection")
eq(connections, after_first_release, "cached release does not repeat backend operation")

reset()
lock = assert(provider():lock("scoped-success"))
local scoped_a, scoped_b, scoped_c = lock:with_lock(function(held)
    assert(held and held.owner, "the callback receives the acquired lease")
    return "completed", nil, "tail"
end, 1000)
eq(scoped_a, "completed", "a successful scope returns callback values")
eq(scoped_b, nil, "scope preserves nil callback results")
eq(scoped_c, "tail", "scope preserves callback result arity")

reset()
lock = assert(provider():lock("scoped-lost"))
state.eval_result = 0
local scoped, scope_err, lost_outcome = lock:with_lock(function()
    return "must-not-look-protected"
end, 1000)
eq(scoped, nil, "lost ownership must not report successful scoped work")
eq(scope_err, "lease_lost")
eq(lost_outcome, "already_not_owned")

reset()
lock = assert(provider():lock("scoped-unknown"))
state.eval_error = true
scoped, scope_err, release_err = lock:with_lock(function() return "work" end, 1000)
eq(scoped, nil, "unknown cleanup must not report successful scoped work")
eq(scope_err, "release_unknown")
if not tostring(release_err):find("injected eval failure", 1, true) then
    error("scoped unknown release must preserve its cleanup diagnostic")
end

reset()
lock = assert(provider():lock("scoped-callback-error"))
state.eval_error = true
local callback_error = "callback failure remains primary"
local failed, callback_trace, failed_outcome, failed_release_err = lock:with_lock(function()
    error(callback_error, 0)
end, 1000)
eq(failed, nil)
if not tostring(callback_trace):find(callback_error, 1, true) then
    error("scoped callback error must remain primary when release is unknown")
end
eq(failed_outcome, "unknown", "cleanup uncertainty remains observable beside callback failure")
if not tostring(failed_release_err):find("injected eval failure", 1, true) then
    error("callback failure must retain the cleanup diagnostic")
end

print("PASS: Lua/OpenResty Dilock focused contract tests")
