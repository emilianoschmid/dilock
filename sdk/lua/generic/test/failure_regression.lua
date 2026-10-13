local fake = { key = nil, value = nil, cleanup_calls = 0, set_calls = 0, mode = "lost_set" }

package.preload.redis = function()
    local module = {}
    function module.connect()
        local client = {}
        function client.type(_, key)
            if key == "dilock:namespace:failure-regression:meta" then return "string" end
            return fake.value and "string" or "none"
        end
        function client.get(_, key)
            assert(key == "dilock:namespace:failure-regression:meta")
            return '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",'
                .. '"profile":"redis-single-authority-v1","coordination_epoch":"test-epoch",'
                .. '"fixed_lease_ms":null}'
        end
        function client.set(_, key, value)
            fake.set_calls = fake.set_calls + 1
            fake.key, fake.value = key, value
            if fake.mode == "lost_set" then error("simulated lost SET response") end
            return true
        end
        function client.eval(_, script, key_count, key, owner)
            assert(key_count == 1)
            assert(script:find("redis.call('GET'", 1, true) and script:find("redis.call('DEL'", 1, true))
            fake.cleanup_calls = fake.cleanup_calls + 1
            if fake.mode == "release_unknown" then error("simulated lost release response") end
            if fake.key == key and fake.value == owner then
                fake.value = nil
                return 1
            end
            return 0
        end
        client.network = { socket = { close = function() end } }
        return client
    end
    return module
end

local dilock = require "dilock"
local provider = assert(dilock.new({ namespace = "failure-regression", epoch = "test-epoch" }))
local handle = assert(provider:lock("response-lost"))
local lease, outcome = handle:try_acquire(5000)
assert(lease == nil and outcome == "acquisition_unknown", "lost SET response must remain unknown")
assert(fake.set_calls == 1, "acquisition must not retry SET")
assert(fake.cleanup_calls == 1, "uncertain owner must receive one bounded cleanup attempt")
assert(fake.value == nil, "cleanup must remove only the matching uncertain owner")

-- A callback failure and an unknown release must both remain observable.
fake.mode = "release_unknown"
local callback_error = { marker = "original callback error" }
local observed
local observed_provider = assert(dilock.new({ namespace = "failure-regression", epoch = "test-epoch",
    cleanup_observer = function(event) observed = event end }))
local observed_scope = assert(observed_provider:lock("body-and-cleanup-error"))
local ok, thrown = pcall(function()
    observed_scope:with_lock(function() error(callback_error, 0) end, 5000)
end)
assert(not ok and type(thrown) == "table", "cleanup failure should return a structured callback error")
assert(thrown.kind == "dilock_with_lock_error" and thrown.cause == callback_error,
    "structured error must preserve the exact callback error as its cause")
assert(thrown.cleanup_outcome == "unknown" and thrown.cleanup_error == "backend_unavailable",
    "structured error must expose uncertain cleanup")
assert(observed and observed.outcome == "unknown" and observed.error == "backend_unavailable"
    and observed.callback_error == callback_error, "observer must receive cleanup and callback errors")

-- An observer failure is captured and cannot replace the callback's cause.
local throwing_provider = assert(dilock.new({ namespace = "failure-regression", epoch = "test-epoch",
    cleanup_observer = function() error("observer failure") end }))
local throwing_scope = assert(throwing_provider:lock("observer-error"))
local second_callback_error = { marker = "second callback error" }
local second_ok, second_thrown = pcall(function()
    throwing_scope:with_lock(function() error(second_callback_error, 0) end, 5000)
end)
assert(not second_ok and second_thrown.cause == second_callback_error,
    "observer failure must not replace callback error")
assert(second_thrown.cleanup_outcome == "unknown"
    and tostring(second_thrown.observer_error):find("observer failure", 1, true),
    "observer failure and cleanup outcome must remain inspectable")

print("Lua generic uncertain acquisition and cleanup regression: PASS")
