-- Exercise Lua Redis clients that report failures as `nil, err` instead of
-- throwing. Run this focused test in an isolated LuaJIT image without network.
local module_root = os.getenv("DILOCK_LUA_MODULE_ROOT") or "/src/lib"
package.path = module_root .. "/?.lua;" .. package.path
local fake = { mode = "ok", key = nil, value = nil }

package.preload.redis = function()
    return {
        connect = function()
            if fake.mode == "connect_error" then return nil, "connection refused" end
            local client = { network = { socket = { close = function() end } } }
            function client.type(_, key)
                if fake.mode == "metadata_type_error" and key:match(":meta$") then
                    return nil, "read timeout"
                end
                if key:match(":meta$") then return "string" end
                return fake.value and "string" or "none"
            end
            function client.get(_, key)
                if fake.mode == "metadata_get_error" then return nil, "read timeout" end
                assert(key == "dilock:namespace:errors:meta")
                return "metadata"
            end
            function client.set(_, key, value)
                if fake.mode == "set_error_after_apply" then
                    fake.key, fake.value = key, value
                    return nil, "read timeout"
                end
                fake.key, fake.value = key, value
                return true
            end
            function client.eval(_, _, _, key, owner)
                if fake.mode == "release_error" then return nil, "read timeout" end
                if fake.key == key and fake.value == owner then
                    fake.value = nil
                    return 1
                end
                return 0
            end
            return client
        end,
    }
end
package.preload.dkjson = function()
    local null = {}
    return {
        null = null,
        decode = function() return {
            protocol_major = 1,
            identity_scheme = "sha256-framed-utf8-v1",
            profile = "redis-single-authority-v1",
            coordination_epoch = "test-epoch",
            fixed_lease_ms = null,
        } end,
    }
end
package.preload["openssl.digest"] = function()
    return {
        new = function()
            return { update = function() end, final = function() return string.rep("x", 32) end }
        end,
    }
end
package.preload["openssl.rand"] = function()
    return { bytes = function(count) return string.rep("r", count) end }
end
package.preload.socket = function() return {} end
package.preload.system = function() return { monotime = function() return 1 end } end

local dilock = require "dilock"
local function provider() return assert(dilock.new({ namespace = "errors", epoch = "test-epoch" })) end
local function check(condition, message) if not condition then error(message, 2) end end

fake.mode = "connect_error"
local lease, outcome = provider():lock("connect"):try_acquire(5000)
check(lease == nil and outcome == "backend_unavailable", "nil,error connect was not a backend failure")

fake.mode = "metadata_type_error"
lease, outcome = provider():lock("type"):try_acquire(5000)
check(lease == nil and outcome == "backend_unavailable", "nil,error TYPE was misclassified")

fake.mode = "metadata_get_error"
lease, outcome = provider():lock("get"):try_acquire(5000)
check(lease == nil and outcome == "backend_unavailable", "nil,error GET was misclassified")

fake.mode = "set_error_after_apply"
lease, outcome = provider():lock("set"):try_acquire(5000)
check(lease == nil and outcome == "acquisition_unknown", "nil,error SET was not kept uncertain")
check(fake.value == nil, "owner-checked cleanup did not clear the uncertain acquisition")

fake.mode = "ok"
lease = assert(provider():lock("release"):try_acquire(5000))
fake.mode = "release_error"
local release_err
outcome, release_err = lease:release()
check(outcome == "unknown" and release_err == "backend_unavailable",
    "nil,error EVAL was not reported as uncertain cleanup")

print("Lua generic nil,error Redis-client regression: PASS")
