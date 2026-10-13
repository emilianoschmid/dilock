-- This handler is intentionally executed by ngx_lua so resty.redis cosockets
-- and the OpenResty monotonic clock are exercised in their supported context.
local redis = require "resty.redis"
local dilock = require "dilock"

local namespace = "docker-test"
local epoch = "openresty-redis-live-v1"
local resource = "integration:handoff"
local expiry_resource = "integration:expiry"
local wrong_type_resource = "integration:wrong-type"
local wrong_metadata_namespace = "docker-test-wrong-meta"
local redis_host = os.getenv("DILOCK_REDIS_HOST") or "redis"

local admin = assert(redis:new())
admin:set_timeouts(1000, 1000, 1000)
assert(admin:connect(redis_host, 6379))
assert(admin:set("dilock:namespace:" .. namespace .. ":meta",
    [[{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"openresty-redis-live-v1","fixed_lease_ms":null}]]))
admin:close()

local first_provider = assert(dilock.new({ host = redis_host, port = 6379, namespace = namespace, epoch = epoch }))
local second_provider = assert(dilock.new({ host = redis_host, port = 6379, namespace = namespace, epoch = epoch }))
local first_lock = assert(first_provider:lock(resource))
local second_lock = assert(second_provider:lock(resource))
local first_lease, outcome = first_lock:try_acquire(5000)
assert(first_lease, "first owner failed to acquire: " .. tostring(outcome))

local contended, contention = second_lock:try_acquire(5000)
assert(contended == nil and contention == "busy", "second owner must observe busy, got " .. tostring(contention))

local released, release_error = first_lease:release()
assert(released == "released", "first owner release failed: " .. tostring(release_error))
local second_lease, handoff = second_lock:try_acquire(5000)
assert(second_lease, "successor failed after release: " .. tostring(handoff))

-- Reconstruct the stale local lease solely to exercise its owner-checked Lua
-- script after the successor owns the same key; production callers cannot
-- bypass Lease:release's cached terminal outcome.
local Lease = getmetatable(first_lease)
local stale_lease = setmetatable({ provider = first_provider, key = first_lease.key, owner = first_lease.owner }, Lease)
local stale_result, stale_error = stale_lease:release()
assert(stale_result == "already_not_owned", "stale release result: " .. tostring(stale_result) .. ", " .. tostring(stale_error))
local still_busy, still_busy_result = first_lock:try_acquire(5000)
assert(still_busy == nil and still_busy_result == "busy", "stale release removed successor ownership")
assert(second_lease:release() == "released", "successor cleanup failed")

local expiry_lock = assert(first_provider:lock(expiry_resource))
local short_lease = assert(expiry_lock:try_acquire(120))
ngx.sleep(0.2)
local after_expiry, expiry_result = expiry_lock:try_acquire(1000)
assert(after_expiry, "lease did not expire: " .. tostring(expiry_result))
assert(short_lease:release() == "already_not_owned", "expired owner release did not preserve successor state")
assert(after_expiry:release() == "released", "expired-key successor cleanup failed")

local wrong_type_lock = assert(first_provider:lock(wrong_type_resource))
local wrong_type_admin = assert(redis:new())
wrong_type_admin:set_timeouts(1000, 1000, 1000)
assert(wrong_type_admin:connect(redis_host, 6379))
assert(wrong_type_admin:hset(wrong_type_lock.key, "field", "value"))
wrong_type_admin:close()
local wrong_type_lease, wrong_type_result = wrong_type_lock:try_acquire(1000)
assert(wrong_type_lease == nil and wrong_type_result == "protocol_mismatch",
    "wrong-type lock key was not rejected: " .. tostring(wrong_type_result))
local cleanup = assert(redis:new())
cleanup:set_timeouts(1000, 1000, 1000)
assert(cleanup:connect(redis_host, 6379))
assert(cleanup:del(wrong_type_lock.key))
cleanup:close()

local wrong_metadata_provider = assert(dilock.new({ host = redis_host, port = 6379,
    namespace = wrong_metadata_namespace, epoch = epoch }))
local wrong_metadata_lock = assert(wrong_metadata_provider:lock("integration:wrong-metadata"))
local wrong_metadata_key = "dilock:namespace:" .. wrong_metadata_namespace .. ":meta"
local wrong_metadata_admin = assert(redis:new())
wrong_metadata_admin:set_timeouts(1000, 1000, 1000)
assert(wrong_metadata_admin:connect(redis_host, 6379))
assert(wrong_metadata_admin:hset(wrong_metadata_key, "field", "not-json"))
wrong_metadata_admin:close()
local wrong_metadata_lease, wrong_metadata_result = wrong_metadata_lock:try_acquire(1000)
assert(wrong_metadata_lease == nil and wrong_metadata_result == "protocol_mismatch",
    "wrong-type namespace metadata was not rejected as a protocol mismatch: " .. tostring(wrong_metadata_result))
local metadata_cleanup = assert(redis:new())
metadata_cleanup:set_timeouts(1000, 1000, 1000)
assert(metadata_cleanup:connect(redis_host, 6379))
assert(metadata_cleanup:del(wrong_metadata_key))
metadata_cleanup:close()

local scope_lock = assert(first_provider:lock("integration:scoped"))
local scoped_value, scoped_detail = scope_lock:with_lock(function(held)
    assert(held and held.owner, "scoped callback receives the Redis lease")
    return "scoped-work-completed"
end, 1000)
assert(scoped_value == "scoped-work-completed" and scoped_detail == nil,
    "Redis with_lock returns successful work only after acknowledged release")

local scope_lost_lock = assert(first_provider:lock("integration:scoped-expiry"))
local scope_successor
local lost_value, lost_error, lost_outcome = scope_lost_lock:with_lock(function()
    ngx.sleep(0.5)
    local successor_lock = assert(second_provider:lock("integration:scoped-expiry"))
    local scope_acquire_error
    scope_successor, scope_acquire_error = successor_lock:try_acquire(1000)
    assert(scope_successor, "successor should acquire after the scoped lease expires: " .. tostring(scope_acquire_error))
    return "must-not-look-protected"
end, 300)
assert(lost_value == nil and lost_error == "lease_lost" and lost_outcome == "already_not_owned",
    "Redis scope must reject a callback result after ownership is lost")
assert(scope_successor:release() == "released", "scoped successor cleanup failed")

ngx.say("PASS: OpenResty request exercised Redis contention, release handoff, stale owner cleanup, TTL expiry, and wrong-type lock/metadata rejection")
