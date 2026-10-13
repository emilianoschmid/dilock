local redis = require "redis"
local json = require "dkjson"
local system = require "system"
local dilock = require "dilock"

local function check(value, message)
    if not value then error(message or "assertion failed", 2) end
end

local function client()
    return redis.connect({ host = assert(os.getenv("REDIS_HOST")), port = 6379, timeout = 2 })
end

local namespace = "lua-generic-validation"
local epoch = "docker-redis-20261008"
local metadata_key = "dilock:namespace:" .. namespace .. ":meta"
local metadata = json.encode({
    protocol_major = 1,
    identity_scheme = "sha256-framed-utf8-v1",
    profile = "redis-single-authority-v1",
    coordination_epoch = epoch,
    fixed_lease_ms = json.null,
})

local admin = client()
admin:del(metadata_key)
admin:del("dilock:namespace:" .. namespace .. ":wrong-type")
check(admin:set(metadata_key, metadata, "NX") == true, "namespace metadata provision failed")

local provider = assert(dilock.new({ host = assert(os.getenv("REDIS_HOST")), namespace = namespace,
    epoch = epoch, rpc_timeout_ms = 2000 }))
local wrong_epoch = assert(dilock.new({ host = os.getenv("REDIS_HOST"), namespace = namespace,
    epoch = "wrong-epoch", rpc_timeout_ms = 2000 }))
local handle = assert(provider:lock("consumer/redis"))
local no_lease, invalid_ttl = handle:try_acquire(0)
check(no_lease == nil and invalid_ttl == "invalid_ttl", "invalid TTL was not rejected before Redis I/O")
local lease, result = handle:try_acquire(2000)
check(lease ~= nil, "acquire failed: " .. tostring(result))

local id = assert(dilock.resource_id(namespace, "consumer/redis"))
local key = "dilock:{" .. id .. "}:lease"
local owner = admin:get(key)
check(type(owner) == "string" and owner:match("^dilock/1/%x%x%x%x%x%x%x%x"), "owner token format invalid")
check(#owner == #"dilock/1/" + 64, "owner token must encode 32 random bytes")
local ttl = admin:pttl(key)
check(type(ttl) == "number" and ttl > 0 and ttl <= 2000, "lease TTL not installed")
local contested, busy = handle:try_acquire(2000)
check(contested == nil and busy == "busy", "contender must report acknowledged busy")

local mismatch, mismatch_err = wrong_epoch:lock("consumer/redis"):try_acquire(2000)
check(mismatch == nil and mismatch_err == "protocol_mismatch", "epoch mismatch was not rejected")

-- A stale lease must not delete a successor after expiry/reacquisition.
local short_handle = assert(provider:lock("expiry/reacquire"))
local old = assert(short_handle:try_acquire(120))
system.sleep(0.2)
local successor = assert(short_handle:try_acquire(2000))
local stale_outcome = old:release()
check(stale_outcome == "already_not_owned", "expired lease did not observe loss of ownership")
local successor_owner = admin:get("dilock:{" .. assert(dilock.resource_id(namespace, "expiry/reacquire")) .. "}:lease")
check(successor_owner == successor.owner, "stale release deleted successor")
check(successor:release() == "released", "successor release failed")

-- Wrong-type lock keys are protocol mismatches and remain untouched.
local malformed = assert(provider:lock("wrong-type"))
local malformed_id = assert(dilock.resource_id(namespace, "wrong-type"))
local malformed_key = "dilock:{" .. malformed_id .. "}:lease"
admin:hset(malformed_key, "field", "value")
local malformed_lease, malformed_err = malformed:try_acquire(2000)
check(malformed_lease == nil and malformed_err == "protocol_mismatch", "wrong-type contention was hidden")
check(admin:type(malformed_key) == "hash", "wrong-type key was modified")
admin:del(malformed_key)

-- Metadata with a mismatched epoch is rejected before attempting acquisition.
local missing = assert(dilock.new({ host = os.getenv("REDIS_HOST"), namespace = "lua-generic-missing",
    epoch = epoch, rpc_timeout_ms = 2000 }))
local no_meta, no_meta_err = missing:lock("resource"):try_acquire(2000)
check(no_meta == nil and no_meta_err == "protocol_mismatch", "missing metadata was silently created")

local wrong_metadata_namespace = "lua-generic-metadata-type"
local wrong_metadata_key = "dilock:namespace:" .. wrong_metadata_namespace .. ":meta"
admin:hset(wrong_metadata_key, "field", "value")
local wrong_metadata_provider = assert(dilock.new({ host = os.getenv("REDIS_HOST"),
    namespace = wrong_metadata_namespace, epoch = epoch, rpc_timeout_ms = 2000 }))
local wrong_metadata_lease, wrong_metadata_err = wrong_metadata_provider:lock("resource"):try_acquire(2000)
check(wrong_metadata_lease == nil and wrong_metadata_err == "protocol_mismatch",
    "wrong-type namespace metadata was hidden as a backend error")
check(admin:type(wrong_metadata_key) == "hash", "wrong-type metadata key was modified")
admin:del(wrong_metadata_key)

local scoped = assert(provider:lock("scope"))
local scope_result = scoped:with_lock(function() return "scope-ok" end, 2000)
check(scope_result == "scope-ok", "with_lock result or release failed")
local body_error = pcall(function()
    scoped:with_lock(function() error("expected-body-error") end, 2000)
end)
check(not body_error, "with_lock must propagate body errors")
local after_scope = assert(scoped:try_acquire(2000))
check(after_scope:release() == "released", "with_lock leaked the lease")

local release_outcome = lease:release()
check(release_outcome == "released", "lease release failed")
check(lease:release() == "released", "release must be idempotent")
admin:del(metadata_key)
admin.network.socket:close()
print("Lua generic Redis 8 consumer: PASS")
