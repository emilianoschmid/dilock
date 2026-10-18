local postgres = require "dilock.postgres"

local config = {
    database = os.getenv("DILOCK_POSTGRES_DATABASE") or "dilock",
    user = os.getenv("DILOCK_POSTGRES_USER") or "postgres",
    password = os.getenv("DILOCK_POSTGRES_PASSWORD") or "postgres",
    host = os.getenv("DILOCK_POSTGRES_HOST") or "postgres",
    port = tonumber(os.getenv("DILOCK_POSTGRES_PORT") or "5432"),
    namespace = os.getenv("DILOCK_TEST_NAMESPACE") or "lua-postgres-live",
    epoch = os.getenv("DILOCK_TEST_EPOCH") or "lua-postgres-live-epoch",
}

assert(postgres.provision(config))
local owner = assert(postgres.new(config))
local successor = assert(postgres.new(config))
local owner_lock = assert(owner:lock("lua/postgres-live"))
local successor_lock = assert(successor:lock("lua/postgres-live"))
local lease = assert(owner_lock:try_acquire())
assert(lease:is_valid())
local busy, category = successor_lock:try_acquire()
assert(busy == nil and category == "busy", category)
assert(lease:renew())
assert(lease:release() == "released")

local handoff = assert(successor_lock:try_acquire())
assert(handoff:is_valid())
assert(handoff:release() == "released")
assert(owner:close())
assert(successor:close())
print("PASS Lua PostgreSQL: provision, contention, liveness, release, and handoff")
