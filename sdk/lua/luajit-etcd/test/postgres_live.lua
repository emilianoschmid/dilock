local postgres = require "dilock_postgres_luajit"

local dsn = assert(os.getenv("DILOCK_POSTGRES_DSN"), "set DILOCK_POSTGRES_DSN")
local config = {
    connection_string = dsn,
    namespace = "luajit-postgres-live",
    epoch = "luajit-postgres-live-epoch",
    library_path = os.getenv("DILOCK_C_LIBRARY") or "dilock_c",
}

local api, category, detail = postgres.load(config.library_path)
assert(api, tostring(category) .. ": " .. tostring(detail))
assert(api:provision(config))

local owner = assert(api:new(config))
local contender = assert(api:new(config))
local owner_lock = assert(owner:lock("luajit/postgres-live"))
local contender_lock = assert(contender:lock("luajit/postgres-live"))
local lease = assert(owner_lock:try_acquire())
assert(lease:is_valid())
local busy, busy_category = contender_lock:try_acquire()
assert(busy == nil and busy_category == "busy", busy_category)
assert(lease:renew())
assert(lease:release() == "released")
lease:destroy()

local successor = assert(contender_lock:try_acquire())
assert(successor:is_valid())
assert(successor:release() == "released")
successor:destroy()
owner_lock:destroy()
contender_lock:destroy()
assert(owner:close())
assert(contender:close())
print("PASS LuaJIT PostgreSQL: provision, contention, liveness, release, and handoff")
