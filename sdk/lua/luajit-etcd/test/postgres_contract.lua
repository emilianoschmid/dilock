local postgres = require "dilock_postgres_luajit"
assert(type(postgres.load) == "function")
assert(type(postgres.new) == "function")
local api, category = postgres.load("definitely-missing-dilock-c-library")
assert(api == nil and category == "backend_unavailable")
print("PASS: LuaJIT PostgreSQL facade contract and missing-library handling")
