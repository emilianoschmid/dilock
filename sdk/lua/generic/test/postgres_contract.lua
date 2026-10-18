local postgres = require "dilock.postgres"
assert(type(postgres.advisory_key) == "function")
assert(postgres.advisory_key("8000000000000000" .. string.rep("0", 48)) == "-9223372036854775808")
assert(postgres.advisory_key("7fffffffffffffff" .. string.rep("0", 48)) == "9223372036854775807")
print("PASS: Lua PostgreSQL advisory-key contract")
