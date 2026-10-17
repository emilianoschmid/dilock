local script_path = arg and arg[0] or "test/consul_contract.lua"
local root = script_path:match("^(.*)/test/[^/]+$") or "."
package.path = root .. "/lib/?.lua;" .. package.path
local ffi_binding = require "dilock_etcd_luajit_ffi"
local consul = require "dilock_consul_luajit"
local function check(value, message) if not value then error(message, 2) end end

check(ffi_binding.decimal_u64(18446744073709551615ULL) == "18446744073709551615",
    "full unsigned Consul LockIndex must remain exact")
local config = { epoch = "epoch-1" }
local normalized = assert(consul.validate(config))
check(normalized.namespace == "default" and normalized.default_lease_ms == 30000,
    "Consul defaults are stable and validated")
local invalid, err = consul.validate({ epoch = "epoch-1", endpoint = "http://host/path" })
check(not invalid and err == "invalid_endpoint", "Consul endpoint must be an authority URL")
invalid, err = consul.validate({ epoch = "epoch-1", default_lease_ms = 9999 })
check(not invalid and err == "invalid_default_lease_ms", "Consul session TTL minimum must be enforced")
invalid, err = consul.validate({ epoch = "epoch-1", token = "bad\nheader" })
check(not invalid and err == "invalid_token", "ACL token must be header safe")
print("LuaJIT Consul facade contract: PASS")
