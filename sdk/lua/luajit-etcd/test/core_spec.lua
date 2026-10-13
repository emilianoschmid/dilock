local script_path = arg and arg[0] or "test/core_spec.lua"
local package_root = script_path:match("^(.*)/test/[^/]+$") or "."
package.path = package_root .. "/lib/?.lua;" .. package.path
local core = require "dilock_etcd_luajit_core"
local ffi_binding = require "dilock_etcd_luajit_ffi"
local function check(value, message) if not value then error(message, 2) end end

check(ffi_binding.decimal_i64(9007199254740993LL) == "9007199254740993",
    "int64 revisions beyond Lua's exact-number range remain decimal strings")
check(ffi_binding.decimal_i64(9223372036854775807LL) == "9223372036854775807",
    "maximum signed int64 revision remains exact")

local function fake_api()
    local api = { version = 1, calls = {}, outcome = "released", valid = true }
    local function record(name) api.calls[#api.calls + 1] = name end
    function api:abi_version() record("abi_version"); return self.version end
    function api:provider_create(config)
        record("provider_create")
        self.config = config
        if self.provider_error then return nil, self.provider_error, "metadata unavailable" end
        return { kind = "provider" }
    end
    function api:provider_destroy() record("provider_destroy") end
    function api:lock_create(_, resource) record("lock_create"); return { resource = resource } end
    function api:lock_destroy() record("lock_destroy") end
    function api:acquire(_, wait, ttl)
        record("acquire")
        self.wait, self.ttl = wait, ttl
        if self.acquire_error then return nil, self.acquire_error, "injected native error" end
        return { kind = "lease" }
    end
    function api:lease_info()
        record("lease_info")
        if self.info_error then return nil, self.info_error, "fence read failed" end
        return { create_revision = "9223372036854775807", coordination_epoch = "epoch-1", effective_ttl_ms = 30000 }
    end
    function api:lease_valid() record("lease_valid"); return self.valid end
    function api:lease_release() record("lease_release"); return self.outcome end
    function api:lease_destroy() record("lease_destroy") end
    return api
end

local invalid, err = core.new(true, fake_api())
check(not invalid and err == "invalid_config", "non-table options must fail explicitly")
invalid, err = core.new({ epoch = "epoch-1", endpoint = false }, fake_api())
check(not invalid and err == "invalid_endpoint", "false endpoint must not silently select a default")
invalid, err = core.new({ epoch = "epoch-1", library_path = 12 }, fake_api())
check(not invalid and err == "invalid_library_path", "library path type must be validated")
invalid, err = core.new({ epoch = "epoch-1", tls_client_certificate = "client.pem" }, fake_api())
check(not invalid and err == "invalid_tls_client_identity",
    "TLS client certificate and key must be configured together")
local mismatched = fake_api(); mismatched.version = 9
invalid, err = core.new({ epoch = "epoch-1" }, mismatched)
check(not invalid and err == "unsupported_c_abi_version", "unknown ABI version must fail closed")
local unavailable = fake_api(); unavailable.provider_error = "backend_unavailable"
invalid, err = core.new({ epoch = "epoch-1" }, unavailable)
check(not invalid and err == "backend_unavailable", "metadata outage must not construct a provider")
local cleanup_api = fake_api(); cleanup_api.info_error = "protocol_mismatch"
local cleanup_provider = assert(core.new({ epoch = "epoch-1" }, cleanup_api))
local cleanup_lock = assert(cleanup_provider:lock("fence-read-failure"))
local not_returned, info_error = cleanup_lock:acquire()
check(not not_returned and info_error == "protocol_mismatch",
    "failure to obtain fence evidence must not return a lease")
local cleanup_count = 0
for _, call in ipairs(cleanup_api.calls) do
    if call == "lease_release" then cleanup_count = cleanup_count + 1 end
    if call == "lease_destroy" then cleanup_count = cleanup_count + 1 end
end
check(cleanup_count == 2, "unreturned lease receives one release and one native destruction")
check(cleanup_provider:close(), "provider closes after failed acquisition evidence is cleaned")

local api = fake_api()
local provider = assert(core.new({ epoch = "epoch-1" }, api))
local first = assert(provider:lock("invoice:42"))
check(provider:lock("invoice:42") == first, "same resource reuses one native handle")
local unsupported, capability = first:try_acquire()
check(not unsupported and capability:find("unsupported_capability", 1, true),
    "etcd native Lock must not pretend try_acquire exists")
check(select(2, provider:lock("bad\0resource")) == "invalid_resource",
    "NUL resource must not be truncated at the C ABI boundary")
check(select(2, first:acquire({ wait_ms = 0 })) == "invalid_wait_ms", "zero wait must be rejected")
check(select(2, first:acquire(false)) == "invalid_options",
    "false options must not silently select the default acquisition policy")
check(select(2, first:acquire({ lease_ms = 2000 })) == "invalid_lease_ms",
    "lease TTL must exceed RPC timeout plus safety margin")

api.acquire_error = "wait_timeout"
local none, timeout = first:acquire({ wait_ms = 17, lease_ms = 4000 })
check(not none and timeout == "wait_timeout" and api.wait == 17,
    "bounded timeout stays distinct from contention")
api.acquire_error = nil
local lease = assert(first:acquire({ wait_ms = 19, lease_ms = 5000 }))
check(api.wait == 19 and api.ttl == 5000, "explicit wait and TTL reach the native API")
check(lease.fence.create_revision == "9223372036854775807",
    "fence remains an exact decimal string without Lua-number conversion")
check(lease.fence.coordination_epoch == "epoch-1" and lease.effective_ttl_ms == 30000,
    "lease returns verified epoch and effective server TTL")
check(lease:is_valid() == true, "validity delegates to the native lease")
local closed, busy = provider:close()
check(not closed and busy:find("provider_busy", 1, true), "provider cannot close under an active lease")

api.outcome = "unknown"
local release, release_error = lease:release()
check(release == "unknown", "unknown cleanup remains distinct")
local calls_before_repeat = #api.calls
local repeated, repeated_error = lease:release()
check(repeated == "unknown" and repeated_error == release_error and #api.calls == calls_before_repeat,
    "release result is cached and the native lease is destroyed exactly once")
check(provider:close(), "provider closes after lease release")
check(provider:close(), "provider close is idempotent")
local after_close, close_error = provider:lock("closed")
check(not after_close and close_error == "provider_closed", "closed provider rejects new handles")

local scoped_api = fake_api()
local scoped_provider = assert(core.new({ epoch = "epoch-1" }, scoped_api))
local scoped_lock = assert(scoped_provider:lock("scoped"))
local callback_value = scoped_lock:with_lock(function(active)
    check(active:is_valid() == true, "with_lock callback receives the active lease")
    return "completed"
end)
check(callback_value == "completed", "with_lock returns callback value after acknowledged release")
check(scoped_api.calls[#scoped_api.calls - 1] == "lease_release"
    and scoped_api.calls[#scoped_api.calls] == "lease_destroy",
    "with_lock releases and destroys before returning")

local callback_marker = {}
local callback_ok, callback_error = pcall(function()
    scoped_lock:with_lock(function() error(callback_marker, 0) end)
end)
check(not callback_ok and callback_error == callback_marker,
    "with_lock preserves exact callback error when cleanup is acknowledged")

scoped_api.outcome = "unknown"
local combined_ok, combined_error = pcall(function()
    scoped_lock:with_lock(function() error(callback_marker, 0) end)
end)
check(not combined_ok and type(combined_error) == "table"
    and combined_error.kind == "dilock_with_lock_error"
    and combined_error.cause == callback_marker
    and combined_error.cleanup_outcome == "unknown",
    "with_lock exposes callback cause and unknown cleanup together")
local cleanup_failed, cleanup_outcome = scoped_lock:with_lock(function() return "must-not-escape" end)
check(cleanup_failed == nil and cleanup_outcome == "unknown",
    "with_lock never returns successful work when cleanup is unacknowledged")
check(scoped_provider:close(), "scoped provider closes after all callback leases are destroyed")

print("LuaJIT C ABI etcd core deterministic contract: PASS")
