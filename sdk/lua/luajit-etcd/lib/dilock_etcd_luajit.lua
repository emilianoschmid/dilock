-- Public standalone LuaJIT etcd adapter over the separately installed C ABI.
local core = require "dilock_etcd_luajit_core"
local native = require "dilock_etcd_luajit_ffi"

local M = {}

--- Create a synchronous LuaJIT provider backed by the installed Dilock C ABI.
-- The C ABI owns etcd gRPC, native Lock/Lease semantics, fencing and cleanup.
-- @param config table with `epoch`; optional endpoint, namespace, timeout,
-- TLS paths, auto_renew, and library_path settings.
-- @return provider or nil, category, detail. C ABI v1 and its native runtime
-- dependencies must be installed and discoverable by the dynamic loader.
function M.new(config)
    if config == nil then return nil, "invalid_config" end
    local normalized, validation_error = core.validate(config)
    if not normalized then return nil, validation_error end
    local api, status, detail = native.load(config.library_path)
    if not api then return nil, status, detail end
    return core.new(normalized, api)
end

return M
