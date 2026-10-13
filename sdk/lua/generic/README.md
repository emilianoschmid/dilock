# Standalone Lua Redis SDK (experimental)

This package is separate from [`../`](../README.md), which is the
OpenResty/ngx_lua integration. It uses `nrk/redis-lua` over LuaSocket TCP and
targets Lua 5.1 and LuaJIT. The Redis client upstream documents compatibility
with Lua 5.1 and LuaJIT 2.0; its release is old, so this adapter remains
experimental.

Install locally from this directory with LuaRocks 3.13 or compatible:

```sh
luarocks make dilock-0.1.0-1.rockspec
```

Create one provider per Redis authority and deployment epoch. Metadata must
already be provisioned by an administrator; ordinary clients never create,
repair, or overwrite it. For a disposable development namespace, provision
the exact value explicitly:

```sh
redis-cli SET dilock:namespace:billing:meta \
  '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"deployment-2026-10","fixed_lease_ms":null}' NX
```

Minimal usage:

```lua
local dilock = require "dilock"
local provider, err = dilock.new({
    host = os.getenv("DILOCK_REDIS_HOST") or "127.0.0.1",
    port = 6379,
    namespace = "billing",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
    rpc_timeout_ms = 2000,
})
assert(provider, err)
local handle = assert(provider:lock("invoice:42"))
local lease, outcome = handle:try_acquire(30000)
if outcome == "busy" then
    return
end
assert(lease, outcome)

local ok, work_err = xpcall(function()
    -- Keep work within the lease or use application-level fencing elsewhere.
end, function(value) return value end)
local released, release_err = lease:release()
if not ok then error(work_err, 0) end
assert(released == "released", release_err or released)
```

`handle:try_acquire(ttl_ms)` is one-shot and returns `lease` or `nil, category`;
`nil, "busy"` is only acknowledged Redis contention. `lease:release()` returns
`released`, `already_not_owned`, or `unknown`. `handle:with_lock(fn, ttl_ms)`
is a synchronous finally-style scope and returns the callback's single value
after an acknowledged release. When cleanup does not return `released`, an
optional provider `cleanup_observer(event)` is called with `event.outcome`,
`event.error`, and (if the callback failed) `event.callback_error`. The observer
is protected: its own error never replaces the lock or callback result. On a
normal callback return, cleanup failure is returned as
`nil, outcome, detail, observer_error`. If both the callback and cleanup fail,
`with_lock` raises a table with `kind = "dilock_with_lock_error"`, the exact
callback error in `cause`, and public `cleanup_outcome`, `cleanup_error`, and
`observer_error` fields. Callback errors still propagate unchanged when cleanup
is acknowledged. The observer is intended for synchronous diagnostics; it
must not block indefinitely or perform lock recovery.

The API blocks the current Lua thread during Redis I/O; LuaSocket timeouts are
bounded by `rpc_timeout_ms`. There is no async cancellation or automatic
renewal. TLS is rejected as `unsupported` by this first adapter version. AUTH
with password and optional ACL username is supported. Errors never become
ordinary contention; a SET transport error is `acquisition_unknown` followed
by bounded best-effort owner-checked cleanup. Redis owner tokens are not
fencing tokens, and lease expiry does not stop a process or work already
running. Redis failover/eviction guarantees remain those of the shared
single-authority profile.

This SDK does not alter or replace the OpenResty integration. Verify the
selected Lua runtime and Redis client in your own build environment.
