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

## Experimental Consul adapter

The package also exports `require("dilock.consul")`. This Lua 5.1-compatible
adapter uses LuaSocket HTTP and Consul KV sessions. It supports plain HTTP
authorities only; HTTPS endpoints fail closed because this package has no
verified TLS HTTP transport. Do not send ACL tokens over an untrusted network.
Provision the namespace explicitly with `consul.provision(config)` using
Consul CAS=0, then construct a provider with `consul.new(config)`; normal
provider construction only validates existing metadata. Each attempt owns a
unique Consul session. `handle:acquire({wait_ms, lease_ms})` waits finitely,
`handle:try_acquire(lease_ms)` makes one attempt, and `lease:renew()` must be
called explicitly for long work. `lease.fence` contains the epoch and exact
decimal LockIndex. Call `lease:release()` and `provider:close()`; protected
storage must atomically enforce that fence.

```lua
local consul = require "dilock.consul"
local provider = assert(consul.new({
    endpoint = "http://127.0.0.1:8500",
    namespace = "billing",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
}))
local lock = assert(provider:lock("invoice:42"))
local result = assert(lock:with_lock(function(lease)
    return update_invoice(lease.fence.coordination_epoch, lease.fence.lock_index)
end, { wait_ms = 5000, lease_ms = 30000 }))
assert(provider:close())
```

## Experimental PostgreSQL adapter

`require("dilock.postgres")` uses LuaSQL PostgreSQL connections and keeps the
physical connection that acquired `pg_try_advisory_lock` inside the lease.
Provisioning is explicit through `postgres.provision(config)`; `postgres.new`
only validates the existing `dilock_namespace_meta` row. This profile has no
TTL and no fencing token. LuaSQL connection parameters are supplied as
`database`, `user`, `password`, `host`, and `port`.

```lua
local postgres = require "dilock.postgres"
local config = {
    database = "dilock", host = "127.0.0.1", port = 5432,
    user = "postgres", password = assert(os.getenv("DILOCK_POSTGRES_PASSWORD")),
    namespace = "billing", epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
}
assert(postgres.provision(config))
local provider = assert(postgres.new(config))
local lease = assert(assert(provider:lock("invoice:42")):try_acquire())
assert(lease:renew()) -- liveness only; it does not extend a PostgreSQL TTL
assert(lease:release() == "released")
assert(provider:close())
```

This adapter is experimental and requires LuaSQL PostgreSQL plus a compatible
LuaSocket/LuaSystem runtime. The companion `dilock_postgres_luajit` module in
the `luajit-etcd` rock uses the native C ABI route for LuaJIT deployments.
