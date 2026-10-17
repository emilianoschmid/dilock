# Standalone LuaJIT etcd bridge (experimental)

This is a separate synchronous LuaJIT package that calls Dilock C ABI v1. The
distributed lock protocol, native etcd Lock/Lease RPCs, keepalive, verification,
fencing, and bounded cleanup remain in the installed Dilock C++ etcd backend.
This bridge is not a generic Lua engine and is not the OpenResty adapter.

Build and install the C ABI with the etcd backend enabled, then install this
local rock:

```sh
luarocks make dilock-etcd-luajit-0.1.0-1.rockspec
```

The runtime must be LuaJIT with FFI and the C ABI's dynamic dependencies
available to the system loader. Pass `library_path` if `libdilock_c` is not on
the default loader path. The native library must report ABI version 1.

```lua
local dilock = require "dilock_etcd_luajit"
local provider, category, detail = dilock.new({
    endpoint = os.getenv("DILOCK_ETCD_ENDPOINT") or "127.0.0.1:2379",
    namespace = "billing",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
    rpc_timeout_ms = 2000,
    cleanup_timeout_ms = 2000,
    safety_margin_ms = 25,
    default_wait_ms = 5000,
    default_lease_ms = 30000,
    auto_renew = true,
})
assert(provider, tostring(category) .. ": " .. tostring(detail))

local handle = assert(provider:lock("invoice:42"))
local result, outcome, detail = handle:with_lock(function(lease)
    -- Persist and enforce this epoch-scoped fence atomically with protected data.
    return update_invoice(lease.fence.coordination_epoch, lease.fence.create_revision)
end, { wait_ms = 5000, lease_ms = 30000 })
assert(outcome == nil, tostring(outcome) .. ": " .. tostring(detail))
assert(provider:close())
```

`acquire` makes one native etcd Lock attempt with a finite wait budget. The C++
backend grants a unique lease, keeps queued attempts alive, verifies the exact
returned Lock key and lease, and reads `create_revision`. `fence.create_revision`
stays a decimal string so all signed 64-bit values remain exact in Lua. Native
`try_acquire` is unsupported and returns `unsupported_capability`.
`handle:with_lock(fn, options)` is a synchronous scope: it returns the
callback's first result only after acknowledged release. If the callback raises
and release succeeds, the original error is rethrown unchanged. If release is
not acknowledged, successful callback results are withheld; when both callback
and cleanup fail, the raised table preserves `cause`, `cleanup_outcome`, and
`cleanup_error`. Explicit `acquire`/`release` remains available when the scope
does not fit the caller's control flow.

FFI calls are synchronous and block the calling LuaJIT thread. The bridge does
not expose mid-call cancellation, and Lua coroutines cannot interrupt a native
call. Acquisition is still bounded by `wait_ms` plus the configured native RPC
and cleanup budgets; timeout cleanup revokes that attempt's unique lease. A
client that needs prompt external cancellation must run the call in a separately
managed OS thread/process and must not report cancellation until the native call
returns with confirmed cleanup. Do not destroy a provider while a lease is
active: `provider:close()` returns `provider_busy`. Release every lease and
close each provider explicitly. The API is not safe for concurrent calls from
multiple OS threads through one LuaJIT state.

The C ABI's `auto_renew` defaults to false. Enable it for protected work that
may outlive the requested TTL, and still check `lease:is_valid()` as a local
observation. Expiry cannot stop a stale process. etcd fencing is effective only
when the protected resource atomically registers the epoch/revision and rejects
stale writes. TLS certificate paths are optional; without them the C ABI uses
insecure gRPC credentials. Namespace metadata must be provisioned beforehand.

This bridge remains experimental. Build and test it against the exact
LuaJIT, native ABI, and etcd client versions used by your application.

## Consul facade

The same LuaJIT rock also provides `dilock_consul_luajit`, a synchronous
facade over the shared C ABI Consul engine. This keeps Consul session/KV
semantics, exact `LockIndex` fencing, and bounded cleanup in the native
backend. Provision namespace metadata explicitly with `dilock.provision()`
when operating the namespace, then create normal providers with `dilock.new()`.

```lua
local consul = require "dilock_consul_luajit"
local provider = assert(consul.new({
    endpoint = "http://127.0.0.1:8500",
    namespace = "billing",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
}))
local lock = assert(provider:lock("invoice:42"))
local result = assert(lock:with_lock(function(lease)
    return update_invoice(lease.fence.coordination_epoch, lease.fence.lock_index)
end))
assert(provider:close())
```

`try_acquire`, `acquire`, `lease:is_valid()`, `lease:renew()`, and
`lease:release()` are available. Fences preserve the full unsigned 64-bit
index as a decimal string. The C ABI provider validates existing metadata;
ordinary provider construction never provisions or repairs it.
