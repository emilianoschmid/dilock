# Dilock Lua SDKs

This README documents the OpenResty/ngx_lua adapter in `lib/`. The separate
standalone Lua 5.1/LuaJIT Redis package is experimental and lives under
[`generic/`](generic/README.md); it has its own dependencies and validation
scope. Neither implementation changes or replaces the other.

The separate [`luajit-etcd/`](luajit-etcd/README.md) package is a synchronous
LuaJIT bridge over the installed Dilock C ABI. It is experimental and has separate dependencies; it does not make the
OpenResty facade a generic Lua engine.

## OpenResty/ngx_lua adapter

This is an OpenResty/ngx_lua integration, not a generic Lua or LuaJIT client.
It uses `resty.redis` cosockets and must be called from a request context where
cosockets are available. The Redis facade does not support automatic renewal or strong fencing. Each Redis operation creates and closes its own connection
object; never store a `resty.redis` object in module-level shared state.
Configure nginx's `resolver` for the DNS service used by the configured Redis
host; ngx_lua cosockets resolve hostnames through nginx's resolver directive.

Provision the namespace metadata key before use. The `try_acquire` API returns
`nil, "busy"` only after Redis acknowledges `SET NX` contention; other backend
errors return an error category. For bounded callback work, use
`handle:with_lock(fn, ttl_ms)`: it awaits owner-checked release before returning
callback values. A successful callback returns `nil, "lease_lost"` if Redis no
longer confirms this owner, or `nil, "release_unknown"` if cleanup is
unacknowledged. If the callback raises, `with_lock` returns `nil`, its traceback,
and the release outcome/error separately so cleanup cannot mask the application
failure. The helper is synchronous and must run in an OpenResty request context
where cosockets are available. For custom lifetime handling, explicitly call
`lease:release()` in a Lua `xpcall`/finally-style cleanup path. Request abort
and worker shutdown behavior must be evaluated in the host application; a Redis
lease expiry does not stop work already running in a worker.

See `example.lua` for request-handler usage and `test/` for the OpenResty
request integration fixture. This adapter is experimental and requires a
compatible native installation.

This is a Lua client adapter: it connects to Redis through `lua-resty-redis`
and sends normal Redis commands from an OpenResty request. Owner-checked
release uses an embedded Redis Lua script sent with `EVAL`; Redis already
contains the Lua engine, so operators do not install a separate Lua service or
manually load Dilock scripts. Redis ACLs must allow the required commands,
including `EVAL`.

The short example uses loopback, the default namespace and timeouts, and a
30-second requested lease. The fully configured request below shows all
provider fields currently accepted by `dilock.new` and the explicit TTL on
`try_acquire`. Supply credentials through the deployment's secret environment,
not source control.

`dilock.new` rejects malformed connection settings before opening a socket:
host and TLS server name must be non-empty strings of at most 255 characters
without whitespace or NUL; port must be an integer from 1 through 65535; each
connect/send/read timeout must be an integer from 1 through 120000 ms. TLS must
be a boolean, and an ACL username requires a non-empty password. These checks
return `nil, "invalid_config"` rather than allowing a client-library exception
or silently ignoring a malformed option.

When `tls = true`, certificate verification defaults to enabled. Set
`verify_tls = false` only for a deliberately unverified connection; this skips
server identity checks and permits man-in-the-middle interception.

```lua
local dilock = require "dilock"
local provider, err = dilock.new({
    host = assert(os.getenv("DILOCK_REDIS_HOST"), "DILOCK_REDIS_HOST is required"),
    port = 6380,
    username = os.getenv("DILOCK_REDIS_USERNAME"),
    password = os.getenv("DILOCK_REDIS_PASSWORD"),
    tls = true,
    verify_tls = true,
    server_name = "redis.internal",
    connect_timeout = 1500,
    send_timeout = 1500,
    read_timeout = 1500,
    namespace = "reports",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH"), "DILOCK_COORDINATION_EPOCH is required"),
})
assert(provider, err)
local handle
handle, err = provider:lock("daily")
assert(handle, err)
local value, scope_err, release_outcome, release_err = handle:with_lock(function(lease)
    -- Keep protected work within the lease; expiry does not stop stale work.
    return "report:daily:updated"
end, 60000)
if scope_err == "busy" then return ngx.exit(ngx.HTTP_CONFLICT) end
if scope_err == "release_unknown" then
    ngx.log(ngx.ERR, "Dilock release outcome unknown: ", release_err or "")
    return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end
if scope_err == "lease_lost" then
    ngx.log(ngx.ERR, "Dilock lease lost during scoped work: ", release_outcome or "")
    return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end
if scope_err then
    ngx.log(ngx.ERR, "Dilock scoped operation failed: ", scope_err)
    return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end
ngx.say(value)
```

`try_acquire` remains a one-shot operation: there is no wait-budget option.
The Redis connection is opened per provider operation in the request context;
never share a cosocket object at module scope.

## Experimental etcd backend (OpenResty Linux/glibc only)

The separate dilock_etcd facade uses Dilock's installed native C ABI and its etcd gRPC client; it does not use the etcd HTTP gateway, a sidecar, or a Lua gRPC package. Create one reusable provider, obtain resource handles, and use provider:with_lock(resource, fn) for finally-style release. A provider lazily opens one native etcd client per OpenResty worker. Distinct resource handles made from that provider share the client. provider:close() closes resource handles and the provider. Call require("dilock_etcd").shutdown_all() from exit_worker_by_lua_block after nginx drains requests. Normal close runs in the configured thread pool. OpenResty disables ngx.run_worker_thread during exit_worker_by_lua, so worker shutdown uses only local native destructors after confirming no acquisition or lease is active; it makes no backend RPC.

This OpenResty facade requires Linux with glibc, the Dilock C ABI and its runtime dependencies (libdilock_c, libdilock_cpp, libdilock_cpp_etcd, gRPC C++ and protobuf), and OpenResty 1.27.1.2 on Debian Bookworm as tested. Alpine/musl is unsupported for this facade. It is separate from the standalone LuaJIT C ABI bridge documented in [`luajit-etcd/`](luajit-etcd/README.md). Configure nginx:

~~~nginx
thread_pool dilock threads=4 max_queue=64;
lua_check_client_abort on;
lua_worker_thread_vm_pool_size 100;
lua_package_path "/opt/dilock/lua/lib/?.lua;;";
exit_worker_by_lua_block {
    local ok, err = require("dilock_etcd").shutdown_all()
    if not ok then ngx.log(ngx.ERR, "Dilock shutdown cleanup failed: ", err) end
}
~~~

Keep a provider in a worker-scoped Lua module and reuse it across resources:

~~~lua
-- In a module loaded once per OpenResty worker:
local dilock = require "dilock_etcd"
local provider, err = dilock.new({
    endpoint = os.getenv("DILOCK_ETCD_ENDPOINT") or "127.0.0.1:2379",
    namespace = "billing",
    epoch = assert(os.getenv("DILOCK_COORDINATION_EPOCH")),
    default_wait_ms = 5000,
    default_lease_ms = 30000,
    rpc_timeout_ms = 2000,
    cleanup_timeout_ms = 2000,
    safety_margin_ms = 100,
    auto_renew = true,
    thread_pool = "dilock",
})
assert(provider, err)
return provider
~~~

~~~lua
-- In an ngx_lua request handler:
local provider = require "billing.dilock_provider"
local result, err = provider:with_lock("invoice:42", function(lease)
    -- Store the fencing revision with the protected data and reject stale
    -- revisions at the resource being protected.
    return update_invoice(lease.fence.create_revision)
end)
if not result then ngx.log(ngx.ERR, "Dilock operation failed: ", err) end
~~~

`with_lock` returns the callback values only after release returns `released`.
If etcd confirms the key is no longer owned, it returns `nil, "lease_lost",
"already_not_owned"`; an unknown release returns `nil, "release_unknown", ...`.
When the callback itself fails, its traceback remains the primary error and the
release outcome is returned separately.

The acquired etcd lease uses the native Lock/Lease lifecycle, including keepalive, verification that the Lock key is attached to the exact lease, and create_revision fencing. The revision is returned as an exact decimal string; Lua numbers cannot safely represent all signed 64-bit revisions. try_acquire is unsupported because native etcd Lock queues; use acquire with wait_ms. Busy, cancellation, timeout, backend failure, acquisition uncertainty, protocol mismatch and lease loss remain distinct outcomes.

If native acquisition succeeds but reading the fence or effective TTL fails,
the worker attempts bounded owner-specific lease cleanup before returning. An
unacknowledged cleanup is reported as `acquisition_unknown`; the acquired
lease is never handed to application code without its fence and TTL details.

All etcd C ABI operations, including ABI-version checks, lease validity and
cancellation-token create/request/destroy, run in the configured OpenResty
worker thread pool. Only integer pointer halves and other primitive values cross
the worker boundary. The abort callback waits for the short cancellation signal
worker before acquisition can destroy its token; it never releases an active
lease while application work is still running.

A client disconnect during queued acquisition signals the C ABI cancellation handle. The operation reports cancelled only after the native backend has attempted and acknowledged bounded lease cleanup. A disconnect during protected application work only sets lease:abort_requested(). The callback must stop cooperatively; with_lock releases in its finally path only after that callback returns. OpenResty cannot stop arbitrary application work, and releasing while it continues could permit overlapping owners. A callback that does not return can therefore keep its lease alive through auto-renew. Lease expiry does not stop a process, and fencing is effective only when the protected resource enforces the fence.

This adapter is experimental. OpenResty documents ngx.run_worker_thread as experimental and Linux-only; deployment compatibility, TLS, multi-worker reload/failure behavior, production thread-pool sizing, and cross-language interoperability remain not covered by the SDK's documented profile. It requires a working, ABI-compatible native install. It has not been validated on Alpine, generic Lua, or other OpenResty releases.
