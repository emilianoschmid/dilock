# Lua Redis quickstart (OpenResty)

`main.lua` is an OpenResty/ngx_lua request-handler example using Dilock's `resty.redis` adapter. It connects directly to Redis from a request context and does not use the C++ engine. This adapter is experimental, not a generic standalone Lua client or stable release.

Prerequisites: OpenResty with ngx_lua cosockets, `lua-resty-redis`, the Dilock module on `lua_package_path`, Redis, and pre-provisioned `billing` namespace metadata for the sample epoch. Set `DILOCK_REDIS_HOST`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to match the provisioned metadata. Configure nginx `resolver` for the Redis host. Redis's built-in Lua engine runs the owner-checked release script; operators do not install a separate Lua service or manually load scripts. Redis ACLs must allow the required commands, including `EVAL`.

Use this file as a request handler, for example in a compatible nginx location:

```nginx
lua_package_path "/path/to/dilock/sdk/lua/lib/?.lua;;";
location = /dilock-quickstart {
    content_by_lua_file /path/to/dilock/docs/examples/lua-redis/quickstart/main.lua;
}
```

Expose the route only in a local test deployment and invoke it through OpenResty. Do not call it from a non-request Lua context; `ngx` and cosockets are request-scoped. The handler never provisions namespace metadata.

The code keeps provider defaults and overrides only connection identity through environment settings. For the full Redis configuration surface (TLS verification, ACL credentials, timeouts, namespace and explicit TTL), use the configured request example in `sdk/lua/README.md`. This adapter opens a connection for each provider operation; distinct handles do not share a cosocket connection, and module-level cosocket reuse is unsupported.
