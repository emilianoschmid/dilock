# LuaJIT etcd quickstart

`main.lua` uses the standalone LuaJIT FFI bridge over Dilock C ABI v1. The native C++ etcd engine performs Lock/Lease RPCs, keepalive, verification, fencing and cleanup. This is a distinct experimental bridge, not a generic Lua etcd client or the OpenResty adapter.

Prerequisites: LuaJIT with FFI, the local LuaRocks package under `sdk/lua/luajit-etcd`, installed shared C ABI with etcd and its gRPC/C++ runtime dependencies, etcd, and pre-provisioned `billing` metadata for `etcd-native-lock-v1` and the configured epoch. Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_ETCD_NAMESPACE`, `DILOCK_ETCD_EPOCH`, and `DILOCK_C_LIBRARY`. The bridge validates metadata and never creates it.

After building/installing the C ABI and installing the local rock as documented in `sdk/lua/luajit-etcd/README.md`, run with LuaJIT:

```sh
luarocks make sdk/lua/luajit-etcd/dilock-etcd-luajit-0.1.0-1.rockspec
DILOCK_ETCD_ENDPOINT=127.0.0.1:2379 DILOCK_ETCD_NAMESPACE=billing DILOCK_ETCD_EPOCH=local-etcd-epoch DILOCK_C_LIBRARY="$PWD/.local/dilock-c/lib/libdilock_c.so" luajit docs/examples/lua-etcd/quickstart/main.lua
```

Adjust the shared-library path for the host OS. All native shared dependencies must be loadable. The example checks a fence; protected writes must atomically reject stale `(epoch, create_revision)` values. Use a disposable namespace.

This short path uses the ABI's default wait/lease and timing budgets while opting into auto-renew for the scoped work. The full configuration example in `sdk/lua/luajit-etcd/README.md` shows explicit wait/lease values and the remaining supported provider settings. Multiple resource handles from one native provider share its etcd client; use independent scopes sequentially unless you deliberately coordinate them, since they are not one atomic multi-lock operation.
