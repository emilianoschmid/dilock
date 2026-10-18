package = "dilock-etcd-luajit"
version = "0.1.0-1"
source = { url = "file://." }
description = {
   summary = "Standalone LuaJIT etcd adapter over Dilock's C ABI",
   detailed = "Experimental synchronous LuaJIT bridge; etcd protocol and leases are implemented by the separately installed Dilock C ABI.",
   license = "Apache-2.0",
}
dependencies = {
   "lua >= 5.1",
}
build = {
   type = "builtin",
   modules = {
      dilock_etcd_luajit = "lib/dilock_etcd_luajit.lua",
      dilock_etcd_luajit_core = "lib/dilock_etcd_luajit_core.lua",
      dilock_etcd_luajit_ffi = "lib/dilock_etcd_luajit_ffi.lua",
      dilock_consul_luajit = "lib/dilock_consul_luajit.lua",
      dilock_postgres_luajit = "lib/dilock_postgres_luajit.lua",
   },
}
