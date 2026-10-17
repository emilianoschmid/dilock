package = "dilock"
version = "0.1.0-1"
source = {
   url = "file://.",
}
description = {
   summary = "Standalone Lua Redis and Consul distributed-lock client",
   detailed = "Experimental synchronous Lua 5.1/LuaJIT Redis and Consul lock SDK.",
   license = "Apache-2.0",
}
dependencies = {
   "lua >= 5.1",
   "redis-lua = 2.0.4-1",
   "luasocket = 3.1.0-1",
   "luaossl = 20260910-0",
   "dkjson = 2.11-1",
   "luasystem = 0.7.1-1",
}
build = {
   type = "builtin",
   modules = {
      dilock = "lib/dilock.lua",
      ["dilock.consul"] = "lib/dilock/consul.lua",
   },
}
