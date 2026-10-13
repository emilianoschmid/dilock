#!/bin/sh
set -eu

mkdir -p /tmp/consumer
cd /src
luarocks --lua-version=5.1 make dilock-0.1.0-1.rockspec
mkdir -p /tmp/consumer /tmp/artifact-tree
cd /tmp/consumer
luarocks --lua-version=5.1 pack dilock 0.1.0-1
unzip -Z1 /tmp/consumer/dilock-0.1.0-1.all.rock | grep -Fx 'doc/LICENSE' >/dev/null
artifact_license_hash=$(unzip -p /tmp/consumer/dilock-0.1.0-1.all.rock doc/LICENSE | sha256sum | cut -d' ' -f1)
[ "$artifact_license_hash" = cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30 ]
luarocks --lua-version=5.1 --tree=/tmp/artifact-tree install --deps-mode=none /tmp/consumer/dilock-0.1.0-1.all.rock
eval "$(luarocks --lua-version=5.1 --tree=/tmp/artifact-tree path)"
export DILOCK_VECTOR_PATH=/spec/vectors/identity.json

lua5.1 -e 'local p=require("dilock"); local s=debug.getinfo(p.new, "S").source; assert(not s:find("/src/", 1, true)); assert(s:find("/tmp/artifact-tree/", 1, true)); print("packed artifact consumer module: " .. s)'
lua5.1 /src/test/unit.lua
lua5.1 /src/test/failure_regression.lua
lua5.1 /src/test/error_return_regression.lua
REDIS_HOST=${REDIS_HOST:-redis} lua5.1 /src/test/redis_integration.lua
luajit -e 'local p=require("dilock"); local s=debug.getinfo(p.new, "S").source; assert(not s:find("/src/", 1, true)); assert(s:find("/tmp/artifact-tree/", 1, true)); print("LuaJIT packed artifact consumer module: " .. s)'
luajit /src/test/unit.lua
luajit /src/test/failure_regression.lua
luajit /src/test/error_return_regression.lua
REDIS_HOST=${REDIS_HOST:-redis} luajit /src/test/redis_integration.lua

# Build a source rock from a generated local archive. This uses no remote
# source URL; the temporary file:// URL exists only during local packing.
/src/test/create-source-rock.sh /tmp/source-stage/dilock-0.1.0-1.src.rock
test -f /tmp/source-stage/dilock-0.1.0-1.src.rock
unzip -p /tmp/source-stage/dilock-0.1.0-1.src.rock dilock-0.1.0-1.tar.gz > /tmp/source-stage/dilock-0.1.0-1.tar.gz
tar -xOf /tmp/source-stage/dilock-0.1.0-1.tar.gz dilock-0.1.0-1/LICENSE | sha256sum | grep -Fx 'cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  -' >/dev/null
mkdir -p /tmp/source-artifact-tree
luarocks --lua-version=5.1 --tree=/tmp/source-artifact-tree install --deps-mode=none /tmp/source-stage/dilock-0.1.0-1.src.rock
source_module_hash=$(sha256sum /tmp/source-artifact-tree/share/lua/5.1/dilock.lua | cut -d' ' -f1)
checkout_module_hash=$(sha256sum /src/lib/dilock.lua | cut -d' ' -f1)
[ "$source_module_hash" = "$checkout_module_hash" ]
eval "$(luarocks --lua-version=5.1 --tree=/tmp/source-artifact-tree path)"
export DILOCK_VECTOR_PATH=/spec/vectors/identity.json
lua5.1 -e 'local p=require("dilock"); local s=debug.getinfo(p.new, "S").source; assert(not s:find("/src/", 1, true)); assert(s:find("/tmp/source-artifact-tree/", 1, true)); print("source rock consumer module: " .. s)'
lua5.1 /src/test/unit.lua
lua5.1 /src/test/failure_regression.lua
lua5.1 /src/test/error_return_regression.lua
REDIS_HOST=${REDIS_HOST:-redis} lua5.1 /src/test/redis_integration.lua
luajit -e 'local p=require("dilock"); local s=debug.getinfo(p.new, "S").source; assert(not s:find("/src/", 1, true)); assert(s:find("/tmp/source-artifact-tree/", 1, true)); print("LuaJIT source rock consumer module: " .. s)'
luajit /src/test/unit.lua
luajit /src/test/failure_regression.lua
luajit /src/test/error_return_regression.lua
REDIS_HOST=${REDIS_HOST:-redis} luajit /src/test/redis_integration.lua

printf '\n--- Runtime versions ---\n'
lua5.1 -v
luajit -v
luarocks --version
dpkg-query -W -f='${Package} ${Version}\n' lua5.1 luajit luarocks libssl3
printf '\n--- Package source hash ---\n'
sha256sum /tmp/artifact-tree/share/lua/5.1/dilock.lua
printf '\n--- Packed artifact hash and packaged docs ---\n'
sha256sum /tmp/consumer/dilock-0.1.0-1.all.rock
sha256sum /tmp/source-stage/dilock-0.1.0-1.src.rock
unzip -l /tmp/consumer/dilock-0.1.0-1.all.rock
unzip -l /tmp/source-stage/dilock-0.1.0-1.src.rock
printf '\n--- Dependency source hashes ---\n'
find /usr/local/share/lua/5.1 -maxdepth 2 -type f \( -name redis.lua -o -name socket.lua -o -name dkjson.lua -o -name system.lua \) -print0 | sort -z | xargs -0 sha256sum
find /usr/local/lib/lua/5.1 -maxdepth 3 -type f \( -name '*openssl*.so' -o -name 'core.so' \) -print0 | sort -z | xargs -0 sha256sum
printf '\n--- Dependency versions ---\n'
luarocks --lua-version=5.1 list
