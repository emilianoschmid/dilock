#!/bin/sh
set -eu
cd /mnt/c/codex/dilock
mkdir -p .tools .tmp
version=8.10.2
if [ ! -f ".tools/redis-$version.tar.gz" ]; then
  curl --fail --location "https://download.redis.io/releases/redis-$version.tar.gz" -o ".tools/redis-$version.tar.gz"
fi
sha256sum ".tools/redis-$version.tar.gz"
if [ ! -d ".tools/redis-$version" ]; then tar -xzf ".tools/redis-$version.tar.gz" -C .tools; fi
PATH="/mnt/c/codex/dilock/.tools/make/usr/bin:$PATH" make -C ".tools/redis-$version" -j4 BUILD_TLS=no MALLOC=libc
# No background daemon, persistence, external bind, or existing service changes.
exec ".tools/redis-$version/src/redis-server" --bind 127.0.0.1 --port 16379 --save '' --appendonly no --maxmemory-policy noeviction --protected-mode yes
