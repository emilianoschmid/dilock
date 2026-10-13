# C++ Redis consumer

This standalone consumer uses the installed public `Dilock::redis` CMake target and sends real Redis commands. It demonstrates contention between independent provider connection pools, acknowledged release, and successor acquisition. It does not use a mock backend or create/repair namespace metadata.

## Prerequisites

- CMake 3.24 or newer and a C++20 compiler.
- A locally built and staged Dilock C++ Redis package. From the repository root, build and install the public package using the dependencies described in [`sdk/cpp/README.md`](../../../sdk/cpp/README.md), or use an existing Redis-only stage such as `.tools/cpp-stage`.
- A Redis authority reachable from this machine, with a namespace already provisioned for profile `redis-single-authority-v1`, identity scheme `sha256-framed-utf8-v1`, the chosen `coordination_epoch`, and `fixed_lease_ms: null`. Provisioning is a deployment operation and is intentionally not part of this consumer.

Configure and build from the repository root in the same environment as the staged package. For the checked local Linux stage under WSL, include its dependency prefixes so CMake can resolve OpenSSL, nlohmann-json, and redis-plus-plus:

```sh
cd /mnt/c/codex/dilock
cmake -S docs/examples/cpp-redis -B .tools/cpp-redis-consumer-ninja -G Ninja \
  -DCMAKE_MAKE_PROGRAM=/usr/bin/ninja -DCMAKE_CXX_COMPILER=/usr/bin/c++ \
  -DOPENSSL_INCLUDE_DIR=/mnt/c/codex/dilock/.tools/openssl/usr/include \
  -DOPENSSL_CRYPTO_LIBRARY=/mnt/c/codex/dilock/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu/libcrypto.so.3 \
  -DCMAKE_PREFIX_PATH="/mnt/c/codex/dilock/.tools/cpp-stage;/mnt/c/codex/dilock/.tools/cppinstall;/mnt/c/codex/dilock/.tools/cppdeps/usr"
cmake --build .tools/cpp-redis-consumer-ninja --config Release
```

Set the already provisioned namespace identity and connect to your local Redis, then run the compiled consumer in that Linux environment:

```sh
export DILOCK_REDIS_HOST=127.0.0.1
export DILOCK_REDIS_PORT=6379
export DILOCK_NAMESPACE=example
export DILOCK_EPOCH=deployment-2026-01
# Set these only when the local Redis requires authentication. Do not put credentials in source control.
# export DILOCK_REDIS_USERNAME=...
# export DILOCK_REDIS_PASSWORD=...
./.tools/cpp-redis-consumer-ninja/dilock_cpp_redis_consumer
```

The expected output is:

```text
contender: acknowledged busy
contender: acquired after owner release
```

The example creates a unique resource name per run. It explicitly releases both leases and checks Redis acknowledgements; destructors remain bounded best-effort cleanup if an exception interrupts the flow.

`RedisProvider` validates existing namespace metadata while constructing its Redis client. Invalid/missing metadata, backend errors, and uncertain command outcomes are reported as errors, not converted to contention. Redis TTL provides eventual expiry after a process crash, but it cannot stop stale application work. This single-authority profile does not provide strong fencing or failover safety. See [`sdk/cpp/README.md`](../../../sdk/cpp/README.md) for lease lifetime, synchronous cancellation limits, thread safety, and error semantics.
