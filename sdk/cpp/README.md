# Dilock C++ SDK

The C++ SDK provides synchronous Redis leases using the draft 1 identity and ownership protocol. It targets C++20 for `std::stop_token`; Redis commands use the maintained [`redis-plus-plus`](https://github.com/sewenew/redis-plus-plus) client. This Redis profile assumes one authoritative Redis keyspace without lock-key eviction, manual deletion, or state-losing failover. It does not provide a strong fencing token. Strict mutual exclusion assumes one writable authority that preserves lock state; asynchronous replica promotion can lose a held key and is unsupported for this guarantee. Use resource-side fencing enforced atomically when failover safety is required.

An optional etcd profile uses etcd's native v3 gRPC Lock/Unlock and lease APIs. It is built only when `DILOCK_BUILD_ETCD=ON`; that build also requires Protobuf 3.21+, gRPC C++ 1.51+, and their CMake packages. The profile identifier is `etcd-native-lock-v1`. `try_acquire` throws `unsupported_capability`: etcd has no native try-lock RPC, and a deadline is not evidence of contention. TTLs are rounded up to whole seconds by etcd, so `effective_ttl()` reports the server-granted value. Each attempt owns a unique lease. Keepalive runs while queued; after acquisition it stops by default and continues only when `EtcdOptions::auto_renew` is true. The returned fence contains the epoch and a verified `create_revision`; applications must atomically register and enforce that fence at the protected resource.

Protobuf describes etcd's official gRPC messages; it is not a Dilock protocol or a service installed beside etcd. When building the optional profile from source, CMake runs `protoc` and the gRPC C++ plugin over the pinned etcd definitions, then compiles those generated stubs into Dilock. An installed-package consumer needs the normal Protobuf/gRPC link dependencies, and a dynamically linked application must deploy their runtime libraries. The etcd server needs no Dilock, Protobuf, or Lua installation. The default Redis-only profile does not use Protobuf or gRPC.

Release first checks the exact opaque key and its lease, then sends `Unlock` for that key and revokes the acquisition's unique lease, within one cleanup deadline. It returns `released` only when the check confirms this acquisition owns the key and both `Unlock` and `LeaseRevoke` are positively acknowledged. An acknowledged revoke without both proofs returns `already_not_owned`, because the unique lease confirms this attempt can no longer own a lock; an etcd `NOT_FOUND` revoke or confirmed key absence also returns `already_not_owned`. Unresolved ownership or cleanup returns `unknown`. Repeated calls return the cached terminal outcome.

## Build

Install CMake, OpenSSL 3, nlohmann-json 3.11 or later, and redis-plus-plus 1.3.15 (with its hiredis dependency). Then configure and build:

```sh
cmake -S sdk/cpp -B build/cpp -DCMAKE_PREFIX_PATH=/path/to/dependencies
cmake --build build/cpp --config Release
ctest --test-dir build/cpp -C Release --output-on-failure
cmake --install build/cpp --prefix /path/to/stage
```

Build the Redis-only local package with `conan create sdk/cpp --build=missing`. Conan keeps the etcd requirements out of this default profile. To include etcd, set `-o "&:with_etcd=True"`; that profile adds Protobuf 3.21.12 and gRPC 1.54.3 and exports both `Dilock::redis` and `Dilock::etcd`. The recipe pins redis-plus-plus 1.3.15, OpenSSL 3.5.8, and nlohmann-json 3.12.0. Nothing is published automatically.

## Example

```cpp
#include <dilock/redis.hpp>

#include <iostream>
#include <stdexcept>
#include <string>

int main(int argc, char** argv) {
    if (argc != 2 || argv[1][0] == '\0') {
        std::cerr << "Pass the deployment-approved namespace epoch.\n";
        return 2;
    }
    dilock::RedisOptions options;
    options.coordination_epoch = std::string(argv[1]);

    dilock::RedisProvider provider(options); // Reads and validates provisioned metadata.
    auto monthly_close = provider.lock("monthly-close");
    {
        auto lease = monthly_close.acquire(); // Five-second wait, thirty-second TTL.
        std::cout << "Run a short operation while lease.valid() remains true.\n";
        if (lease.release() != dilock::ReleaseResult::released) {
            throw std::runtime_error("Redis release was not acknowledged as released");
        }
    }
    // RAII remains a bounded, owner-checked best-effort fallback on exceptional exits.
}
```

For full transport and per-acquisition configuration, explicit release
acknowledgement, and two resource handles sharing one provider, see the
[Redis](../../docs/examples/cpp-redis/quickstart/README.md) and
[etcd](../../docs/examples/cpp-etcd/quickstart/README.md) quickstarts. The two
handles share a client/channel; creating two providers is only needed when an
example intentionally exercises independent-client contention or handoff.

Create the namespace metadata once during deployment with `dilock::provision_redis_namespace`. It uses create-if-absent; an existing mismatch is an error. Application code must not silently create or repair metadata.

For etcd, provision `/dilock/namespaces/<namespace>/meta` before constructing `EtcdProvider`, with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. The SDK validates but never creates or repairs this metadata. See [`etcd.hpp`](include/dilock/etcd.hpp) for cancellation, timeout, lifetime and thread-safety details and [the etcd profile contract](../../spec/backends/ETCD.md) for fencing and recovery assumptions.

## Lifetime and errors

`RedisProvider` owns a shared Redis client and is safe to use from multiple threads; returned resource handles share that provider. A `Lease` is move-only and serializes its own renewal/release operations. Its destructor makes one bounded, best-effort owner-checked release; use explicit `release()` when the acknowledgement matters. When configured, the cleanup observer receives every destructor outcome other than `released`. The destructor does not mask exceptions from the protected work.

Acquisition and renewal are synchronous. A stop request is checked before sending, between contention polls, and after each Redis round-trip. It cannot interrupt a command already inside hiredis; `rpc_timeout` bounds that wait. If cancellation arrives after Redis acknowledges acquisition, the SDK performs bounded owner-specific cleanup and reports cancellation only when cleanup is acknowledged; otherwise it reports `acquisition_outcome_unknown`. A transport exception during `SET` is `acquisition_outcome_unknown`, because the client cannot prove whether Redis applied the command. It is never returned as busy. After acknowledged contention, the SDK makes one bounded, read-only `TYPE` query: strings and expired keys remain busy, other types raise `protocol_mismatch`, and query failures raise `backend_unavailable`. `try_acquire` returns an empty optional only for Redis contention. `acquire` throws `wait_timeout` when its caller's wait budget expires and cleanup is acknowledged.

Every lease has a conservative local deadline based on the monotonic request-send time and configured safety margin. `valid()` is a local observation, not a server query. Explicit `renew()` extends only the same owner; a not-owner reply or elapsed deadline makes the lease lost. Release uses the canonical compare-and-delete Lua script. Redis errors are surfaced as typed `Error` values; release returns `unknown` if Redis did not acknowledge its result. Cleanup uses its own configured RPC timeout, not the acquisition stop token.

