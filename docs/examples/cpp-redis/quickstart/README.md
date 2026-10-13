# C++ Redis quickstart

Provision the namespace metadata with an approved epoch before running this
example. Set `DILOCK_EPOCH` to that epoch and optionally set
`DILOCK_REDIS_HOST` (the default is `127.0.0.1`). Build against the locally
installed Dilock CMake package:

```sh
cmake -S docs/examples/cpp-redis/quickstart -B build/cpp-redis-quickstart \
  -DCMAKE_PREFIX_PATH=/path/to/dilock-install
cmake --build build/cpp-redis-quickstart --config Release
```

Run three modes. Without flags, one short lock uses the common defaults (five-second wait, thirty-second TTL). `--configured` holds one resource using the complete explicit policy; `--shared-provider` holds two distinct resources concurrently through the same provider and pool:

```sh
DILOCK_EPOCH=approved-epoch ./build/cpp-redis-quickstart/dilock_redis_quickstart
DILOCK_EPOCH=approved-epoch ./build/cpp-redis-quickstart/dilock_redis_quickstart --configured
DILOCK_EPOCH=approved-epoch ./build/cpp-redis-quickstart/dilock_redis_quickstart --shared-provider
```

Pass `--configured` to set transport and acquisition policy explicitly; the
example uses ten seconds and sixty seconds for provider defaults and explicit
two-second/fifteen-second acquisitions. Configure `DILOCK_NAMESPACE`,
`DILOCK_REDIS_PORT`, `DILOCK_REDIS_USERNAME`, `DILOCK_REDIS_PASSWORD`, and
`DILOCK_REDIS_DATABASE` as needed; host, connection settings, safety margin,
poll interval, default policy, and cleanup observer are set in the example.
Credentials come from the environment and are never printed. The URI/Redis
profile does not provide TLS in this C++ SDK; remote encrypted/authenticated
use must follow the backend's deployment requirements.

The shared-provider mode holds two independent resources through one provider
and its Redis connection pool. It acquires `reports/daily` before
`reports/weekly`; callers that coordinate multiple resources should use one
stable global order to avoid circular waits. This is not a multi-resource
transaction. The program explicitly checks for an acknowledged `released`
result before reporting success; RAII remains bounded best-effort fallback
cleanup. It does not renew the TTL or prove protected work cannot outlive its
lease. Redis does not supply a strong fence.
