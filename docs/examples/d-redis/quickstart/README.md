# D Redis quickstart

`main.d` demonstrates the D facade and `scope(exit)` handle cleanup. Distributed coordination is performed by the existing C++ Redis engine through Dilock C ABI; the D package is an experimental/internal binding, not a stable DUB release.

Prerequisites: DMD or compatible D toolchain, shared C ABI with Redis, Redis, and pre-provisioned `billing` metadata matching `local-redis-epoch`. Set `DILOCK_REDIS_HOST` and `DILOCK_EPOCH` to match. Provider creation validates metadata but does not provision it.

After building/installing the C ABI as described in `sdk/c/README.md`, compile from the repository root (adjust paths for your install):

```sh
dmd -Isdk/d/source -L-L.local/dilock-c/lib -L-ldilock_c -of=build/d-redis docs/examples/d-redis/quickstart/main.d
```

The runtime loader must locate the C ABI and transitive C++ libraries. Run against the provisioned Redis test namespace.

For full Redis connection and timing configuration, use `RedisProviderOptions` and `RedisProvider.connect(options)`; the type exposes endpoint, credentials, database and provider defaults. A provider can make multiple handles, for example `provider.lock("invoice/42")` and `provider.lock("reports/daily")`; acquire/release each one sequentially because separate handles are not an atomic group.
