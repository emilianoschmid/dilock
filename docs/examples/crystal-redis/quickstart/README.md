# Crystal Redis quickstart

`main.cr` is the Crystal Redis client using the SDK's `jgaskins/redis` shard. Redis coordination uses the Crystal Redis adapter, not the C++ etcd bridge. This SDK remains experimental and is not ready for a stable release.

Prerequisites: Crystal 1.21+, Shards dependencies installed for `sdk/crystal`, Redis, and pre-provisioned `billing` namespace metadata for the epoch configured in the source. Set `REDIS_URL` and update the epoch to match that metadata. The SDK validates metadata; it does not create it.

From the repository root, install dependencies and run the copied source with the SDK on Crystal's search path:

```sh
cd sdk/crystal
shards install
cd ../..
crystal run --path=sdk/crystal/src --path=sdk/crystal/lib docs/examples/crystal-redis/quickstart/main.cr
```

The scoped API waits for owner-checked release before returning. Use a disposable Redis authority.

`main.cr` demonstrates both the default `with_lease` call and an explicit 60-second TTL. The same provider creates two independent handles and executes them sequentially; this is not an atomic multi-lock operation. Redis provider connection settings are configured at provider construction; see `sdk/crystal/README.md` for the exposed options and limits.
