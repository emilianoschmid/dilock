# Haskell Redis quickstart

`main.hs` uses the Haskell Redis adapter with `hedis`; `bracket` closes the application-owned Redis connection. Redis coordination is implemented by this SDK path, not by the C++ etcd bridge. The SDK is experimental; no stable package is implied.

Prerequisites: GHC/Cabal, dependencies in `sdk/haskell/dilock.cabal`, Redis, and pre-provisioned metadata for `reports` / `local-redis-epoch`. Set `DILOCK_REDIS_HOST`, `DILOCK_NAMESPACE`, and `DILOCK_COORDINATION_EPOCH` to match. Dilock validates but never creates metadata.

From the repository root, run the packaged quickstart (canonical source: `sdk/haskell/examples/redis/Main.hs`; this folder contains the same consumer example):

```sh
cd sdk/haskell
DILOCK_REDIS_HOST=127.0.0.1 DILOCK_NAMESPACE=reports DILOCK_COORDINATION_EPOCH=local-redis-epoch cabal run dilock-redis-quickstart
```

Use a disposable namespace and check the release outcome before treating work as complete.

The sample is the default 5-second wait / 30-second lease path. For configured acquisition, use `AcquireOptions`; the same provider and borrowed connection can create several handles, but operate on them sequentially unless the `hedis` connection's concurrency contract permits parallel use. Independent locks are not atomic as a group:

```haskell
invoice <- lock redisProvider "invoice/42"
daily <- lock redisProvider "reports/daily"
(_first, firstOutcome) <- withLease invoice
  (AcquireOptions { waitMilliseconds = 10000, leaseMilliseconds = 60000 })
  observeCleanup (\_lease -> pure ())
(_second, secondOutcome) <- withLease daily defaultAcquireOptions
  observeCleanup (\_lease -> pure ())
```

The API intentionally borrows the application's Redis connection; it does not add provider-level transport settings. See `sdk/haskell/README.md` for lifecycle and concurrency constraints.
