# Nim Redis quickstart

`main.nim` uses the Nim binding over Dilock C ABI. The distributed lock protocol and Redis engine are implemented by the existing C++ backend; Nim is a typed facade, not a separate Redis implementation. Experimental/internal candidate; no stable Nimble release is implied.

Prerequisites: Nim 2.2+, shared C ABI with Redis, Redis, and pre-provisioned `billing` metadata matching `local-redis-epoch`. Set `DILOCK_REDIS_HOST` and `DILOCK_EPOCH` as needed. Provision metadata before connecting.

Build/install the C ABI as described in `sdk/c/README.md`, then compile this consumer from the repository root (adjust the prefix for your install):

```sh
nim c --path:sdk/nim/src --passC:-I.local/dilock-c/include --passL:-L.local/dilock-c/lib --passL:-ldilock_c -o:build/nim-redis docs/examples/nim-redis/quickstart/main.nim
```

The runtime loader must find `libdilock_c` and native dependencies. Running the binary requires Redis and matching metadata; use a disposable namespace.

For explicit per-attempt timing, keep one provider and create separate handles; those acquisitions remain independent and sequential:

```nim
let provider = connectRedis("redis.internal", "billing", "prod-east-1", port = 6380)
defer: provider.destroy()
let invoice = provider.lock("invoice/42")
defer: invoice.destroy()
let report = provider.lock("reports/daily")
defer: report.destroy()
withLease lease, invoice.acquire(10_000, 60_000):
  let firstOutcome = lease.release()
  if firstOutcome != ReleaseOutcome.released: raise newException(IOError, "invoice release was not confirmed")
withLease lease, report.acquireDefault():
  let secondOutcome = lease.release()
  if secondOutcome != ReleaseOutcome.released: raise newException(IOError, "report release was not confirmed")
```

The connection, credentials and timing defaults exposed by Nim are limited to those of the C ABI facade; consult `sdk/nim/README.md` for its full options.
