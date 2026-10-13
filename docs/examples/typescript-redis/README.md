# TypeScript Redis example

Start with the executable one-resource quickstart. It acquires using finite defaults and awaits release in `finally`:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
npm --prefix docs/examples/typescript-redis/quickstart start
```

See [quickstart README](quickstart/README.md) for installing the local package, `--configured` full policy, and `--shared-provider` two-resource mode. Shared provider reuses the client, but the acquisitions remain independent and are not atomic.

For an advanced compatibility scenario, see [ADVANCED-CONSUMER.md](ADVANCED-CONSUMER.md) and `consumer.ts`: two independent providers demonstrate acknowledged contention, release, and successor acquisition. It is deliberately longer than the single-resource quickstart.
