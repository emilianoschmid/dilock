# Python Redis example

Start with the minimal one-resource quickstart. It creates one provider and one lock handle, acquires with safe finite defaults, and uses `async with` for bounded cleanup:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
python docs/examples/python-redis/quickstart/main.py
```

See [quickstart README](quickstart/README.md) for prerequisites, configuration, the `--configured` policy mode, and `--shared-provider` two-resource mode. Two resources reuse a provider/client, but acquisitions are independent and not atomic.

For the advanced contention scenario, [ADVANCED-CONSUMER.md](ADVANCED-CONSUMER.md) and `consumer.py` demonstrate two independently owned providers, acknowledged busy, explicit release, and successor acquisition. That scenario is useful for interoperability; it is not the shortest path to using one lock.
