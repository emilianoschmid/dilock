# Python Redis consumer

This executable example uses the public `dilock` Python API and two independently owned Redis providers. It demonstrates acknowledged contention, explicit release, and a successor acquisition. It sends real Redis commands; it does not emulate a distributed lock.

## Prerequisites

- Python 3.11 or newer.
- A Redis authority reachable from this machine.
- A namespace provisioned by an operator with profile `redis-single-authority-v1`, identity scheme `sha256-framed-utf8-v1`, the chosen `coordination_epoch`, and `fixed_lease_ms: null`. Runtime SDK calls never create or repair metadata.

From the repository root, install the checkout into an isolated environment:

```powershell
py -3 -m venv .tools/python-api-example
.tools/python-api-example/Scripts/python.exe -m pip install -e "sdk/python[redis]"
```

Set the endpoint and the already provisioned namespace identity, then run:

```powershell
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "deployment-2026-01"
.tools/python-api-example/Scripts/python.exe docs/examples/python-redis/consumer.py
```

Expected output:

```text
contender: acknowledged busy
contender: acquired after owner release
```

The resource name is unique per run. Credentials may be included in `DILOCK_REDIS_URL`; the SDK omits them from its errors. Do not commit credentials to source control or provision a namespace implicitly from this example.

Both providers own their Redis clients and are closed by `async with`. Each lease is an acquisition with its own owner token; explicit release is awaited so the acknowledgement is checked. A process crash falls back to Redis TTL. This example does not establish failover safety or strong fencing; Redis single-authority does not provide strong fencing, and a lease cannot stop stale application work.
