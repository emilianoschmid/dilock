# Python Redis quickstart

Install the local SDK and provision matching namespace metadata before the first run. For local development, from the repository root:

```powershell
python -m pip install -e "sdk/python[redis]"
python tools/provision.py --endpoint redis://127.0.0.1:6379 --namespace default --epoch dev-1
$env:DILOCK_EPOCH = "dev-1"
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
python docs/examples/python-redis/quickstart/main.py
python docs/examples/python-redis/quickstart/main.py --configured
python docs/examples/python-redis/quickstart/main.py --shared-provider
```

No flags acquires one resource with finite defaults (5 s wait, 30 s lease); `--configured` acquires one with the full 10 s/60 s, 2 s RPC/cleanup policy. `--shared-provider` holds two independent resources via one provider and pool; it is not an atomic multi-lock operation.

The SDK does not create or repair metadata. `async with` awaits bounded cleanup; an uncertain release remains observable. Lease expiration cannot stop stale application work, so keep the operation bounded or use renewal and observe lease loss. This sample URI is plaintext; use the Redis URI/client TLS and authentication settings approved for your environment, and do not commit credentials.
