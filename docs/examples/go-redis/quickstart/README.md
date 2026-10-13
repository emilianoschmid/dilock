# Go Redis quickstart

The no-flag command acquires one resource using defaults. `--configured` acquires one resource using a complete explicit policy. `--shared-provider` holds two distinct resources via the same provider/client. The latter demonstrates reuse only: the leases are independent and acquisition is not atomic.

Provision matching namespace metadata administratively before running; the SDK never creates or repairs it. For local development, from the repository root:

```powershell
python tools/provision.py --endpoint redis://127.0.0.1:6379 --namespace default --epoch dev-1
$env:DILOCK_EPOCH = "dev-1"
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379"
go -C sdk/go run ../../docs/examples/go-redis/quickstart/main.go
go -C sdk/go run ../../docs/examples/go-redis/quickstart/main.go --configured
go -C sdk/go run ../../docs/examples/go-redis/quickstart/main.go --shared-provider
```

Defaults bound the wait to 5 s and request a 30 s lease; configured mode uses 10 s/60 s and explicit 2 s RPC/cleanup budgets. Each `WithLease` callback runs once, with cleanup on an independent bounded context. TTL cleanup does not stop expired/stale application work; Redis single-authority does not provide strong fencing across unsafe failover. The example uses a URI accepted by its Redis client; production TLS and credentials must be configured in that URI/client configuration and protected as secrets.
