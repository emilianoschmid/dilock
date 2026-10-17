# Python Consul quickstart

This is the first Consul adapter slice. It exercises Consul's HTTP KV session
lock profile from Python; the other language routes remain planned. Use a
disposable Consul agent for this example. The included development service is
in-memory, loopback-only, and does not validate quorum or failover:

```powershell
docker compose -f infra/consul.compose.yml up -d
python -m pip install -e "sdk/python[consul]"
$env:DILOCK_CONSUL_ENDPOINT = "http://127.0.0.1:28500"
$env:DILOCK_NAMESPACE = "quickstart"
$env:DILOCK_EPOCH = "local-consul-dev-1"
python tools/provision_consul.py --endpoint $env:DILOCK_CONSUL_ENDPOINT --namespace $env:DILOCK_NAMESPACE --epoch $env:DILOCK_EPOCH
python docs/examples/python-consul/quickstart/main.py
docker compose -f infra/consul.compose.yml down
```

For an ACL protected agent, set `CONSUL_HTTP_TOKEN` in the process environment.
The token is sent in `X-Consul-Token`; do not place it in the endpoint URL.
Allow session create/renew/destroy and KV read/acquire/release for the Dilock
prefix. Never grant the application an operation that deletes lock entries if
you rely on `LockIndex` as a fence.

Consul requires a session TTL of at least 10 seconds. The SDK rounds a requested
duration upward to a whole second and returns the effective lease duration.
Fences are `(approved epoch, LockIndex)` per resource. Consul KV lock entries
must be retained because deleting and recreating one can reset its `LockIndex`.
This HTTP API is advisory; the protected resource must atomically enforce any
fence before accepting writes. See [the backend profile](../../../../spec/backends/CONSUL.md)
and [ADR 0010](../../../../docs/adr/0010-consul-http-kv-session.md).
