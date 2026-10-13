# Go etcd quickstart

No flag acquires one resource with defaults; `--configured` acquires one using an explicit full policy; `--shared-provider` acquires two independent resources from one provider and gRPC channel. They are not acquired atomically and do not represent two connections.

Provision `/dilock/namespaces/default/meta` with the etcd native-lock profile and approved epoch `dev-1` before connecting. The SDK validates but does not create or repair the record. For a local development cluster, from the repository root:

```powershell
python -c "from tools.provision_etcd import provision_etcd; print(provision_etcd('http://127.0.0.1:2379', 'default', 'dev-1'))"
$env:DILOCK_ETCD_ENDPOINT = "127.0.0.1:2379"
$env:DILOCK_EPOCH = "dev-1"
go -C sdk/go run ../../docs/examples/go-etcd/quickstart/main.go
go -C sdk/go run ../../docs/examples/go-etcd/quickstart/main.go --configured
go -C sdk/go run ../../docs/examples/go-etcd/quickstart/main.go --shared-provider
```

Default wait is 5 s with a requested 30 s TTL. Configured mode uses 10 s/60 s and requires the epoch-scoped etcd fence. The example configures a plaintext endpoint only; it does not wire TLS credentials, client certificates, or authentication, even though the underlying etcd client may support them. Add and review that transport configuration before using protected deployments. Enforce the printed epoch/revision atomically at the protected resource; TTL expiry cannot stop stale work. Cleanup uses a bounded context independent of the work context and reports uncertainty through the observer.
