# TypeScript etcd example

The shortest example is the one-resource quickstart with finite defaults:

```powershell
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "operator-approved-cluster-incarnation"
npm --prefix docs/examples/typescript-etcd run quickstart
```

See [quickstart README](quickstart/README.md) for local package installation, the explicit `--configured` policy, the two-resource `--shared-provider` path, cancellation and security limits. Reusing one provider/channel does not make distinct etcd lock acquisitions atomic.

The longer [ADVANCED-CONSUMER.md](ADVANCED-CONSUMER.md) and `consumer.ts` show two independently owned providers, sequential acquisition, and epoch-scoped create-revision fences. That example is for compatibility exploration rather than basic usage.
