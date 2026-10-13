# Python etcd public API consumer

This asyncio consumer uses two separately owned `EtcdProvider` channels. With one unique resource it observes `WaitTimeout` from a bounded waiter while the owner is held, explicitly releases the owner, then checks that the successor's verified `create_revision` is larger. Network calls and cleanup are asynchronous; providers and leases are closed/released explicitly, including exceptional paths.

The app requires a pre-provisioned `/dilock/namespaces/<namespace>/meta` record with `protocol_major: 1`, `identity_scheme: "sha256-framed-utf8-v1"`, `profile: "etcd-native-lock-v1"`, the approved `coordination_epoch`, and `fixed_lease_ms: null`. It never creates metadata. Treat `(epoch, create_revision)` as a fencing token only when the protected resource atomically registers and checks it. A lease or local `Lost` signal cannot stop an already-running stale process.

In this checkout, validate imports and syntax with the already installed local SDK source, without PyPI access:

```powershell
$env:PYTHONPATH = (Resolve-Path sdk/python/src).Path
python -m compileall -q docs/examples/python-etcd/consumer.py
python docs/examples/python-etcd/consumer.py
```

The last command requires `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and
`DILOCK_EPOCH`, plus a reachable etcd authority with matching pre-provisioned
metadata. It reports connection or protocol failures directly; it does not
create or repair namespace metadata.
