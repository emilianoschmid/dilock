# Python etcd quickstart

Install the local SDK and have an administrator provision `/dilock/namespaces/default/meta` with the etcd-native profile and approved coordination epoch. For local development only, the repo helper can provision the sample identity:

```powershell
python -m pip install -e "sdk/python[etcd]"
python -c "from tools.provision_etcd import provision_etcd; print(provision_etcd('http://127.0.0.1:2379', 'default', 'dev-1'))"
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_EPOCH = "dev-1"
python docs/examples/python-etcd/quickstart/main.py
python docs/examples/python-etcd/quickstart/main.py --configured
python docs/examples/python-etcd/quickstart/main.py --shared-provider
```

No flags acquires one resource using 5 s wait and 30 s lease defaults. `--configured` uses a full 10 s/60 s policy with 2 s RPC/cleanup budgets and requires etcd fencing. `--shared-provider` holds two different resources through one channel; these leases are independent and the acquisition is not atomic. The SDK validates metadata but does not create or repair it.

Both modes use `async with` and bounded cleanup. The etcd fence is epoch plus `create_revision`; a protected resource must atomically enforce it to reject stale writers. Lease expiry cannot stop paused work. The example endpoint is plaintext HTTP; the Python SDK supports HTTPS using the etcd client's normal TLS trust configuration, but this quickstart does not demonstrate custom roots, client certificates, or username/password auth. Configure and verify those transport controls separately before production.
