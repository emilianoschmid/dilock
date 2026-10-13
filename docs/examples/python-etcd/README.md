# Python etcd example

For the common case, run the minimal one-resource quickstart. It creates one provider/handle, uses finite acquisition defaults, and releases within the async scope:

```powershell
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_NAMESPACE = "example"
$env:DILOCK_EPOCH = "operator-approved-cluster-incarnation"
python docs/examples/python-etcd/quickstart/main.py
```

See [quickstart README](quickstart/README.md) for pre-provisioned metadata, the complete `--configured` policy, the two-resource `--shared-provider` mode, and TLS/auth details. Shared provider means transport reuse; two etcd resources are independent, not atomic.

The longer [ADVANCED-CONSUMER.md](ADVANCED-CONSUMER.md) / `consumer.py` scenario uses two separately owned providers to show bounded contention, explicit release, successor acquisition, and increasing create-revision. It is a conformance-style example, not the minimal usage path.
