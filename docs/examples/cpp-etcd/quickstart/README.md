# C++ etcd quickstart

Provision namespace metadata with an approved epoch before running this
example. Set `DILOCK_EPOCH` and optionally `DILOCK_ETCD_ENDPOINT` (default
`127.0.0.1:2379`). Build against the locally installed optional etcd package:

```sh
cmake -S docs/examples/cpp-etcd/quickstart -B build/cpp-etcd-quickstart \
  -DCMAKE_PREFIX_PATH=/path/to/dilock-etcd-install
cmake --build build/cpp-etcd-quickstart --config Release
```

Run three modes. Without flags, one short lock uses the common defaults (five-second wait, thirty-second lease). `--configured` holds one resource using the complete explicit policy; `--shared-provider` holds two distinct resources concurrently through the same provider/channel:

```sh
DILOCK_EPOCH=approved-epoch ./build/cpp-etcd-quickstart/dilock_etcd_quickstart
DILOCK_EPOCH=approved-epoch ./build/cpp-etcd-quickstart/dilock_etcd_quickstart --configured
DILOCK_EPOCH=approved-epoch ./build/cpp-etcd-quickstart/dilock_etcd_quickstart --shared-provider
```

Pass `--configured` for explicit provider and per-acquisition policy; the
example also prints the native create revisions. Configure
`DILOCK_NAMESPACE`; the endpoint, RPC and cleanup bounds, safety margin,
default acquisition, no-auto-renew setting, insecure local credentials and
cleanup observer are shown in code. Replace insecure credentials with
deployment-managed TLS credentials when using remote etcd.

The shared-provider mode holds two independent resources through one provider
and gRPC channel. It acquires `reports/daily` before `reports/weekly`; callers
that coordinate multiple resources should use one stable global order to avoid
circular waits. This is not a multi-resource transaction, and etcd grants each
acquisition its own server lease. The program checks for acknowledged
`released` outcomes before reporting success; RAII remains bounded best-effort
fallback cleanup. This does not stop a paused process or protect downstream
writes. Applications must atomically enforce the returned epoch and revision
at their protected resource.
