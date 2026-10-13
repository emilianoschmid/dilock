# Dart conformance worker

This internal JSONL process adapter is for cross-SDK conformance tests. It is
not included in the public Dart package artifacts and does not provision or
modify backend namespace metadata.

Run it with one JSON configuration file containing `backend` (`redis` or
`etcd`), `endpoint`, `namespace`, and `epoch`. The optional `profile` must
match `redis-single-authority-v1` or `etcd-native-lock-v1`. Optional bounded
integer defaults are `wait_ms` (5000), `lease_ms` (30000), `rpc_ms` (2000),
and `cleanup_ms` (2000). The worker validates backend metadata through the
provider before reading requests. Do not place credentials in request lines
or stdout; endpoint credentials are never echoed.

```sh
dart run bin/dilock_worker.dart config.json
```

The worker supports acquire, try-acquire, renew, release, local inspect,
request cancellation, hello, and bounded shutdown for both Dart adapters.
Both adapters can manually and automatically renew; etcd additionally reports
its verified fencing epoch and revision. Renewal uses the immutable lease
duration captured at acquisition. A `renew` request that asks for a different
duration returns `unsupported_capability` rather than silently changing the
lease policy. Redis does not report a strong fence. Backend errors and
uncertain acquisition outcomes remain distinct from ordinary contention.

Input is limited to 64 KiB per line, 64 active requests, and 64 held leases.
Malformed input is reported only on stderr and receives no response because a
safe request ID may not be available. Shutdown cancels pending acquisitions,
waits at most two seconds for them to settle, attempts owner-specific release
with the SDK cleanup budget, and reports outstanding cleanup attempts.
