# `@dilock/etcd`

etcd native-lock backend for Dilock. Installing this package installs grpc-js, proto-loader and `@dilock/core`; it does not install Redis or node-redis.

Connect one provider to one etcd authority with pre-provisioned namespace metadata and a stable operator-approved epoch. The provider owns its gRPC clients and must be closed. Each acquisition uses its own server lease and queues with native etcd `Lock`; `tryAcquire` is unsupported because the native service cannot safely distinguish contention from backend delay. Use `autoRenew` to keep the lease renewed after acquisition. Cancellation and unknown RPC outcomes are errors, never ordinary contention.

`withLease` awaits release before returning. A release failure rejects successful callback work; if the callback already failed, that error remains primary and the provider cleanup observer reports the unknown release.
The callback is validated before acquisition; a non-function argument raises `InvalidArgumentError` without backend I/O.
`cleanupTimeoutMs` is one total monotonic budget for the sequential native `Unlock` and lease `Revoke` calls. If `Unlock` uses the full budget, Dilock does not start another cleanup RPC; an acknowledged `Unlock` is sufficient to confirm that the lock key was removed. Repeated release calls return the cached acknowledged outcome without replaying cleanup RPCs.

An `https://` endpoint uses gRPC default root trust. Custom roots and client certificates are not configurable in this profile. The returned fence is `(epoch, create_revision)` and must be atomically enforced by the protected resource. Lease expiry cannot stop stale application work.

The package name and scope are local candidates pending ownership verification. This artifact is private and has not been published.
