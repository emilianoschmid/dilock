# `@dilock/redis`

Redis single-authority backend for Dilock. Installing this package installs node-redis and `@dilock/core`; it does not install etcd or gRPC dependencies.

Import the provider and related types from `@dilock/redis`. The provider owns its Redis client and closes it when `close()` is awaited. `tryAcquire` returns `undefined` only for confirmed ordinary contention. `acquire` waits up to `waitMs`; `rpcTimeoutMs` bounds a command. Cancellation or an uncertain response after dispatch is an error and never ordinary contention. Always release in `finally` and close the provider.

`withLease` awaits release before returning. A release failure rejects successful callback work; if the callback already failed, that error remains primary and the provider cleanup observer reports the unknown release.
The callback is validated before acquisition; a non-function argument raises `InvalidArgumentError` without backend I/O.

Redis TTL provides eventual cleanup after a crash; it cannot stop stale application work. This profile does not provide strong fencing and cannot promise safety across state loss or unsafe failover. Strict mutual exclusion assumes one writable authority that preserves lock state; asynchronous replica promotion can lose a held key and is unsupported for this guarantee. Use resource-side fencing enforced atomically when failover safety is required. Namespace metadata must be provisioned explicitly before normal clients connect.

The package name and scope are local candidates pending ownership verification. This artifact is private and has not been published.
