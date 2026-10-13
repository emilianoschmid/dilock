# Haskell Redis quickstart

This executable uses the default 5-second acquisition wait and 30-second lease.
It borrows a `hedis` connection owned by the application and closes it with
`bracket`. Do not share that connection concurrently unless the `hedis`
connection's concurrency contract allows it.

The namespace metadata must already be provisioned for the configured
namespace and coordination epoch. Dilock validates it and never creates or
changes it. The lease is distributed coordination, not a local mutex; expiry
does not stop work that continues running. The quickstart exits unsuccessfully
unless release is acknowledged as `Released`: `AlreadyNotOwned` means this
lease did not remove a currently owned key, while `ReleaseUnknown` means the
acknowledgement could not be established. Use the fence at the protected
resource when it supports fencing.

Run from `sdk/haskell` after provisioning matching Redis metadata:

```sh
DILOCK_REDIS_HOST=127.0.0.1 \
DILOCK_NAMESPACE=reports \
DILOCK_COORDINATION_EPOCH=prod-east-1 \
cabal run dilock-redis-quickstart
```

The endpoint defaults to `127.0.0.1`; the example defaults to namespace
`reports` and epoch `local-redis-epoch`. Override all three for a deployment.
