# Haskell etcd C ABI quickstart

This is an **experimental Haskell facade over Dilock C ABI v1 and the shared
C++ etcd engine**, not an independent Haskell gRPC implementation. Build and
install the Dilock C ABI with etcd enabled first. The consuming process must
be able to load `libdilock_c` and its C++/gRPC dependency closure at runtime.

Namespace metadata must already be provisioned with the configured epoch.
The SDK verifies the metadata but does not create or modify it. Automatic
keepalive is enabled for the scoped work; the `create_revision` fence must be
checked atomically by the protected resource before a write. Lease expiry does
not stop work that continues running. The quickstart exits unsuccessfully
unless release is acknowledged as `Released`: `AlreadyNotOwned` means this
lease no longer owns the key, while `ReleaseUnknown` means the acknowledgement
could not be established.

From `sdk/haskell`, with ABI headers/libraries installed under `/usr/local`:

```sh
DILOCK_ETCD_ENDPOINT=127.0.0.1:2379 \
DILOCK_NAMESPACE=reports \
DILOCK_COORDINATION_EPOCH=prod-east-1 \
LD_LIBRARY_PATH=/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} \
cabal run dilock-etcd-cabi-quickstart --flags=+etcd-c-abi \
  --extra-include-dirs=/usr/local/include \
  --extra-lib-dirs=/usr/local/lib
```

Defaults are endpoint `127.0.0.1:2379`, namespace `reports`, and epoch
`local-etcd-epoch`. Set values matching the pre-provisioned deployment.
