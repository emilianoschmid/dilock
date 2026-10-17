# Haskell Consul C ABI example

This optional example uses the shared Dilock C ABI v1 Consul engine. Build and
install a Consul-enabled native library first, then run it with:

```sh
cabal run dilock-consul-cabi-quickstart --flags=+consul-c-abi \
  --extra-include-dirs=/usr/local/include --extra-lib-dirs=/usr/local/lib
```

The example calls the explicit metadata provisioning operation. In deployed
systems, perform that operation through an authorized setup path, then keep
ordinary provider construction read-only. Retain every lock KV key so its
`LockIndex` remains monotone. The C ABI dependency and its C++ runtime libraries
must be installed at both build time and runtime.
