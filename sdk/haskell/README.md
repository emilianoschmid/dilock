# Dilock for Haskell (experimental source preview)

This package is an experimental client for the shared Dilock draft 1 contract. Its Redis module borrows a `hedis` connection; the caller owns connection lifecycle and must not concurrently use one connection in ways forbidden by `hedis`. Redis `withLease` returns the work value together with its `ReleaseOutcome`; cleanup is best effort and does not stop work after lease expiry. When the work throws or is cancelled, the supplied cleanup observer receives the release outcome (`ReleaseUnknown` when cleanup itself throws), and the original work exception or cancellation is rethrown. Observer exceptions are suppressed on this exceptional path so they cannot mask the original error; keep observers prompt and non-blocking. Backend errors and uncertain writes raise typed exceptions; only Redis's null bulk reply to `SET NX` is reported as contention (the `hedis` constructor is `Bulk Nothing`). `renew` is explicit and owner-checked.

The namespace must already have compatible metadata provisioned. The default package build remains Redis-only. Optional `etcd-c-abi` and `consul-c-abi` flags add experimental Haskell facades over Dilock C ABI v1 and the shared C++ backends; these are neither standalone Haskell transport implementations nor cross-SDK conformance evidence. The previews are not production-readiness claims.

Executable quickstarts are available for the caller-owned Redis connection
([Redis example and instructions](examples/redis/README.md)) and, when the
optional C ABI is installed, the etcd facade
([etcd C ABI example and instructions](examples/etcd-c-abi/README.md)).
The optional Consul facade also has a quickstart
([Consul C ABI example and instructions](examples/consul-c-abi/README.md)).

The optional PostgreSQL facade is enabled with `postgres-c-abi`; its
[PostgreSQL C ABI quickstart](examples/postgres-c-abi/README.md) uses one
private PostgreSQL session per acquisition. Provisioning is explicit, runtime
construction only validates `dilock_namespace_meta`, and the profile provides
neither a TTL nor a fencing token. It remains experimental. The opt-in
`dilock-postgres-cabi-live` consumer uses two providers to check provisioning,
contention, liveness, release and handoff against a disposable PostgreSQL
authority when `DILOCK_POSTGRES_DSN` is set. The local validation uses the
PostgreSQL-only C ABI build, so it does not require etcd or Consul symbols;
hosted CI evidence remains pending. A successful local build is not a
production-readiness claim.

```haskell
import Control.Exception (bracket)
import Database.Redis (connect, defaultConnectInfo, disconnect)
import Dilock.Redis (ReleaseOutcome(..), defaultAcquireOptions, lock, provider, withLease)

main :: IO ()
main = bracket (connect defaultConnectInfo) disconnect $ \connection -> do
  redisProvider <- provider connection "reports" "prod-east-1"
  handle <- lock redisProvider "daily"
  (_result, releaseOutcome) <- withLease handle
    defaultAcquireOptions observeCleanup $ \_lease -> do
      -- Run short protected work here.
      pure ()
  case releaseOutcome of
    ReleaseUnknown -> putStrLn "Release outcome is unknown"
    _ -> pure ()

observeCleanup :: ReleaseOutcome -> IO ()
observeCleanup ReleaseUnknown = putStrLn "Work failed; release outcome is unknown"
observeCleanup _ = pure ()
```

On the success path, inspect the returned `ReleaseOutcome`. The observer runs only when work exits by exception or asynchronous cancellation, including when cleanup is interrupted; it receives `ReleaseUnknown` if cleanup raised an exception. An observer error never replaces the original work exception/cancellation. `withLease` does not suppress work failures.

If `release` returns `ReleaseUnknown`, the same lease keeps that terminal
outcome on later calls and does not send another release script. An ambiguous
acknowledgement must remain visible; a retry could replace it with a misleading
`AlreadyNotOwned` result.

The package uses `hedis` 0.15.2, `aeson`, `cryptonite`, and `memory`; see `dilock.cabal`. Lua scripts are packaged as data and sent by the client; infrastructure does not install a Lua service.

For a different wait or lease budget, pass an `AcquireOptions` value. Waits
must be between 1 ms and 24 hours, and leases must be between 100 ms and 24
hours for both `acquire` and `tryAcquire`. Contention polling uses bounded jitter and caps each sleep at the
remaining wait budget. A lease acknowledged only after the wait budget is
discarded with owner-checked cleanup; if that cleanup is unconfirmed, acquire
raises `AcquisitionUnknown`. The provider borrows the connection and checks
metadata; it does not configure connection deadlines, so an in-flight hedis
round trip can still make the call return later than the requested wait budget.

```haskell
import Control.Exception (bracket)
import Database.Redis (connect, defaultConnectInfo, disconnect)
import Dilock.Redis (AcquireOptions(..), ReleaseOutcome(..), lock, provider, withLease)

main :: IO ()
main = bracket (connect defaultConnectInfo) disconnect $ \connection -> do
  redisProvider <- provider connection "reports" "prod-east-1"
  handle <- lock redisProvider "daily"
  (_value, outcome) <- withLease handle
    (AcquireOptions { waitMilliseconds = 10000, leaseMilliseconds = 60000 })
    observeCleanup
    (\_lease -> pure ())
  case outcome of
    ReleaseUnknown -> putStrLn "Release outcome is unknown"
    _ -> pure ()

observeCleanup :: ReleaseOutcome -> IO ()
observeCleanup ReleaseUnknown = putStrLn "Work failed; release outcome is unknown"
observeCleanup _ = pure ()
```

The Redis source preview has no automatic renewal or transport deadline
configuration. Its connection remains caller-owned and must follow hedis's
concurrency rules. Lease expiry does not stop work.

Build and run the tests locally with `cabal test` after installing the declared dependencies.

## Optional etcd facade over the Dilock C ABI

This component is deliberately opt-in and experimental. It requires Dilock
C ABI v1 built and installed as a shared library with both C++ backend
profiles. Its enum values and struct layout are not promised stable across a
future major ABI. A Haskell consumer also needs the C++ runtime and the C ABI's
transitive Redis++, hiredis, OpenSSL, protobuf, gRPC and related native
libraries available to the dynamic loader. Cabal does not build, bundle or
download those native dependencies.

Build and install the native facade first:

```sh
cmake -S sdk/cpp -B build/dilock-haskell-etcd \
  -DBUILD_SHARED_LIBS=ON \
  -DDILOCK_BUILD_ETCD=ON \
  -DDILOCK_BUILD_CONSUL=ON \
  -DDILOCK_BUILD_C_API=ON \
  -DDILOCK_BUILD_TESTS=ON
cmake --build build/dilock-haskell-etcd
ctest --test-dir build/dilock-haskell-etcd --output-on-failure
cmake --install build/dilock-haskell-etcd --prefix /usr/local
cabal test --flags=+etcd-c-abi \
  --extra-include-dirs=/usr/local/include \
  --extra-lib-dirs=/usr/local/lib
```

The example uses ABI v1. `defaultEtcdOptions` provides endpoint and timeout
defaults; supply the deployment-approved epoch and namespace metadata out of
band. Provider construction only reads and validates metadata. TLS fields take
PEM contents, not file paths. Set `autoRenew = True` when work may outlive the
lease TTL; etcd keepalive also runs while acquisition is queued.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad (unless)
import Dilock.Etcd.CABI

main :: IO ()
main = do
  let options = (defaultEtcdOptions "prod-east-1")
        { endpoint = "127.0.0.1:2379"
        , nameSpace = "reports"
        , autoRenew = True
        }
  withEtcdProvider options $ \provider ->
    withEtcdLock provider "daily" $ \handle -> do
      (value, releaseResult) <- withEtcdLease handle
        (defaultEtcdAcquireOptions options)
        (\_outcome -> pure ())
        (\lease -> do
          (epoch, revision) <- etcdLeaseFence lease
          unless (epoch == "prod-east-1" && revision > 0) $
            fail "invalid etcd fence"
          pure "protected result")
      print (value, releaseResult)
```

Blocking native calls use `foreign import ccall safe`, so other Haskell
capabilities can run. If an async exception interrupts acquisition, the
wrapper requests the C ABI cancellation token, waits for native cancellation
and cleanup, cleans a lease that raced with cancellation, then rethrows the
original exception. `withEtcdLease` explicitly releases and destroys the
native handle on normal and exceptional work exits; inspect the returned
`EtcdReleaseOutcome` and use its observer to report cleanup on exceptions.
`EtcdError.nativeStatus` preserves distinctions such as timeout, cancellation,
backend failure, acquisition unknown, protocol mismatch and unsupported
capability; none become ordinary contention. A value returned by
`acquireEtcd` is caller-owned and must be ended with `releaseEtcdLease` or,
preferably, `withEtcdLease`.

`tryAcquireEtcd` fails immediately with the typed unsupported-capability status.
The native etcd `Lock` RPC queues waiters and cannot provide a safe one-shot
busy result; use `acquireEtcd` with a finite wait budget instead.

The C ABI facade is optional and versioned ABI v1; installing it is a native
system prerequisite at build and runtime. A successful Haskell source build or
local etcd test does not make this component standalone, portable across all
toolchains, conformant across SDKs, or production ready.

## Optional Consul facade over the Dilock C ABI

Build with `--flags=+consul-c-abi` after installing the Consul-enabled shared
Dilock C ABI and its runtime dependencies. `provisionConsulNamespace` is an
explicit setup operation; ordinary `withConsulProvider` calls only validate
metadata. The optional ACL token is passed to the native provider and is not
part of the endpoint.

The facade includes cancellable acquisition, nonblocking `tryAcquireConsul`,
explicit renewal, effective TTL, awaited release, and the `(epoch, LockIndex)`
fence. `withConsulLease` releases and destroys its native lease on normal and
exceptional exits. Call `destroyConsulLease` after observing the result when
managing an acquired lease manually. Retain lock KV keys so Consul cannot reset
their LockIndex.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad (unless)
import Dilock.Consul.CABI

main :: IO ()
main = do
  let options = (defaultConsulOptions "billing-epoch-1") { nameSpace = "billing" }
  -- Provision once through a controlled setup operation; not on each worker startup.
  provisionConsulNamespace options
  withConsulProvider options $ \provider ->
    withConsulLock provider "invoice/8391" $ \handle -> do
      (value, releaseOutcome) <- withConsulLease handle
        (defaultConsulAcquireOptions options) (\_ -> pure ()) $ \lease -> do
          (epoch, lockIndex) <- consulLeaseFence lease
          unless (epoch == coordinationEpoch options && lockIndex > 0) $
            fail "invalid Consul fence"
          pure "protected result"
      print (value, releaseOutcome)
```

The opt-in single-node live test uses `DILOCK_TEST_CONSUL_ENDPOINT`,
`DILOCK_TEST_CONSUL_NAMESPACE`, `DILOCK_TEST_CONSUL_EPOCH`, and optional
`DILOCK_TEST_CONSUL_TOKEN`. It requires pre-provisioned metadata and does not
create or repair the namespace record.
