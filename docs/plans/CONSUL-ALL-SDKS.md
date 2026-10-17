# Consul support across Dilock SDKs

## Local validation environment (2026-10-17)

The local Consul dev server is defined in [`infra/consul.compose.yml`](../../infra/consul.compose.yml). It runs Consul 1.22.5 with HTTP bound to `127.0.0.1:28500` and service discovery on the Compose network as `http://consul:8500`. `docker compose -f infra/consul.compose.yml config` passed, the service became healthy, and `/v1/status/leader` returned a leader. `tools/provision_consul.py` creates the namespace metadata explicitly; the default test namespace used for the direct SDK run was `consul-validation` with epoch `local-validation-2026-10-16`. Language test runners ran as ephemeral containers on the same Docker network. The Consul server and its named volume are local development state; stop with `docker compose -f infra/consul.compose.yml down` when no longer needed.

Example setup and namespace provisioning:

```powershell
docker compose -f infra/consul.compose.yml up -d
python tools/provision_consul.py --endpoint http://127.0.0.1:28500 --namespace consul-validation --epoch local-validation-2026-10-16
```

The host Docker API is still subject to the Windows named-pipe ACL, so runner commands were executed with the explicitly authorized elevated Docker access. Earlier toolchain gaps were bypassed with version-pinned ephemeral runners where possible; they should not be read as evidence that those toolchains are installed on the host. The previous 2026-10-15 checkout/Node interruption is resolved.

## Contract

Implement the `consul-kv-session-v1` profile from [the backend specification](../../spec/backends/CONSUL.md) through each active SDK and shared engine. Consul is an optional backend; Redis/etcd behavior and dependency closures remain unchanged. Require explicit metadata provisioning, per-attempt sessions, owner-checked acquire/release, conservative renewal, and `(epoch, LockIndex)` fencing. Lock KV entries must be retained so their fencing counter cannot reset.

## Execution phases

1. Define and provision the profile; implement Python reference adapter and real-server proof.
2. Implement shared language engines (currently .NET, Java, TypeScript, Elixir, C++/C ABI) and expose them through each facade/runtime route.
3. Implement direct SDK routes without a shared engine.
4. Add each route to the release catalog, docs, examples, and a capability/evidence matrix.
5. Run offline contract checks and real Consul lifecycle/contention tests where toolchains exist. Keep single-node behavior separate from quorum/failover evidence.

## Route status

`Implemented` means source and a consumer path exist. It does not imply real-server or interoperability evidence; record those separately in the SDK tests and release artifacts.

| Ecosystem | Route | Status | Evidence / blocker |
|---|---|---|---|
| C# / .NET | .NET engine | Implemented | .NET 8 container full test run: 48 tests, 34 passed, 14 skipped, 0 failed; Consul package built. Re-ran `LiveConsulContentionRenewalAndLockIndexFence` against the local Consul: 1/1 passed with contention, renewal, release and increasing `LockIndex`. Redis/etcd environment-gated cases were skipped |
| Python | Direct HTTPX adapter | Implemented | Python 3.11 container: real Consul suite 3 passed, including contention, renewal, stale-owner cleanup and fencing. Host asyncio runtime still hangs; container runner bypasses the host issue |
| F# | .NET facade | Implemented | .NET 8 container consumer build succeeded with 0 warnings/errors; earlier single-node live lifecycle evidence remains |
| VB.NET | .NET facade | Implemented | .NET 8 container consumer build succeeded with 0 warnings/errors; earlier single-node live lifecycle evidence remains |
| PowerShell | .NET module | Implemented | Earlier module import and single-node live lifecycle passed; not rerun in this validation pass |
| Java | Java engine | Implemented | Maven/JDK 21 container reactor `install` for core/Redis/etcd/Consul passed after explicit namespace provisioning; Consul live lifecycle passed. Adapter suite: 6 tests, 0 failures/errors/skips |
| Kotlin | Java facade | Implemented | Gradle 9.1/JDK 21 test build passed with a live Consul facade test exercising contention and increasing LockIndex |
| Scala | Java facade | Implemented | sbt 1.10.7 cross-run passed: Scala 2.13 8/8 and Scala 3 19/19; both ran live Consul contention/handoff/fencing |
| Groovy | Java facade | Implemented | Maven/JDK 21: 11 contract tests and Consul live facade lifecycle passed; 2 Redis/etcd live tests skipped. Live test found and fixed a missing `withLease(ConsulLease, Closure)` overload |
| Clojure | Java facade | Implemented | Clojure tools-deps/Java 21: 15 tests, 56 assertions, 0 failures/errors; includes Consul live contention, scoped release and fence handoff |
| TypeScript | TypeScript engine | Implemented | Node 22 container `npm ci && npm test`: 48 total, 44 passed, 4 skipped, 0 failed; strict build and Consul live test passed. Four skipped cases are Redis/etcd integrations |
| JavaScript | TypeScript runtime facade | Implemented | Included in the same TypeScript workspace install/build/test run; package tests passed |
| Elixir | Elixir engine | Implemented | Elixir 1.18/OTP 28 container: full `mix test` passed 59 tests; targeted live Consul contention/handoff/fencing passed 1/1. Hex reported a HIGH advisory for the pinned transitive `protobuf` 0.17.0 package; Redix/GRPC emitted missing optional-adapter warnings |
| Erlang | Elixir facade | Implemented | `erlc` compiled the facade and `elixir` ran EUnit with the required BEAM runtime loaded: 15/15 passed. The Consul protocol lifecycle is covered by the Elixir engine test; no Erlang-specific live API test exists |
| C++ | Native engine | Implemented | Linux container build passed with Consul+etcd+C ABI enabled. Full CTest: 7 passed, 2 skipped (Redis/etcd live endpoints unset), 0 failed. Consul contract and live integration passed; lifecycle covered contention, renewal, release, handoff, and increasing `LockIndex` |
| C | C ABI over C++ | Implemented | C17 API consumer and Consul quickstart compiled with warnings-as-errors. C ABI consumer passed; live quickstart acquired/released against local Consul and returned `LockIndex=1`. C ABI lifecycle is also exercised through the C++ engine live integration |
| Nim | C ABI facade | Implemented | Nim 2.2.12 contract suite passed 9/9; new C ABI live consumer passed Consul contention, renewal, release, handoff, and increasing `LockIndex` |
| D | C ABI facade | Implemented | DUB/LDC 1.40 unit-test build passed; standalone LDC consumer compiled and passed live Consul contention, renewal, release, handoff, and increasing `LockIndex` |
| Fortran | ISO_C_BINDING facade | Implemented | gfortran 14/CMake Release built all targets; facade CTest passed 1/1; installed package and external consumer built and passed; Consul quickstart acquired, renewed, fenced (`LockIndex=1`) and released against local Consul |
| Zig | C ABI facade | Implemented | Zig 0.17.0 suite: 9/9 tests passed, all package/examples built, Consul quickstart passed live with fence `local-consul-epoch:2`, TTL 30000 ms and confirmed release. Fixed Consul poll default parity (50 ms) and Zig 0.17 environment API in the quickstart |
| Go | Native `net/http` module | Implemented | Go 1.26 container `go test -mod=readonly -count=1 ./consul` passed, including contract and real Consul contention/renew/release/fencing lifecycle |
| Rust | C ABI facade over shared C++ engine | Implemented | `dilock-consul` contract tests passed 3/3 with native link; Consul quickstart passed live provisioning, acquisition, fence (`local-consul-epoch:3`) and release. Revalidation used runner Rust 1.85 with `--ignore-rust-version`; earlier Rust 1.90 `cargo check` also passed |
| PHP | Native HTTP streams | Implemented | PHP container dependency install and syntax checks passed; custom live lifecycle against Consul passed, including contention, renewal, release and successor fencing. PHPUnit itself was not run |
| Ruby | Direct standard-library HTTP adapter | Implemented | Ruby 3.4 container contracts: 4 runs/8 assertions; real lifecycle: 1 run/9 assertions; all passed |
| Swift | Direct SDK | Implemented | Swift 6.4.1 Linux build passed; `swift test --filter DilockConsulTests --jobs 4` passed 4/4, including real Consul contention, renewal, release, and increasing `LockIndex`. Added the conditional `FoundationNetworking` import for Linux and split nested `#require` expressions so the test compiles |
| Dart | Native `dart:io` package | Implemented | Dart stable container: `dart analyze lib test` clean and `dart test` passed all 4 tests, including live Consul contention, renewal, release and monotonic LockIndex lifecycle |
| Crystal | C ABI facade over shared C++ engine | Implemented | Official Crystal 1.21.1 tarball checksum verified; Consul contract spec passed 2/2 and live spec passed 1/1 against local Consul. Covers explicit provisioning, acquire, validity, TTL, fence and release |
| Haskell | C ABI facade over the shared C++ engine | Implemented | GHC 9.6.6/Cabal built the Consul-enabled package; `cabal test --flag=consul-c-abi` passed both suites, including C ABI contracts; live executable passed acquire, contention, renewal, release, and increasing fencing. Removed an unused test binding and reran live target cleanly |
| OCaml | Optional direct package | Implemented | OCaml 5.3.0/opam 2.3.0 with pinned `mtime` 2.2.0: base and Consul `dune build @runtest` passed; Consul contract suite passed 5/5; live lifecycle passed provisioning, acquire, renewal, nonzero fence and release. Installed both packages into the local opam switch and built/ran a separate Dune consumer against Consul. Fixed Dune project/package naming, Cohttp method API, large `LockIndex` parsing via `Yojson.Safe`, acquisition option unwrapping and `with_lease` call signature |
| Perl | Optional direct HTTP adapter | Implemented | Perl 5.40 container `prove -Ilib t/consul_adapter.t t/consul_integration.t`: 2 files, 21 tests, all passed including real Consul lifecycle |
| Julia | Direct HTTP adapter | Implemented | Julia 1.13.2 official binary hash verified; `Pkg.instantiate()` and full `test/runtests.jl` passed (171 assertions total), including live Consul metadata provisioning, acquisition, fencing, renewal and release. Fixed HTTP.jl timeout keywords to use bounded integer-second values; live test exposed the former Float64 type error |
| R | Direct HTTP SDK | Implemented | R 4.5 syntax parsed; metadata/epoch, extra-field, unsigned LockIndex, duplicate/malformed LockIndex, invalid TTL/token and release uncertainty contracts passed; native clock helper compiled and live Consul lifecycle passed contention, renewal, release and fence handoff (`1 → 2`) |
| Lua / LuaJIT | LuaSocket + OpenResty cosocket + shared C ABI | Implemented | Lua 5.1 deterministic Consul contract and live lifecycle passed; LuaJIT Consul contract and C ABI live lifecycle passed. OpenResty 1.27.1.2 integration endpoint passed against Consul with contention, renewal, fencing, release and successor handoff. Exposed the shared `dilock.resource_id` used by the Consul facade and added Docker DNS resolver configuration to the integration fixture |

Objective-C is a catalog placeholder and is outside the 33 active routes. Every active route now has local compile/contract/lifecycle evidence as recorded above; `Implemented` continues to mean source/package/docs presence, while the evidence column records what actually ran. Cross-language contention, multi-node quorum and failover remain separate gates; this local dev server is single-node evidence only.
