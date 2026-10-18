# PostgreSQL advisory-lock support across Dilock SDKs

## Objective

Implement `postgres-advisory-session-v1` across every active Dilock language
and ecosystem route, while preserving the shared draft-1 identity and failure
semantics. The objective is implementation plus evidence, not merely adding
examples or a provider name: each route needs a native build/test path, a
consumer or facade check, and real PostgreSQL contention evidence before it can
be described as supported.

The profile is defined in [`spec/backends/POSTGRESQL.md`](../../spec/backends/POSTGRESQL.md).
It uses one private PostgreSQL session per logical acquisition,
`pg_try_advisory_lock(bigint)` for bounded polling,
`pg_advisory_unlock(bigint)` on the same session, and an explicit metadata row
in `dilock_namespace_meta`. It has no server TTL and no generic fencing. The
database connection is the coordination domain; connection loss is cleanup,
not proof that stale application work stopped.

## Delivery phases

1. Contract and provisioning: identity conversion, metadata schema, SQL
   provisioning, backend profile, capability/error mapping and conformance
   scenarios.
2. Reference vertical slices: Python, C#/.NET, C++/C ABI, Java, TypeScript,
   Go, Rust and Elixir with a disposable real PostgreSQL service.
3. Native direct routes: Swift, Dart, PHP, Ruby, Perl, Julia, R, OCaml and
   Lua/LuaJIT.
4. Facades and catalog routes: JavaScript, Kotlin, Scala, Groovy, Clojure,
   F#, Visual Basic .NET, PowerShell, Erlang, Crystal, Haskell, Nim, D,
   Fortran and Zig.
5. Cross-language contention matrix, package consumers, CI jobs, release
   catalog and documentation. Objective-C remains a catalog placeholder, as
   it has no Dilock SDK source route.

## Route matrix

| Ecosystem | PostgreSQL route | State |
|---|---|---|
| Python | Native async `psycopg` adapter | Local PostgreSQL 16 conformance pass; hosted CI evidence pending |
| C# / .NET | Native Npgsql provider | Implemented first slice and live consumer/CI job; local provider build passes, hosted PostgreSQL evidence pending |
| C++ | Native libpq provider | Containerized C++ build, contract and live PostgreSQL contention pass; hosted CI evidence pending |
| C | C ABI over C++ provider | PostgreSQL-only C ABI build, quickstart and live contention/handoff pass; hosted CI evidence pending |
| Java | Native JDBC provider | Maven reactor plus real PostgreSQL JUnit pass locally (3 tests, 0 failures); hosted CI evidence pending |
| TypeScript / Node.js | Native `pg` provider | Workspace build, contract suite, exact-tarball consumer and local PostgreSQL 16 contention/session-loss conformance pass; hosted CI evidence pending |
| JavaScript | TypeScript runtime facade | Implemented through `@dilock/postgres`; exact-tarball consumer and local PostgreSQL route pass; hosted CI evidence pending |
| Go | Native `pgx` provider | Dockerized Go 1.26 live contention/handoff pass and CI job; hosted evidence pending |
| Rust | Native `tokio-postgres` provider or shared reviewed ABI | Dockerized Rust 1.88 live contention/handoff pass and CI job; hosted evidence pending |
| Elixir | Native Postgrex provider | Mix compile plus local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Erlang | Elixir engine facade | Erlang facade compiled and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Swift | Native PostgresNIO provider | Swift 6.1 containerized contract and real PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Dart | `postgres` client adapter | Dart analyze/contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| PHP | PDO PostgreSQL adapter | PHP 8.4/PDO PostgreSQL contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Ruby | `pg` adapter | Ruby 3.3/pg contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Perl | DBI/DBD::Pg adapter | Perl/DBI/DBD::Pg contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| Julia | LibPQ.jl adapter | Julia 1.10/LibPQ 1.18 contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| R | DBI/RPostgres adapter | R 4.4/DBI/RPostgres contract and local PostgreSQL contention/renew/handoff pass; hosted CI evidence pending |
| OCaml | Lwt + ctypes C ABI facade over native PostgreSQL provider | Dune/ctypes package build and real PostgreSQL provision, contention, liveness, release and handoff pass locally; hosted CI evidence pending |
| Kotlin | Java JDBC facade | Gradle suite plus real PostgreSQL facade contention/handoff pass locally; hosted CI evidence pending |
| Scala 2.13 / 3 | Java JDBC facade | Scala 3 and 2.13 sbt compile plus real PostgreSQL facade contention/handoff pass locally; hosted CI evidence pending |
| Groovy | Java JDBC facade | Maven/Groovy suite plus real PostgreSQL facade contention/handoff pass locally (15 tests, 3 unrelated live skips); hosted CI evidence pending |
| Clojure | Java JDBC facade | tools.deps suite plus real PostgreSQL live contention/handoff pass locally (15 tests, 52 assertions); hosted CI evidence pending |
| F# | .NET Npgsql facade | Docker .NET 8 build and local PostgreSQL contention/cancellation/handoff consumer pass; hosted CI evidence pending |
| Visual Basic .NET | .NET Npgsql facade | Docker .NET 8 build and local PostgreSQL contention/cancellation/handoff consumer pass; hosted CI evidence pending |
| PowerShell | .NET Npgsql module | Module/example syntax and local PostgreSQL contention/cancellation/handoff consumer pass; hosted CI evidence pending |
| Crystal | C ABI facade | Crystal 1.21 format, C ABI link and real PostgreSQL contention/handoff pass locally; hosted CI evidence pending |
| Nim | C ABI facade | Nim 2.2 compile/link plus real PostgreSQL provision, contention, liveness, release and handoff pass locally; hosted CI evidence pending |
| D | C ABI facade | Dedicated PostgreSQL-only D facade compiles, links against the PostgreSQL-only C ABI, and passes real PostgreSQL provision, contention, liveness, release and handoff; hosted CI evidence pending |
| Fortran | C ABI facade | Fortran PostgreSQL live consumer compiles, links against the PostgreSQL-only C ABI with dead-code elimination, and passes real PostgreSQL provision, contention, liveness, release and handoff; hosted CI evidence pending |
| Zig | C ABI facade | Zig 0.17 isolated `test-postgres` build and real PostgreSQL provision, contention, liveness, release and handoff pass locally; hosted CI evidence pending |
| Haskell | C ABI facade | Cabal build and real PostgreSQL provision, contention, liveness, release and handoff pass locally; hosted CI evidence pending |
| Lua / LuaJIT | LuaSQL PostgreSQL adapter plus LuaJIT C ABI facade | Fixed LuaJIT import and LuaSQL scalar-result cleanup bugs; both LuaJIT C ABI and generic LuaSQL provision, contention, liveness, release and handoff pass locally; hosted CI evidence pending |

## Acceptance gates

- Metadata is explicitly provisioned and semantically validated; runtime
  clients never create or repair it.
- The advisory key conversion is identical in every route and covered by
  shared golden vectors.
- A held acquisition owns one physical session. No transaction pool, shared
  session, unconditional unlock or force-unlock path is accepted.
- Contention, release/handoff, different-resource progress, timeout,
  cancellation, lost acknowledgement, session loss and old-session cleanup
  are tested against the same real PostgreSQL database.
- `require_fencing` fails before backend I/O; `renew` is documented and tested
  as a liveness check only.
- Each native package consumer is built from the installed/archive artifact,
  not just from the repository source tree.
- CI runs the PostgreSQL route and records server/client versions. A green
  single-node run does not imply failover, fencing, production recovery or
  publication readiness.

Provisioning now serializes first-time metadata DDL in the shared Python
helper and every native PostgreSQL provisioning route with the shared
transaction advisory key `hashtextextended('dilock:namespace-meta:v1', 0)`;
facade routes inherit that behavior from their native engine or C ABI. The Go
and Rust integration tests were run concurrently against one PostgreSQL 16
instance, and the local Python/TypeScript cross-language harness verifies
shared ownership, contention, renewal and release/handoff against one
PostgreSQL 16 instance. The serialization guarantee still needs hosted
cross-language matrix evidence before it can be treated as a release claim.

The PR workflow now has disposable PostgreSQL services and route jobs for
Python, TypeScript/Node, .NET, C++/C ABI, C ABI Crystal/Nim/Fortran/D/OCaml/LuaJIT/LuaSQL, Java, Scala 2.13/3, Kotlin, Groovy, Go, Rust,
Elixir/Erlang, Dart, Swift, Julia, PHP, Ruby, Perl, R, Clojure, F#, Visual Basic
.NET, PowerShell and Haskell.
Every PostgreSQL job records both the PostgreSQL client version and the
server version through `conformance/record_postgres_versions.sh` before its
route-specific build or live test.
The Java package-consumer gate also builds the installed/archive PostgreSQL
artifact in an isolated consumer workspace and verifies its JDBC dependency
closure plus provider, lock, lease and provisioning API surface.
The Python wheel/sdist, .NET NuGet, C++ CMake and Rust crate consumer drivers
now include their PostgreSQL artifacts and public surfaces in the same exact
artifact checks. The dedicated Rust PostgreSQL CI job now builds a light
PostgreSQL C ABI for linking and runs that exact-artifact consumer alongside
the live Rust contract; their environment-specific package runs remain
separate evidence from the local source and live-provider tests.
Local evidence additionally covers the C++/C ABI, Crystal, Nim, Fortran, D, OCaml, Haskell, Zig, LuaJIT, LuaSQL, Elixir/Erlang, Dart, Swift,
Julia, PHP, Ruby, Perl, R, F#, Visual Basic .NET, PowerShell, Java, Kotlin,
Scala 2.13/3, Groovy and Clojure routes, plus the local Python/TypeScript
cross-language matrix. Hosted results for those jobs and the cross-language
matrix are still required before the acceptance matrix is complete.

## Evidence refresh — 2026-10-18

- Rust `dilock-postgres` live contract passed 1/1 against a temporary
  PostgreSQL 16 instance: provisioning, same-resource contention, different
  resource progress, timeout, liveness renewal, unsupported fencing, release
  and handoff all completed successfully.
- The Rust exact-artifact consumer passed with Rust 1.88 after packaging and
  extracting `dilock`, `dilock-redis`, `dilock-etcd`, `dilock-consul` and
  `dilock-postgres`, then running an external `cargo check`, `cargo test` and
  `cargo run`. Its Consul check is deliberately public-surface/type coverage;
  native Consul calls remain covered by the dedicated C ABI contract suite.
- The Rust CI C ABI build options were also simulated locally with the Conan
  toolchain and PostgreSQL 16.15 headers; `libdilock_c.so` and
  `libdilock_cpp_postgres.so` installed successfully.
- All native provisioning routes now serialize first-time metadata table/row
  setup with the same PostgreSQL transaction advisory key; facade routes reuse
  those native/C ABI implementations. The route-matrix regression guard passes
  24/24 checks, and the .NET PostgreSQL provider builds with zero warnings.
- A disposable PostgreSQL 16.15 run passed the Python route with contention,
  independent-resource progress, session-loss cleanup, timeout, cancellation,
  capability rejection and handoff. The Linux .NET live consumer also passed
  contention, independent-resource progress, timeout, cancellation, capability
  rejection and handoff after the transactional provisioning change.
- The expanded Python and TypeScript conformance runners now execute metadata
  immutability, missing/mismatched metadata rejection, same-process
  non-reentrancy, old-session release rejection and authentication-failure
  checks. Both runners passed locally against PostgreSQL 16.15; these checks
  remain local evidence until their hosted CI jobs publish fresh reports.
- The full expanded local reports are retained locally as
  `validation/implementation/postgres-advisory-session-expanded-local-20261018.json`
  and
  `validation/implementation/postgres-advisory-session-typescript-expanded-local-20261018.json`;
  they are ignored local-only evidence and are not repository artifacts.
- The Python PostgreSQL contract suite now deterministically verifies that lost
  acquisition and release acknowledgements produce an unknown outcome and
  close the physical session; the six-test contract file passes in the pinned
  Python 3.13/pytest-asyncio environment.
- The live TypeScript route now handles PostgreSQL's asynchronous idle-session
  error event and passes the same forced-session-termination release check;
  without that listener Node could crash before the provider reported the
  required unknown outcome.
- Swift 6.1 target validation now passes 2/2 with Swift Testing explicitly
  selected: signed advisory-key vector and real PostgreSQL contention,
  renewal, release and handoff against PostgreSQL 16.15.
  The retained report is
  `validation/implementation/postgres-advisory-swift-local-20261018.json`.
- The Go `pgx` route also passed its live PostgreSQL test (`go test ./...`,
  1/1) against PostgreSQL 16.15; the retained report is
  `validation/implementation/postgres-advisory-go-local-20261018.json`.
- The Python/TypeScript cross-language harness passed against one disposable
  PostgreSQL 16 instance in both directions: Python held while TypeScript
  observed contention, then TypeScript held while Python observed contention,
  followed by successful release. The retained report is
  `validation/implementation/postgres-cross-language-local-20261018.json`.
- The C++ `libpq` route passed its real PostgreSQL integration target against
  PostgreSQL 16.15, covering transactional provisioning, contention,
  independent-resource progress, timeout, cancellation, renewal, release and
  handoff. The retained report is
  `validation/implementation/postgres-advisory-cpp-local-20261018.json`.
- The shared C ABI facade routes passed locally for Crystal, D, Fortran, Lua,
  LuaJIT, Nim and OCaml against PostgreSQL 16.15. Nim initially exposed a
  runtime-baseline defect: libraries built on Ubuntu 24.04 required newer
  glibc/libstdc++ symbols than its Debian-based image provides. The CI facade
  jobs now build on Ubuntu 22.04, and the compatibility build plus all seven
  consumers pass. The Haskell C ABI consumer also passes locally with
  GHC 9.10.3/Cabal 3.14.1.1 on Debian bookworm; its CI job uses the same
  Ubuntu 22.04 build baseline because the consumer runs in Debian bookworm.
  The retained report is
  `validation/implementation/postgres-advisory-cabi-facades-local-20261018.json`.
- These are local evidence only. Hosted CI, failover/recovery and production
  publication/readiness remain outside the achieved evidence boundary.
