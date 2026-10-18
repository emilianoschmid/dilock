# Dilock

One distributed-lock contract, carried across 33 language ecosystems.

Dilock is an AI-assisted polyglot engineering experiment: it explores how one carefully specified distributed-lock contract can be implemented with idiomatic APIs across a broad set of languages. The repository has 33 language/ecosystem implementations or facades and four backend profiles: Redis, etcd, the draft Consul HTTP/KV session profile, and the experimental PostgreSQL advisory-session profile. Some languages own their backend adapters; others reuse a shared Java, .NET, Elixir, TypeScript, or C ABI engine. This breadth is the point of the project, not a claim that every route has equal maturity.

The design keeps language APIs separate from coordination-backend profiles, so additional synchronization technologies can be explored as new adapters without changing the project's polyglot goal. Redis and etcd have broad SDK coverage, and the Consul profile now has an implementation route and local validation evidence across all 33 active ecosystems. PostgreSQL is the current cross-language implementation effort; it uses session advisory locks, not transaction-pool locks, and provides no generic fencing. Validation depth varies by route; single-node backend tests do not establish quorum, failover or production recovery guarantees. Different backends do not coordinate with each other. The contract specifies resource identity, ownership, lifecycle and failure outcomes, and each SDK exposes it in the language's native style. See the [PostgreSQL implementation plan](docs/plans/POSTGRESQL-ALL-SDKS.md).

**Status: experimental and under active development; not production-ready.** There is no published release. Support and validation vary by SDK/backend; source code and examples do not imply production readiness, failover guarantees, or package availability.

## Documentation

This README is the human-facing project entry point. The [documentation index](docs/humans/README.md) links to shared specifications, architecture and engineering references. Workspace-only agent instructions and project notes are kept outside the public documentation set.

## Start here

Start with [semantics](spec/SEMANTICS.md), [language API guidance](docs/api/LANGUAGE-DESIGN.md), and [backend profiles](docs/architecture/BACKENDS.md). The [small style examples](examples/style/README.md) illustrate idioms only; the [distributed Redis and etcd examples](docs/examples/README.md) exercise the SDK backends. A separate [Redis command-boundary example](examples/redis_python/README.md) demonstrates owner-checked commands; it is not a distributed SDK implementation.

## Languages and distributed-lock technologies

The table below makes the current breadth visible. It lists the SDK implementation route and the technology used to coordinate through each backend. Objective-C remains a catalog placeholder without SDK source, so it is not counted among the 33 implementations. A resource is coordinated only by clients using the same backend, namespace, and protocol profile. See each linked guide for limitations and validation status.

| Language / ecosystem | Redis lock route | etcd lock route | Consul lock route | PostgreSQL lock route | Usage and examples |
|---|---|---|---|---|---|
| C# / .NET | .NET SDK + StackExchange.Redis | .NET SDK + native etcd gRPC | .NET engine | Native Npgsql provider | [SDK guide](sdk/dotnet/README.md) · [examples](docs/examples/README.md) |
| C++ | C++ SDK + redis-plus-plus / hiredis | C++ SDK + native etcd gRPC / Protobuf | Native C++ engine | Native libpq provider | [SDK guide](sdk/cpp/README.md) · [example](sdk/cpp/README.md) |
| Python | Python SDK + redis-py | Python SDK + native etcd gRPC / Protobuf | Direct HTTPX adapter | Native psycopg adapter | [SDK guide](sdk/python/README.md) · [examples](docs/examples/README.md) |
| TypeScript / Node.js | TypeScript SDK + node-redis | TypeScript SDK + native etcd gRPC-js / Protobuf | TypeScript engine | Native `pg` provider | [SDK guide](sdk/typescript/README.md) · [examples](docs/examples/README.md) |
| Go | Go SDK + go-redis | Go SDK + etcd v3 client | Native `net/http` module | Native `pgx` provider | [SDK guide](sdk/go/README.md) · [examples](docs/examples/README.md) |
| Rust | Rust SDK + redis crate | Rust SDK + etcd-client | C ABI over shared C++ engine | Native `tokio-postgres` provider | [SDK guide](sdk/rust/README.md) · [examples](docs/examples/README.md) |
| Java | Java SDK + Jedis | Java SDK + Jetcd | Java engine | Native JDBC provider | [SDK guide](sdk/java/README.md) · [examples](docs/examples/README.md) |
| Dart | Dart Redis adapter | Dart etcd v3 gRPC adapter | Native `dart:io` package | `postgres` client adapter | [SDK guide](sdk/dart/README.md) · [guide examples](sdk/dart/README.md) |
| Elixir | Elixir adapter + Redix | Elixir adapter + generated etcd v3 gRPC stubs | Native Elixir engine | Native Postgrex provider | [SDK guide](sdk/elixir/README.md) · [examples](sdk/elixir/examples/) |
| PHP | PHP adapter + ext-redis | PHP adapter + native etcd gRPC | Native HTTP streams | PDO PostgreSQL adapter | [SDK guide](sdk/php/README.md) · [examples](sdk/php/examples/) |
| Ruby | Ruby adapter + redis gem | Ruby adapter + native etcd gRPC | Direct standard-library HTTP adapter | `pg` adapter | [SDK guide](sdk/ruby/README.md) · [examples](sdk/ruby/examples/) |
| Swift | Swift adapter + RediStack | Swift adapter + grpc-swift / Protobuf | Direct SDK | Native PostgresNIO provider | [SDK guide](sdk/swift/README.md) · [guide examples](sdk/swift/README.md) |
| OCaml | OCaml/Lwt adapter + Redis-Lwt | Experimental native etcd gRPC adapter | Optional direct package | Lwt + ctypes C ABI facade | [SDK guide](sdk/ocaml/README.md) · [guide examples](sdk/ocaml/README.md) |
| Perl | Perl Redis adapter | Perl/EV adapter + native etcd gRPC | Optional direct HTTP adapter | DBI/DBD::Pg adapter | [SDK guide](sdk/perl/README.md) · [guide examples](sdk/perl/README.md) |
| Julia | Experimental adapter + Redis.jl | Experimental adapter + gRPCClient | Direct HTTP adapter | LibPQ.jl adapter | [SDK guide](sdk/julia/README.md) · [examples](sdk/julia/examples/) |
| R | Experimental adapter + redux/hiredis | Experimental native etcd gRPC adapter | Direct HTTP SDK | DBI/RPostgres adapter | [SDK guide](sdk/r/README.md) · [examples](sdk/r/examples/) |
| JavaScript | TypeScript SDK runtime + node-redis | TypeScript SDK runtime + etcd gRPC-js | TypeScript runtime facade | TypeScript runtime facade | [SDK guide](sdk/javascript/README.md) · [examples](sdk/javascript/examples/) |
| Kotlin | Java backend engine + Kotlin facade | Java backend engine + Kotlin facade | Java engine facade | Java JDBC facade | [SDK guide and examples](sdk/kotlin/README.md) |
| Scala | Java backend engine + Scala facade | Java backend engine + Scala facade | Java engine facade | Java JDBC facade | [SDK guide and examples](sdk/scala/README.md) |
| Groovy | Java backend engine + Groovy facade | Java backend engine + Groovy facade | Java engine facade | Java JDBC facade | [SDK guide](sdk/groovy/README.md) · [examples](sdk/groovy/examples/) |
| Clojure | Java backend engine + Clojure facade | Java backend engine + Clojure facade | Java engine facade | Java JDBC facade | [SDK guide](sdk/clojure/README.md) · [examples](sdk/clojure/examples/) |
| F# | .NET backend engine + F# facade | .NET backend engine + F# facade | .NET engine facade | .NET Npgsql facade | [SDK guide and examples](sdk/dotnet/reuse/fsharp/README.md) |
| Visual Basic .NET | .NET backend engine + VB facade | .NET backend engine + VB facade | .NET engine facade | .NET Npgsql facade | [SDK guide and examples](sdk/dotnet/reuse/vb/README.md) |
| PowerShell | .NET backend engine + PowerShell module | .NET backend engine + PowerShell module | .NET engine module | .NET Npgsql module | [SDK guide and examples](sdk/dotnet/reuse/powershell/README.md) |
| Erlang | Elixir backend engine + Erlang facade | Elixir backend engine + Erlang facade | Elixir engine facade | Elixir Postgrex facade | [SDK guide](sdk/erlang/README.md) · [examples](sdk/erlang/examples/) |
| C | C ABI over the C++ Redis engine | C ABI over the C++ etcd engine | C ABI over shared C++ engine | C ABI over C++ provider | [SDK guide](sdk/c/README.md) · [examples](sdk/c/examples/) |
| Objective-C | Not implemented | Not implemented | Not implemented | Not implemented | [Status and examples index](docs/examples/README.md) |
| Nim | C ABI over the C++ Redis engine | C ABI over the C++ etcd engine | C ABI facade | C ABI facade | [SDK guide](sdk/nim/README.md) · [examples](sdk/nim/examples/) |
| D | C ABI over the C++ Redis engine | C ABI over the C++ etcd engine | C ABI facade | C ABI facade | [SDK guide](sdk/d/README.md) · [examples](sdk/d/examples/) |
| Crystal | Crystal Redis adapter | Experimental C ABI / C++ etcd facade | C ABI facade | C ABI facade | [SDK guide](sdk/crystal/README.md) · [examples](sdk/crystal/examples/) |
| Fortran | ISO_C_BINDING to the C ABI / C++ Redis engine | ISO_C_BINDING to the C ABI / C++ etcd engine | ISO_C_BINDING C ABI facade | ISO_C_BINDING C ABI facade | [SDK guide](sdk/fortran/README.md) · [examples](sdk/fortran/examples/) |
| Zig | C ABI / C++ Redis engine | C ABI / C++ etcd engine | C ABI facade | C ABI facade | [SDK guide](sdk/zig/README.md) · [examples](sdk/zig/examples/) |
| Haskell | Haskell adapter + hedis | Experimental C ABI / C++ etcd facade | C ABI facade | C ABI facade | [SDK guide](sdk/haskell/README.md) · [examples](sdk/haskell/examples/) |
| Lua / LuaJIT | OpenResty `lua-resty-redis` adapter; standalone Lua Redis candidate | Experimental LuaJIT C ABI facade | LuaSocket, OpenResty cosocket, and C ABI routes | LuaSQL PostgreSQL adapter + LuaJIT C ABI facade | [SDK guide](sdk/lua/README.md) · [examples](sdk/lua/) |

The [distributed examples index](docs/examples/README.md) links the backend quickstarts and per-language samples. Some links point to a runnable source file; others point to a guide whose example is embedded in the README. The [style examples](examples/style/README.md) remain syntax references only and are not distributed-lock implementations.

The listed gRPC and Protobuf components are client-side etcd transport dependencies; they do not require installing Dilock or a separate service beside etcd. Redis owner-check scripts are bundled by the SDK and executed by Redis's built-in Lua engine; operators do not install a Lua service or load Dilock scripts manually.

For example, install the Python Redis adapter from this checkout with `python -m pip install 'sdk/python[redis]'`. Provision matching namespace metadata before connecting. Package identifiers and publication ownership remain unverified.

## One repository, native packages

Each SDK uses its native build and package format. Backend clients are optional
dependencies, and consumers should install only the adapter they need. Local
builds and source archives are not published packages or production-readiness
evidence.

```text
sdk/                  Experimental native SDK implementations
examples/style/       Small, local-only implementation references
examples/redis_python/ Redis command-boundary reference, not a complete SDK
spec/                 Semantics, wire identity, backend profiles, Lua
conformance/          Shared scenarios and native-worker protocol
infra/                Disposable backend/fault-test infrastructure
docs/humans/          Human-facing documentation index
docs/examples/        Runnable Redis and etcd SDK examples
docs/                 Architecture, API, engineering and ADR documentation
```

The same logical resource is shared only when clients use the same coordination domain/backend, namespace and protocol. Redis and etcd do not jointly arbitrate a resource. There is no automatic fallback between coordinators.

## Guarantees are not interchangeable

Leases can expire while a process is paused. Renewal does not stop stale work. Random ownership tokens prevent another acquisition from being deleted; they are not fencing tokens. External effects may require resource-side fencing, transactions or idempotency. The Redis single-authority profile does not promise strict exclusivity across state-losing failover. See the [backend profiles](docs/architecture/BACKENDS.md).

## Contributing

Read [CONTRIBUTING](CONTRIBUTING.md), [testing guidance](docs/engineering/TESTING.md) and [package policy](docs/engineering/PACKAGE-MATRIX.md). Public documentation and API comments are in English. No fake badges, unsupported compatibility claims or placeholder packages.

Security reporting: [SECURITY](SECURITY.md). License: [Apache-2.0](LICENSE). This repository contains no credentials, production endpoints or published-package ownership claim.
