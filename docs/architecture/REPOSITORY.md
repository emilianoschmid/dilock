# Repository and package architecture

## Decision

Start with one polyglot monorepo. A GitHub repository is a collaboration/version-control boundary; an SDK directory is a native build boundary; a package is a distribution boundary. They need not coincide.

```text
dilock/
  .github/                 issue templates and reviewed workflows
  spec/                    normative language-independent semantics
    backends/              protocol for each coordinator
    vectors/               cross-language golden data
  sdk/
    dotnet/                native .NET solution
      src/                 Core, Redis, Etcd, optional DI package
      tests/               unit, backend, API and package-consumer tests
      examples/
    python/                native Python workspace
      packages/            dilock, dilock-redis, dilock-etcd
      tests/
      examples/
    cpp/                   one native CMake project
      include/dilock/
      src/                 core/, redis/, etcd/
      tests/
      examples/
      packaging/           Conan and vcpkg
    typescript/            native npm workspace, core and optional adapters
    go/                    core and backend modules with correct module tags
    rust/                  Cargo workspace, core and adapter crates
    java/                  native JVM build, core and adapters
    kotlin/  scala/        idiomatic JVM facades, shared Java protocol engine
    php/  ruby/            Composer/gem projects, optional adapters
    swift/  dart/          SPM/pub projects for trusted application runtimes
    elixir/  c/            Mix project / optional C ABI
  conformance/             shared JSONL worker contract and test scenarios
  tests/                   bootstrap and future cross-SDK orchestration
  infra/                   disposable integration/fault environments
  tools/                   thin orchestration, no production runtime
  docs/                    human-facing API, engineering and ADR documents
```

The blueprint has fifteen SDK design directories; runtime implementation remains planned. Do not create empty publishable packages for every roadmap language. No `.git` directory under `sdk/`; no submodules; no independent backend repository.

## Why this fits this project

A protocol change can require all SDKs, fixtures and failure tests to change together. One PR lets reviewers inspect that complete change. The same repository can release multiple NuGet/PyPI packages and CMake targets. Contributors still build only their SDK; path-aware CI avoids a full matrix for every documentation edit.

This is a project-specific choice, not a universal claim that monorepos are superior. Several independently maintained teams, release cadences or access restrictions can justify a later split. Use an ADR and an explicit versioned conformance release if that happens.

## Dependency direction

Consumer → native SDK facade → internal lifecycle logic + backend adapter → established backend client.

Adapters depend on a small core package; the core never imports a Redis/etcd/NATS client. Test harnesses depend on packaged SDKs; SDKs never depend on the harness. Do not generate public SDK APIs from JSON schemas. Generation is appropriate for official etcd protobuf transport or golden data, not for forcing a lowest-common-denominator public API.

Common server scripts and protocol byte formats have one canonical source. Build-time inclusion is acceptable and must be reproducible; runtime downloads are not.

## Package boundaries

.NET: `Dilock.Core`, `Dilock.Redis`, `Dilock.Etcd`; add `Dilock.Extensions.DependencyInjection` only after its API earns its maintenance cost. Python: distributions `dilock`, `dilock-redis`, `dilock-etcd`; import packages `dilock`, `dilock_redis`, `dilock_etcd` to avoid implicit namespace-package collisions. C++: one source release/recipe with optional components and exported targets `dilock::core`, `dilock::redis`, `dilock::etcd`. A C++ “core-only” build must not discover or download other backend dependencies.

Do not create a package per class. Do not publish a metapackage that silently installs all backends. A package README must work on its package registry, where repository-relative links may resolve differently.

## 2026-10-03 scope extension

The initial SDK scope covers .NET, C++, Python, TypeScript, Go, Rust and Java. SDK and backend support statements must remain specific to the tested profile. Tests stay in this monorepo. Native language examples in `examples/style/` are not distributed SDK tests and never qualify a release.
