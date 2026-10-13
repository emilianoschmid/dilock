# Native API design cards

These decisions concern consumer design, not a common class hierarchy. `spec/SEMANTICS.md` stays authoritative for distributed behavior. The tiny style examples deliberately omit network, TTL and renewal. Their local ownership IDs are not wire tokens.

For every language record: thread/task/loop safety, provider ownership, lifetime of connections, uncertainty outcomes, cancellation, independent cleanup deadline, lease loss, package boundaries and version policy. Public API docs must include invalid/ambiguous cases, not only a success snippet.

## C# / .NET

**Design:** Small interfaces; sealed implementations; Task; CancellationToken; IAsyncDisposable; immutable options.

**Build/test:** dotnet build/test/pack; XML docs; API compatibility; nullable/analyzers.

**Publication:** NuGet: Dilock.Core, Dilock.Redis, Dilock.Etcd. Identifiers require owner verification.

**Small implementation:** `examples/style/dotnet/`.

**Primary reference:** https://learn.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-disposeasync

## C++

**Design:** Value types; move-only RAII lease; std::chrono; std::stop_token; optional Asio integration, no invented coroutine runtime.

**Build/test:** CMake/CTest; GCC/Clang/MSVC; ASan/UBSan; separate TSan jobs; installed consumer.

**Publication:** CMake install/export; Conan 2 recipe; vcpkg overlay, later upstream submission. Identifiers require owner verification.

**Small implementation:** `examples/style/cpp/`.

**Primary reference:** https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines

## Python

**Design:** asyncio; structural Protocol; async context manager; dataclasses for options; native exceptions; py.typed.

**Build/test:** pytest; type checker; ruff; wheel/sdist; clean-venv consumer.

**Publication:** PyPI: dilock, dilock-redis, dilock-etcd. Identifiers require owner verification.

**Small implementation:** `examples/style/python/`.

**Primary reference:** https://docs.python.org/3/library/contextlib.html

## TypeScript / Node.js

**Design:** Promise; AbortSignal; discriminated unions; explicit awaited release/try-finally; optional AsyncDisposable after runtime checks.

**Build/test:** tsc strict; node tests; npm pack; ESM consumer; CJS only if explicitly supported.

**Publication:** npm: @<verified-scope>/core, /redis, /etcd; proposed scope dilock is unreserved. Identifiers require owner verification.

**Small implementation:** `examples/style/typescript/`.

**Primary reference:** https://www.typescriptlang.org/docs/handbook/release-notes/typescript-5-2.html

## Go

**Design:** context.Context first; small consumer-side interfaces; structs and constructors; errors.Is/As; explicit release with separate cleanup context.

**Build/test:** go test ./...; -race; go vet; gofmt; module consumers and correct subdirectory tags.

**Publication:** Go modules under the verified GitHub owner; core and independent backend modules. Identifiers require owner verification.

**Small implementation:** `examples/style/go/`.

**Primary reference:** https://go.dev/doc/effective_go

## Rust

**Design:** Non-Clone lease; Result<Option<Lease>, Error>; ownership; traits where useful; explicit async release; Drop never blocks or pretends to await I/O.

**Build/test:** cargo fmt/clippy/test/doc/package; MSRV; features matrix; doctests.

**Publication:** crates.io: dilock, dilock-redis, dilock-etcd (proposed names). Identifiers require owner verification.

**Small implementation:** `examples/style/rust/`.

**Primary reference:** https://doc.rust-lang.org/book/ch15-03-drop.html

## Java

**Design:** Small interfaces; immutable records/options; AutoCloseable for bounded synchronous cleanup; Optional for busy; typed errors; interruption.

**Build/test:** Gradle or Maven wrapper; JUnit; javadoc; nullness checks; installed consumer.

**Publication:** Maven Central: verified groupId, dilock-core/redis/etcd; source and javadoc jars. Identifiers require owner verification.

**Small implementation:** `examples/style/java/`.

**Primary reference:** https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/AutoCloseable.html

## Kotlin

**Design:** Thin Kotlin/JVM facade over Java; suspend/withLease and structured cancellation; use for a genuinely synchronous AutoCloseable scope.

**Build/test:** Gradle Kotlin DSL; coroutine cancellation tests; Dokka; Java/Kotlin interop.

**Publication:** Maven Central: dilock-kotlin; JVM first, not a claim of Kotlin Multiplatform. Identifiers require owner verification.

**Small implementation:** `examples/style/kotlin/`.

**Primary reference:** https://kotlinlang.org/docs/cancellation-and-timeouts.html

## PHP

**Design:** Strict types; final classes; narrow interfaces; enum outcomes; try/finally or withLease; no destructor network operations.

**Build/test:** Composer; PHPUnit; PHPStan/Psalm; coding-style checks; installed consumer.

**Publication:** Composer / Packagist: <verified-vendor>/dilock and backend packages. Identifiers require owner verification.

**Small implementation:** `examples/style/php/`.

**Primary reference:** https://www.php.net/manual/en/language.exceptions.php

## Ruby

**Design:** Small duck-typed objects; block API; ensure cleanup; Ruby-style exceptions; fiber/thread ownership explicit.

**Build/test:** Bundler; tests; RuboCop; YARD/RBS as appropriate; gem build.

**Publication:** RubyGems: dilock plus optional adapter gems. Identifiers require owner verification.

**Small implementation:** `examples/style/ruby/`.

**Primary reference:** https://docs.ruby-lang.org/en/master/syntax/exceptions_rdoc.html

## Swift

**Design:** async throws; Sendable; actors only for genuinely shared mutable state; explicit awaited release/withLease; no async work in deinit.

**Build/test:** swift test; strict concurrency; DocC; Linux/macOS consumer.

**Publication:** Swift Package Manager via repository tags/subpackage strategy; optional adapter products. Identifiers require owner verification.

**Small implementation:** `examples/style/swift/`.

**Primary reference:** https://www.swift.org/documentation/api-design-guidelines/

## Dart

**Design:** Future; Duration; final classes; null safety; explicit release in finally; documented cancellation protocol.

**Build/test:** dart analyze/test/doc; publish --dry-run; package consumer.

**Publication:** pub.dev: dilock, dilock_redis, dilock_etcd; server/CLI first. Identifiers require owner verification.

**Small implementation:** `examples/style/dart/`.

**Primary reference:** https://dart.dev/language/async

## Elixir

**Design:** Modules, behaviours, tagged tuples; supervised lifecycle; with_lease/try-after; process monitoring; no OO hierarchy translation.

**Build/test:** mix format/test; ExUnit; Dialyzer as appropriate; ExDoc; hex build.

**Publication:** Hex.pm: dilock and optional adapters. Identifiers require owner verification.

**Small implementation:** `examples/style/elixir/`.

**Primary reference:** https://hexdocs.pm/elixir/GenServer.html

## Scala

**Design:** Reuse Java engine; Scala 3 Option/Either; scope helper; optional Cats Effect Resource or ZIO Scope integrations, not mandatory dependencies.

**Build/test:** sbt or shared Gradle build; Scala tests; cross-version consumer; Scaladoc.

**Publication:** Maven Central: Scala-binary-versioned facade; core Java artifact remains shared. Identifiers require owner verification.

**Small implementation:** `examples/style/scala/`.

**Primary reference:** https://docs.scala-lang.org/scala3/book/interacting-with-java.html

## C

**Design:** Opaque handles; status enums; explicit resource ownership; acquire/release/destroy; ABI version and allocator rules.

**Build/test:** C17 baseline with optional C23; warnings; sanitizers; C ABI consumer.

**Publication:** CMake/pkg-config; Conan/vcpkg; release source, selective native binaries. Identifiers require owner verification.

**Small implementation:** `examples/style/c/`.

**Primary reference:** https://www.open-std.org/jtc1/sc22/wg14/


## Hard cases the implementation must not paper over

- Rust `Drop`, C++ destructors and Swift `deinit` are not an awaitable network acknowledgement. A local demonstration can release synchronously; a remote implementation must use explicit bounded async release and/or a tracked best-effort scheduler. No detached untracked cleanup.
- Go `defer` should use a fresh bounded cleanup context, not an acquisition context already cancelled. A lease contains synchronization state and must not be copied after use.
- Kotlin `use` is valid for the synchronous local sample, not as a substitute for a suspending close. The production coroutine helper needs bounded cleanup in an appropriate non-cancellable cleanup region without swallowing the original cancellation.
- TypeScript `using`/`await using` requires both emitted syntax support and runtime disposal symbols; do not assume compiling TypeScript installs them. Explicit `try/finally` is the portable baseline.
- Java synchronous AutoCloseable can be bounded; asynchronous Java acquisition needs a separate helper or closeAsync contract. Do not invoke blocking cleanup on an event-loop thread.
- Python shielding alone does not keep a detached cleanup task tracked or guarantee completion. Design ownership/cleanup task tracking explicitly.
- PHP destructors, Ruby finalizers, GC finalization and JavaScript FinalizationRegistry must not be the primary lock-release mechanism.
- Swift actors serialize isolated state but are reentrant across `await`: terminal transitions happen before suspension and late results cannot resurrect a released lease.
- Elixir process supervision is lifecycle management, not cross-language consensus. Do not substitute BEAM global locks for the agreed Redis/etcd protocol.
- C has real ownership rules, not Java-style interfaces. Explicit destroy consumes a handle; duplicate release before destroy is supported, use-after-destroy is invalid. No exception or allocator ownership crosses an ABI boundary accidentally.
