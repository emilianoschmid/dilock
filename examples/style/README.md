# Small native implementations

Fifteen local-only examples demonstrate API shape and acquisition lifetime. They contain executable checks, not just pseudocode. They are intentionally NOT network/distributed implementations or publishable SDKs.

Each example has a README, source and a command to run it. The shared runner uses installed tools and reports pass/fail/skipped separately:

```shell
python tools/run_style_examples.py --all
python tools/run_style_examples.py python cpp go --strict
```

Build outputs are temporary; evidence is written to `validation/style-results.json`. The runner is not a production build orchestrator. No third-party toolchain is downloaded. Install compilers independently after reviewing their provenance.

| Sample | Native design illustrated |
|---|---|
| [C# / .NET](dotnet/README.md) | Small interfaces; sealed implementations; Task; CancellationToken; IAsyncDisposable; immutable options. |
| [C++](cpp/README.md) | Value types; move-only RAII lease; std::chrono; std::stop_token; optional Asio integration, no invented coroutine runtime. |
| [Python](python/README.md) | asyncio; structural Protocol; async context manager; dataclasses for options; native exceptions; py.typed. |
| [TypeScript / Node.js](typescript/README.md) | Promise; AbortSignal; discriminated unions; explicit awaited release/try-finally; optional AsyncDisposable after runtime checks. |
| [Go](go/README.md) | context.Context first; small consumer-side interfaces; structs and constructors; errors.Is/As; explicit release with separate cleanup context. |
| [Rust](rust/README.md) | Non-Clone lease; Result<Option<Lease>, Error>; ownership; traits where useful; explicit async release; Drop never blocks or pretends to await I/O. |
| [Java](java/README.md) | Small interfaces; immutable records/options; AutoCloseable for bounded synchronous cleanup; Optional for busy; typed errors; interruption. |
| [Kotlin](kotlin/README.md) | Thin Kotlin/JVM facade over Java; suspend/withLease and structured cancellation; use for a genuinely synchronous AutoCloseable scope. |
| [PHP](php/README.md) | Strict types; final classes; narrow interfaces; enum outcomes; try/finally or withLease; no destructor network operations. |
| [Ruby](ruby/README.md) | Small duck-typed objects; block API; ensure cleanup; Ruby-style exceptions; fiber/thread ownership explicit. |
| [Swift](swift/README.md) | async throws; Sendable; actors only for genuinely shared mutable state; explicit awaited release/withLease; no async work in deinit. |
| [Dart](dart/README.md) | Future; Duration; final classes; null safety; explicit release in finally; documented cancellation protocol. |
| [Elixir](elixir/README.md) | Modules, behaviours, tagged tuples; supervised lifecycle; with_lease/try-after; process monitoring; no OO hierarchy translation. |
| [Scala](scala/README.md) | Reuse Java engine; Scala 3 Option/Either; scope helper; optional Cats Effect Resource or ZIO Scope integrations, not mandatory dependencies. |
| [C](c/README.md) | Opaque handles; status enums; explicit resource ownership; acquire/release/destroy; ABI version and allocator rules. |

## How to use these samples

Read them before designing a production SDK, not instead of the semantic contract. Implement real clients and renewals in `sdk/`. Do not serialize these local owner identities; the actual protocol uses independently generated CSPRNG values and canonical identity vectors.

Tests here demonstrate local contention, stale-release rejection and lifetime idioms only. They do not prove network atomicity, process failure recovery, TTL semantics or language interoperability. All distributed support cells remain planned until separate real-backend conformance runs pass.
