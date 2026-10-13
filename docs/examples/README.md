# Distributed lock examples

Each implemented language/backend pair has a consistent quickstart at
`<language>-<backend>/quickstart/`. The directory contains a language-native
`main.*` source file and a README entry point; it links to the local build/run
instructions, metadata requirements, and backend limits for that sample. The
seven original SDKs also retain their deeper consumer examples in the parent
directories.

| Language / ecosystem | Redis quickstart | etcd quickstart |
|---|---|---|
| C# / .NET | [Code and instructions](dotnet-redis/quickstart/README.md) | [Code and instructions](dotnet-etcd/quickstart/README.md) |
| C++ | [Code and instructions](cpp-redis/quickstart/README.md) | [Code and instructions](cpp-etcd/quickstart/README.md) |
| Python | [Code and instructions](python-redis/quickstart/README.md) | [Code and instructions](python-etcd/quickstart/README.md) |
| TypeScript / Node.js | [Code and instructions](typescript-redis/quickstart/README.md) | [Code and instructions](typescript-etcd/quickstart/README.md) |
| Go | [Code and instructions](go-redis/quickstart/README.md) | [Code and instructions](go-etcd/quickstart/README.md) |
| Rust | [Code and instructions](rust-redis/quickstart/README.md) | [Code and instructions](rust-etcd/quickstart/README.md) |
| Java | [Code and instructions](java-redis/quickstart/README.md) | [Code and instructions](java-etcd/quickstart/README.md) |
| Dart | [Code and instructions](dart-redis/quickstart/README.md) | [Code and instructions](dart-etcd/quickstart/README.md) |
| Elixir | [Code and instructions](elixir-redis/quickstart/README.md) | [Code and instructions](elixir-etcd/quickstart/README.md) |
| PHP | [Code and instructions](php-redis/quickstart/README.md) | [Code and instructions](php-etcd/quickstart/README.md) |
| Ruby | [Code and instructions](ruby-redis/quickstart/README.md) | [Code and instructions](ruby-etcd/quickstart/README.md) |
| Swift | [Code and instructions](swift-redis/quickstart/README.md) | [Code and instructions](swift-etcd/quickstart/README.md) |
| OCaml | [Code and instructions](ocaml-redis/quickstart/README.md) | [Code and instructions](ocaml-etcd/quickstart/README.md) |
| Perl | [Code and instructions](perl-redis/quickstart/README.md) | [Code and instructions](perl-etcd/quickstart/README.md) |
| Julia | [Code and instructions](julia-redis/quickstart/README.md) | [Code and instructions](julia-etcd/quickstart/README.md) |
| R | [Code and instructions](r-redis/quickstart/README.md) | [Code and instructions](r-etcd/quickstart/README.md) |
| JavaScript | [Code and instructions](javascript-redis/quickstart/README.md) | [Code and instructions](javascript-etcd/quickstart/README.md) |
| Kotlin | [Code and instructions](kotlin-redis/quickstart/README.md) | [Code and instructions](kotlin-etcd/quickstart/README.md) |
| Scala | [Code and instructions](scala-redis/quickstart/README.md) | [Code and instructions](scala-etcd/quickstart/README.md) |
| Groovy | [Code and instructions](groovy-redis/quickstart/README.md) | [Code and instructions](groovy-etcd/quickstart/README.md) |
| Clojure | [Code and instructions](clojure-redis/quickstart/README.md) | [Code and instructions](clojure-etcd/quickstart/README.md) |
| F# | [Code and instructions](fsharp-redis/quickstart/README.md) | [Code and instructions](fsharp-etcd/quickstart/README.md) |
| Visual Basic .NET | [Code and instructions](vb-redis/quickstart/README.md) | [Code and instructions](vb-etcd/quickstart/README.md) |
| PowerShell | [Code and instructions](powershell-redis/quickstart/README.md) | [Code and instructions](powershell-etcd/quickstart/README.md) |
| Erlang | [Code and instructions](erlang-redis/quickstart/README.md) | [Code and instructions](erlang-etcd/quickstart/README.md) |
| C | [Code and instructions](c-redis/quickstart/README.md) | [Code and instructions](c-etcd/quickstart/README.md) |
| Objective-C | Not implemented; no SDK source or quickstart is available. | Not implemented; no SDK source or quickstart is available. |
| Nim | [Code and instructions](nim-redis/quickstart/README.md) | [Code and instructions](nim-etcd/quickstart/README.md) |
| D | [Code and instructions](d-redis/quickstart/README.md) | [Code and instructions](d-etcd/quickstart/README.md) |
| Crystal | [Code and instructions](crystal-redis/quickstart/README.md) | [Code and instructions](crystal-etcd/quickstart/README.md) |
| Fortran | [Code and instructions](fortran-redis/quickstart/README.md) | [Code and instructions](fortran-etcd/quickstart/README.md) |
| Zig | [Code and instructions](zig-redis/quickstart/README.md) | [Code and instructions](zig-etcd/quickstart/README.md) |
| Haskell | [Code and instructions](haskell-redis/quickstart/README.md) | [Code and instructions](haskell-etcd/quickstart/README.md) |
| Lua / LuaJIT | [Code and instructions](lua-redis/quickstart/README.md) | [Code and instructions](lua-etcd/quickstart/README.md) |

The default path in each sample favors simple initialization. Where the SDK
supports it, the same quickstart also shows explicit timing/transport settings
and reuse of one provider for multiple independent resource handles. Reusing a
provider does not make multiple locks atomic. Some entries are facades over a
shared backend engine; each quickstart identifies that relationship rather
than implying a duplicate implementation.

All examples require namespace metadata to be provisioned before connecting;
the SDKs do not create or repair it as a side effect. They are local examples,
not production-readiness evidence. Backend profiles have different guarantees:
Redis does not provide strong fencing across state-losing failover, and etcd
fences must be enforced atomically by the protected resource. Lease expiry does
not stop stale application work.

The small [`examples/style`](../../examples/style/README.md) programs are only
language-idiom references and are not used as distributed-lock examples.
