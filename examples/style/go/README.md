# Go — local design example

**Not a distributed lock or publishable SDK.** This is a small executable implementation showing native API style and local acquisition ownership.

## Design

context.Context first; small consumer-side interfaces; structs and constructors; errors.Is/As; explicit release with separate cleanup context.

Provider/handle/lease are responsibilities, not a mandatory identical class hierarchy. The memory fixture demonstrates ordinary contention, owner-checked release, stale-owner protection and repeated release. Where idiomatic, a scope also demonstrates cleanup when consumer code throws. Test-only expiry is a hook, not a proposed force-unlock feature.

## Run

From this directory, using an already installed toolchain:

```shell
go test -race -count=1 ./...
go vet ./...
```

The shared runner is `python tools/run_style_examples.py go` from the repository root. It isolates build outputs and records actual results. It does not install toolchains. A missing tool is skipped, not passed. The Scala runner uses offline mode and needs the compiler cached already.

## Deliberate omissions

No remote backend, TTL, renewal, wait budgets, full resource-identity encoding, namespace metadata, transport errors, outcome-unknown recovery, fencing or cross-process interoperability. Local object/reference/UUID identities are NOT the production wire format. Resource validation here is intentionally minimal; the full contract is in `../../../spec/IDENTITY.md`.

A synchronous local release is not evidence that a destructor can perform safe asynchronous network I/O. Every production adapter must implement the full lifecycle contract, tracked bounded cleanup and failure tests. The C fixture supports one resource and is single-threaded; other fixture concurrency constraints are in source comments. None is a general-purpose in-memory SDK.

## Publication target after implementation

Go modules under the verified GitHub owner; core and independent backend modules. No package has been published or name reserved by this bundle.

Primary design reference: https://go.dev/doc/effective_go
