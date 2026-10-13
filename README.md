# Dilock

Distributed locks with native APIs and explicit guarantees.

**Status: experimental and under active development; not production-ready.** This repository contains native SDK implementations for multiple languages and Redis/etcd adapters, but the project has no published release. Support varies by SDK/backend; do not infer production readiness, failover guarantees, or package availability from the presence of source code.

Dilock is being built to coordinate cooperating services written in different languages using infrastructure they already run. The shared contract specifies resource identity, ownership, lifecycle and failure outcomes; each SDK exposes that contract in its language's native style. Backend adapters are optional dependencies, not a mandatory stack.

## Documentation

This README is the human-facing project entry point. The [documentation index](docs/humans/README.md) links to shared specifications, architecture and engineering references. Workspace-only agent instructions and project notes are kept outside the public documentation set.

## Start here

Start with [semantics](spec/SEMANTICS.md), [language API guidance](docs/api/LANGUAGE-DESIGN.md), and [backend profiles](docs/architecture/BACKENDS.md). The [small style examples](examples/style/README.md) illustrate idioms only; the [distributed Redis and etcd examples](docs/examples/README.md) exercise the SDK backends. A separate [Redis command-boundary example](examples/redis_python/README.md) demonstrates owner-checked commands; it is not a distributed SDK implementation.

## Initial SDK scope

The initial implementation scope is C#/.NET, C++, Python, TypeScript/Node.js, Go, Rust and Java, with Redis and etcd adapters. Each SDK/backend pair has its own maturity and limitations; source presence alone is not a support claim.

Python Redis install from this checkout: `python -m pip install 'sdk/python[redis]'`. Use only after explicitly provisioning matching namespace metadata. Style samples under `examples/style/` demonstrate syntax and lifetime; they do not implement distributed locks. Publication identifiers remain unverified.

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
