# Human documentation

This index contains project documentation intended for users and contributors. Workspace prompts, agent instructions, implementation diaries, release evidence and local build artifacts are maintained separately and are not part of the public documentation set.

## Project and support

- [Project overview](../../README.md)
- [Support policy](../../SUPPORT.md)
- [Security policy](../../SECURITY.md)
- [Code of conduct](../../CODE_OF_CONDUCT.md)
- [Contribution guide](../../CONTRIBUTING.md)
- [Governance](../../GOVERNANCE.md)

## Contract and design

- [Lock semantics](../../spec/SEMANTICS.md)
- [Resource identity](../../spec/IDENTITY.md)
- [Namespace metadata](../../spec/NAMESPACE-METADATA.md)
- [Redis profile](../../spec/backends/REDIS.md)
- [etcd profile](../../spec/backends/ETCD.md)
- [NATS experimental profile](../../spec/backends/NATS-SPIKE.md)
- [Language API design guidance](../api/LANGUAGE-DESIGN.md)
- [Repository architecture](../architecture/REPOSITORY.md)
- [Backend guarantees and limitations](../architecture/BACKENDS.md)
- [Architecture decision records](../adr/)

## Engineering references

- [Testing and conformance](../engineering/TESTING.md)
- [Build and CI](../engineering/BUILD-CI.md)
- [Package and release policy](../engineering/PACKAGE-MATRIX.md)
- [Security and observability](../engineering/OBSERVABILITY-SECURITY.md)
- [Style examples](../../examples/style/README.md)
- [Distributed Redis and etcd examples](../examples/README.md)

## SDK documentation

Each SDK directory contains its README, installation/build instructions, backend-specific usage, limitations, and links to its native API documentation where available. SDK support is experimental and varies by language/backend; this index makes no uniform support or production-readiness claim.
