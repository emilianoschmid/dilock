# ADR 0010: Split the TypeScript SDK into optional npm packages

## Status

Accepted for local distribution candidates. Package scope ownership and publication remain unverified and unauthorized.

## Context

The TypeScript SDK previously shipped one package that required both node-redis and gRPC clients even when a consumer used only one backend or only the shared identity/error API. The package matrix requires backend adapters to be optional distribution units. Core identity and error types are shared by both adapters and must remain the same runtime classes across imports.

## Decision

Keep one npm workspace under `sdk/typescript` with three independently packed workspace packages:

- `@dilock/core` contains identity helpers and shared error classes and declares no runtime dependency on either backend.
- `@dilock/redis` depends on `@dilock/core` and node-redis only.
- `@dilock/etcd` depends on `@dilock/core`, grpc-js and proto-loader only, and includes the pinned etcd protocol subset and license notices.

All three candidate manifests remain `private: true`. The workspace root is a private development/test runner, not a distributable facade. Consumers import Redis APIs from `@dilock/redis`, etcd APIs from `@dilock/etcd`, and backend-independent helpers from `@dilock/core`. Adapter entry points re-export core API symbols for convenience while package manager deduplication preserves the shared core dependency.

## Consequences

- Core-only consumers do not install Redis or gRPC libraries.
- Redis and etcd consumer closures are separately installable from their exact local tarballs.
- A consumer that imports a backend must add both the backend package and its transitive core package through normal dependency resolution.
- The former `@dilock/typescript` monolithic import is replaced by package-specific imports; examples and consumer fixtures use those package names.
- `@dilock` names are local candidates only. Before any publication, verify scope ownership and release configuration, then test and publish the exact approved archives.
- This is a distribution and import-path decision only. It does not change protocol, lock ownership, identity vectors or backend safety semantics.
