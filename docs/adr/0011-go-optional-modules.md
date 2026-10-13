# ADR 0011: Split Go core and backend modules

Status: accepted for the local SDK workspace; module identities remain candidates.
Date: 2026-10-04

## Context

The Go SDK had one combined module with the invalid external path `dilock`. Core, Redis and etcd were coupled in one dependency graph, and the conformance worker lived in the same module. The verified repository owner is unavailable, so choosing a public import path would invent identity.

## Decision

Use separate module roots for core (`example.invalid/dilock/core`), Redis (`example.invalid/dilock/redis`) and etcd (`example.invalid/dilock/etcd`), at the local candidate dependency version `v0.1.0`. Core has no backend dependency. Redis owns go-redis only; etcd owns its etcd and gRPC dependencies. The conformance worker is a local tool module depending on all three SDK modules.

The SDK-local `go.work` lists these modules and uses versioned local replacements so repository development and workers resolve without a registry. External consumer fixtures also use explicit local replacements. The placeholder domain is intentionally not a public package identity. It must be replaced with owner-qualified module paths and matching subdirectory tags after ownership verification; no remote install or publication is authorized by this ADR.

Exported package names, constructors, types, and wire behavior remain unchanged. Import paths necessarily change from the prior invalid local `dilock` path; external consumer validation against the final identity remains blocked.

## Consequences

Each adapter can be selected independently and core avoids backend client packages. Running tests requires testing each workspace module root, because `go test ./...` from the core module root covers only core. The worker stays out of the public package set. Isolated Redis and core consumers passed offline; etcd and combined consumers remain blocked where the local cache lacks required transitive module metadata. No production-readiness or publication claim follows from these local results.
