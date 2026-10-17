# ADR 0010: Use Consul's HTTP KV session lock profile

## Status

Accepted as the implementation plan; runtime support is not implied.

## Context

The repository catalogs 33 implemented language ecosystems or facades, with
direct adapters and shared engines. A new backend should not require a Consul
SDK in every language. Consul exposes sessions and KV lock operations through
a common HTTP API. Its native session TTL minimum is 10 seconds, its locks are
advisory, and its per-key `LockIndex` can be reset if a key is deleted and
recreated. The project must preserve the shared lease contract and state those
limits instead of silently promising Redis/etcd equivalence.

## Decision

Implement `consul-kv-session-v1` over the Consul HTTP API. Use one TTL session
per acquisition attempt, `Behavior=release`, zero `LockDelay`, explicit session
renewal, and KV acquire/release. Verify ownership and `LockIndex` with
linearizable KV reads. Preserve lock entries permanently within a coordinator
epoch; never delete them from SDK cleanup. Report fencing only under the
retained-key and approved-epoch conditions in `spec/backends/CONSUL.md`.
Reject unsupported TTLs before sending requests. Do not add automatic fallback
among Consul, Redis, and etcd.

## Implementation plan

1. Land the normative profile, identity paths, and this ADR; define metadata,
   ACL, TTL, loss, cleanup, fencing, and failure semantics.
2. Build Python as the reference HTTP adapter, with strict endpoint handling,
   no hidden retries, explicit provisioning, and unit tests against a scripted
   HTTP transport.
3. Add a version-pinned disposable Consul service and live integration tests;
   verify contention, renewal, stale-owner cleanup, expiry, and fence behavior.
4. Add Consul to the native developer dispatcher, package metadata, worker
   configuration, and cross-language conformance catalog.
5. Port the profile through shared backend engines first (.NET, Java, Elixir,
   TypeScript and C++/C ABI), then direct SDKs and language facades. Each SDK
   reports capabilities and runs its own adapter tests.
6. Execute the Consul matrix for every implemented SDK route, including
   installed-package consumers where available. Keep unbuilt/unavailable routes
   explicitly unverified; do not claim ecosystem-wide support from the Python
   reference implementation.

## Consequences

- HTTP makes transport available in every language without requiring a Consul
  client library.
- Leases need at least 10 seconds; Consul's session cleanup can delay a
  successor after failure, so availability differs from Redis and etcd.
- Fencing is per resource and depends on retaining Consul KV entries. Broader
  Consul ACLs or manual key deletion can violate that assumption.
- Consul KV is documented as feature complete; maintenance and API stability
  are acceptable for this bounded adapter, but new KV feature development is
  not expected.


## Progress in this checkout

- Completed: normative profile, identity paths, ADR, Python reference adapter,
  scripted HTTP contract tests, provisioning helper, pinned disposable Compose
  service, live integration driver, and Python quickstart.
- Pending: Consul worker and cross-language conformance catalog, ports through
  shared .NET/Java/Elixir/TypeScript/C ABI engines, direct SDKs and facades, and
  the full implemented-route matrix. Current evidence supports only the Python
  draft adapter against the pinned single-node test service.
