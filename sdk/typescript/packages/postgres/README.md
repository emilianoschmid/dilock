# `@dilock/postgres`

PostgreSQL advisory-lock backend for the Dilock TypeScript SDK.

The `postgres-advisory-session-v1` profile uses one private physical PostgreSQL
connection for each acquisition. Acquisition calls `pg_try_advisory_lock(bigint)`
and release calls `pg_advisory_unlock(bigint)` on that same connection. PostgreSQL
does not provide a server TTL for this profile: `renew()` is only a bounded
liveness check, and fencing plus automatic renewal are rejected.

Provision a namespace explicitly with `provisionNamespace(connectionString,
namespace, epoch)` before calling `PostgresProvider.connect()`.
