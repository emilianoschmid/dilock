# Dilock.Postgres

Experimental PostgreSQL `postgres-advisory-session-v1` provider for Dilock.

Each acquisition owns one physical PostgreSQL session. The provider disables
Npgsql pooling, uses `pg_try_advisory_lock(bigint)` for bounded contention
polling, and releases only with `pg_advisory_unlock(bigint)` on the same
connection. PostgreSQL advisory locks have no server TTL or generic fencing;
`LeaseOptions.RequireFencing` and `LeaseOptions.AutoRenew` are rejected before
backend I/O, and `RenewAsync` is only a session liveness check.

The operator must provision and verify `dilock_namespace_meta` with
`tools/provision_postgres.py` before connecting. Runtime clients never create,
repair or overwrite that metadata.
