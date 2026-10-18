"""Provision or verify PostgreSQL draft-1 namespace metadata."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def provision_postgres(dsn: str, namespace: str, epoch: str) -> str:
    """Create the metadata table/row only when absent; never overwrite a row."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python" / "src"))
    from dilock.core import identity

    identity(namespace, "validation")
    if not isinstance(epoch, str) or not epoch or "\0" in epoch:
        raise ValueError("epoch must be nonempty and must not contain NUL")
    if len(epoch.encode("utf-8", "strict")) > 1024:
        raise ValueError("epoch must be at most 1024 UTF-8 bytes")
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("install the Python 'postgres' extra before provisioning") from exc

    metadata = (1, "sha256-framed-utf8-v1", "postgres-advisory-session-v1", epoch, None)
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            # Serialize first-time DDL across language routes. PostgreSQL can
            # still race CREATE TABLE IF NOT EXISTS in the system catalog.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("dilock:namespace-meta:v1",))
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dilock_namespace_meta (
                    namespace text PRIMARY KEY,
                    protocol_major integer NOT NULL,
                    identity_scheme text NOT NULL,
                    profile text NOT NULL,
                    coordination_epoch text NOT NULL,
                    fixed_lease_ms bigint NULL
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO dilock_namespace_meta
                    (namespace, protocol_major, identity_scheme, profile,
                     coordination_epoch, fixed_lease_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (namespace) DO NOTHING
                """,
                (namespace, *metadata),
            )
            cursor.execute(
                """
                SELECT protocol_major, identity_scheme, profile,
                       coordination_epoch, fixed_lease_ms
                FROM dilock_namespace_meta
                WHERE namespace = %s
                """,
                (namespace,),
            )
            current = cursor.fetchone()
            if current != metadata:
                raise RuntimeError("existing PostgreSQL namespace metadata conflicts; row was not modified")
    return "created or compatible existing metadata"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("DILOCK_POSTGRES_DSN", ""))
    parser.add_argument("--namespace", default=os.environ.get("DILOCK_NAMESPACE", "default"))
    parser.add_argument("--epoch", default=os.environ.get("DILOCK_COORDINATION_EPOCH", ""))
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DILOCK_POSTGRES_DSN is required")
    if not args.epoch:
        parser.error("--epoch or DILOCK_COORDINATION_EPOCH is required")
    print(provision_postgres(args.dsn, args.namespace, args.epoch))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
