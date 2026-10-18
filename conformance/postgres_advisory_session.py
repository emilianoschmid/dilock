"""Run the PostgreSQL advisory-session profile against one disposable server."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


def run(dsn: str, namespace: str, epoch: str) -> dict[str, object]:
    import psycopg

    from tools.provision_postgres import provision_postgres
    from dilock.core import (
        AcquireOptions,
        BackendUnavailable,
        Options,
        ProtocolMismatch,
        UnsupportedCapability,
        WaitTimeout,
        identity,
    )
    from dilock.postgres import PostgresProvider, advisory_key

    provision_postgres(dsn, namespace, epoch)
    checks: list[str] = []

    def variant_namespace(suffix: str) -> str:
        room = 63 - len(suffix) - 1
        return f"{namespace[:room]}-{suffix}"

    conflicting_epoch = f"{epoch}-conflict"
    try:
        provision_postgres(dsn, namespace, conflicting_epoch)
    except RuntimeError:
        checks.append("metadata-immutable-on-conflict")
    else:
        raise AssertionError("metadata provisioning overwrote a compatible namespace row")

    with psycopg.connect(dsn, autocommit=True) as connection:
        version = connection.execute("SELECT version()").fetchone()[0]
        mismatch_namespace = variant_namespace("mismatch")
        connection.execute(
            """
            INSERT INTO dilock_namespace_meta
                (namespace, protocol_major, identity_scheme, profile,
                 coordination_epoch, fixed_lease_ms)
            VALUES (%s, 1, 'sha256-framed-utf8-v1',
                    'postgres-advisory-session-v1', 'wrong-epoch', NULL)
            ON CONFLICT (namespace) DO UPDATE
                SET coordination_epoch = EXCLUDED.coordination_epoch
            """,
            (mismatch_namespace,),
        )

    async def expect_metadata_error(candidate_namespace: str, candidate_epoch: str) -> None:
        candidate = PostgresProvider(dsn, candidate_namespace, candidate_epoch)
        try:
            try:
                await candidate.lock("metadata-validation").try_acquire()
            except ProtocolMismatch:
                return
            raise AssertionError("PostgreSQL client accepted invalid namespace metadata")
        finally:
            await candidate.aclose()

    async def validate_metadata_contract() -> None:
        await expect_metadata_error(variant_namespace("missing"), epoch)
        await expect_metadata_error(variant_namespace("mismatch"), epoch)

    asyncio.run(validate_metadata_contract())
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            "DELETE FROM dilock_namespace_meta WHERE namespace = %s",
            (variant_namespace("mismatch"),),
        )
    checks.extend(["metadata-missing-rejected", "metadata-mismatch-rejected"])

    with psycopg.connect(dsn, autocommit=True) as first, psycopg.connect(dsn, autocommit=True) as second:
        shared_key = advisory_key(identity(namespace, "shared-resource"))
        other_key = advisory_key(identity(namespace, "different-resource"))
        assert first.execute("SELECT pg_try_advisory_lock(%s::bigint)", (shared_key,)).fetchone()[0] is True
        assert second.execute("SELECT pg_try_advisory_lock(%s::bigint)", (shared_key,)).fetchone()[0] is False
        assert second.execute("SELECT pg_try_advisory_lock(%s::bigint)", (other_key,)).fetchone()[0] is True
        assert second.execute("SELECT pg_advisory_unlock(%s::bigint)", (other_key,)).fetchone()[0] is True
        assert first.execute("SELECT pg_advisory_unlock(%s::bigint)", (shared_key,)).fetchone()[0] is True
        assert second.execute("SELECT pg_try_advisory_lock(%s::bigint)", (shared_key,)).fetchone()[0] is True
        assert second.execute("SELECT pg_advisory_unlock(%s::bigint)", (shared_key,)).fetchone()[0] is True
        checks.extend(["same-resource-contention", "different-resource-progress", "same-session-release-handoff"])

    with psycopg.connect(dsn, autocommit=True) as old_session, psycopg.connect(dsn, autocommit=True) as successor:
        key = advisory_key(identity(namespace, "old-session-release"))
        assert old_session.execute("SELECT pg_try_advisory_lock(%s::bigint)", (key,)).fetchone()[0] is True
        assert old_session.execute("SELECT pg_advisory_unlock(%s::bigint)", (key,)).fetchone()[0] is True
        assert successor.execute("SELECT pg_try_advisory_lock(%s::bigint)", (key,)).fetchone()[0] is True
        assert old_session.execute("SELECT pg_advisory_unlock(%s::bigint)", (key,)).fetchone()[0] is False
        assert successor.execute("SELECT pg_advisory_unlock(%s::bigint)", (key,)).fetchone()[0] is True
    checks.append("old-session-release-rejected")
    with psycopg.connect(dsn, autocommit=True) as abandoned, psycopg.connect(dsn, autocommit=True) as successor:
        key = advisory_key(identity(namespace, "session-loss"))
        assert abandoned.execute("SELECT pg_try_advisory_lock(%s::bigint)", (key,)).fetchone()[0] is True
        abandoned.close()
        deadline = time.monotonic() + 10
        acquired = False
        while time.monotonic() < deadline:
            if successor.execute("SELECT pg_try_advisory_lock(%s::bigint)", (key,)).fetchone()[0]:
                acquired = True
                break
            time.sleep(0.05)
        assert acquired
        assert successor.execute("SELECT pg_advisory_unlock(%s::bigint)", (key,)).fetchone()[0] is True
        checks.append("session-loss-cleanup")

    async def exercise_python_provider() -> None:
        provider = PostgresProvider(dsn, namespace, epoch)
        contender_provider = PostgresProvider(dsn, namespace, epoch)
        lease = None
        try:
            same_process = provider.lock("same-process-non-reentrant")
            same_process_lease = await same_process.try_acquire()
            assert same_process_lease is not None
            assert await same_process.try_acquire() is None
            assert (await same_process_lease.release()).value == "released"
            checks.append("same-process-non-reentrancy")

            handle = provider.lock("python-provider")
            contender = contender_provider.lock("python-provider")
            lease = await handle.try_acquire()
            assert lease is not None
            assert await lease.renew() is True
            assert await contender.try_acquire() is None
            try:
                await contender.acquire(AcquireOptions(
                    wait=0.2, lease=Options(lease=1.0, rpc=0.1, cleanup=0.1)))
            except WaitTimeout:
                pass
            else:
                raise AssertionError("contended PostgreSQL acquisition did not time out")
            for invalid in (Options(require_fencing=True), Options(auto_renew=True)):
                try:
                    await contender.try_acquire(invalid)
                except UnsupportedCapability:
                    pass
                else:
                    raise AssertionError("unsupported PostgreSQL capability was accepted")
            cancelled = asyncio.create_task(contender.acquire(AcquireOptions(
                wait=5.0, lease=Options(lease=1.0, rpc=0.1, cleanup=0.1))))
            await asyncio.sleep(0.05)
            cancelled.cancel()
            try:
                await cancelled
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("PostgreSQL acquisition cancellation was not propagated")
            assert (await lease.release()).value == "released"

            lost_release = await provider.lock("lost-release-ack").try_acquire()
            assert lost_release is not None
            async with lost_release._connection.cursor() as cursor:
                await cursor.execute("SELECT pg_backend_pid()")
                pid = (await cursor.fetchone())[0]
            with psycopg.connect(dsn, autocommit=True) as administrator:
                assert administrator.execute(
                    "SELECT pg_terminate_backend(%s)", (pid,)
                ).fetchone()[0] is True
            assert (await lost_release.release()).value == "release_unknown"
            checks.append("lost-release-acknowledgement")
        finally:
            if lease is not None:
                await lease.release()
            await provider.aclose()
            await contender_provider.aclose()

    async def exercise_authentication_failure() -> None:
        if "postgres:postgres@" not in dsn:
            raise AssertionError("conformance DSN must use the documented postgres URI for auth validation")
        bad_provider = PostgresProvider(dsn.replace("postgres:postgres@", "postgres:wrong-password@", 1), namespace, epoch)
        try:
            try:
                await bad_provider.lock("auth-failure").try_acquire()
            except BackendUnavailable:
                checks.append("authentication-failure")
                return
            raise AssertionError("invalid PostgreSQL credentials were accepted")
        finally:
            await bad_provider.aclose()

    asyncio.run(exercise_python_provider())
    asyncio.run(exercise_authentication_failure())
    checks.extend(["python-provider-route", "timeout", "capability-rejection", "cancellation"])
    return {"status": "passed", "postgres_version": version, "checks": checks}


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--namespace", default="ci-postgres")
    parser.add_argument("--epoch", default="ci-postgres-epoch")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run(args.dsn, args.namespace, args.epoch)
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
