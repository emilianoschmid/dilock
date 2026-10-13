"""Exercise current etcd SDK fences against a disposable local PostgreSQL resource."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import socket
import subprocess
import sys
import time
import uuid
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2

from conformance.etcd_lost_ack import etcd_sdk_config
from conformance.etcd_outage_recovery import LocalEtcdProcess
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker
from tools.provision_etcd import provision_etcd


class PostgresFencedResource:
    """Test-only PostgreSQL resource applying epoch and fence in one transaction."""

    def __init__(self, connection_parameters: dict[str, object], resource: str, epoch: str):
        self.connection_parameters = connection_parameters
        self.resource = resource
        self.epoch = epoch
        with psycopg2.connect(**connection_parameters) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "CREATE TABLE fence_state (resource text PRIMARY KEY, epoch text NOT NULL, "
                    "high bigint NOT NULL CHECK (high >= 0))"
                )
                cursor.execute(
                    "CREATE TABLE protected_value (resource text PRIMARY KEY, owner text NOT NULL, "
                    "epoch text NOT NULL, revision bigint NOT NULL CHECK (revision > 0))"
                )
                cursor.execute(
                    "INSERT INTO fence_state(resource, epoch, high) VALUES (%s, %s, 0)",
                    (resource, epoch),
                )

    def write(self, token_epoch: str, revision: int, owner: str) -> dict:
        # The row lock serializes contenders. The conditional high-watermark update,
        # protected value write, and commit share this PostgreSQL transaction.
        with psycopg2.connect(**self.connection_parameters) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT epoch, high FROM fence_state WHERE resource = %s FOR UPDATE",
                    (self.resource,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError("protected resource has no initialized fence state")
                current_epoch, high = row
                if token_epoch != current_epoch:
                    return {"accepted": False, "reason": "epoch_mismatch", "high": high}
                cursor.execute(
                    "UPDATE fence_state SET high = %s WHERE resource = %s AND epoch = %s AND high < %s "
                    "RETURNING high",
                    (revision, self.resource, token_epoch, revision),
                )
                registered = cursor.fetchone()
                if registered is None:
                    return {"accepted": False, "reason": "stale_fence", "high": high}
                cursor.execute(
                    "INSERT INTO protected_value(resource, owner, epoch, revision) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT(resource) DO UPDATE SET owner = EXCLUDED.owner, epoch = EXCLUDED.epoch, "
                    "revision = EXCLUDED.revision",
                    (self.resource, owner, token_epoch, revision),
                )
                return {"accepted": True, "reason": "newer_fence", "high": revision}

    def current_value(self) -> dict | None:
        with psycopg2.connect(**self.connection_parameters) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT owner, epoch, revision FROM protected_value WHERE resource = %s",
                    (self.resource,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {"owner": row[0], "epoch": row[1], "revision": row[2]}


def docker(*arguments: str, timeout: int = 30) -> str:
    completed = subprocess.run(
        ["docker", *arguments], check=True, capture_output=True, text=True, timeout=timeout
    )
    return completed.stdout.strip()


def available_port_pair() -> tuple[int, int]:
    for port in range(24000, 50000, 2):
        sockets = []
        try:
            for candidate in (port, port + 1):
                item = socket.socket()
                item.bind(("127.0.0.1", candidate))
                sockets.append(item)
            return port, port + 1
        except OSError:
            pass
        finally:
            for item in sockets:
                item.close()
    raise RuntimeError("no free loopback port pair for the disposable etcd member")


async def wait_postgres(parameters: dict[str, object], timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            with psycopg2.connect(**(parameters | {"connect_timeout": 2})):
                return
        except psycopg2.Error as exc:
            last_error = f"{type(exc).__name__}: {exc}".splitlines()[0]
            await asyncio.sleep(0.25)
    raise TimeoutError(f"disposable PostgreSQL did not become ready: {last_error}")


async def main(args: argparse.Namespace) -> int:
    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    run_id = uuid.uuid4().hex
    namespace = "m6-etcd-pg-fencing-" + run_id[:18]
    epoch = "m6-etcd-pg-fencing-local-1"
    resource_name = "m6/fenced-resource/postgresql"
    run_dir = ROOT / "validation/implementation" / ("etcd-fencing-postgres-" + run_id)
    run_dir.mkdir(parents=True)
    network_name = "dilock-etcd-pg-" + run_id[:16]
    container_name = "dilock-pg-" + run_id[:16]
    image = args.postgres_image
    network_created = False
    container_created = False
    image_id = None
    service: LocalEtcdProcess | None = None
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    history: list[dict] = []
    error = None
    cleanup_errors = []
    status = "not_run"
    protected: PostgresFencedResource | None = None
    final_resource_value = None
    endpoint = None
    try:
        image_id = docker("image", "inspect", "--format", "{{.Id}}", image)
        docker("network", "create", network_name)
        network_created = True
        password = secrets.token_urlsafe(24)
        docker(
            "run", "--pull=never", "--detach", "--rm", "--name", container_name, "--network", network_name,
            "--publish", "127.0.0.1::5432", "--env", f"POSTGRES_PASSWORD={password}", image,
        )
        container_created = True
        mapped = docker("port", container_name, "5432/tcp").splitlines()[0]
        postgres_port = int(mapped.rsplit(":", 1)[1])
        pg_parameters: dict[str, object] = {
            "host": "127.0.0.1", "port": postgres_port,
            "dbname": "postgres", "user": "postgres", "password": password,
            "connect_timeout": 3,
        }
        await wait_postgres(pg_parameters)
        protected = PostgresFencedResource(pg_parameters, resource_name, epoch)

        client_port, peer_port = available_port_pair()
        endpoint = f"http://127.0.0.1:{client_port}"
        service = LocalEtcdProcess(run_dir, args.wsl_distribution, endpoint, client_port)
        await service.start()
        provision_etcd(endpoint, namespace, epoch)

        manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
        env = os.environ.copy()
        env["NO_PROXY"] = "localhost,127.0.0.1,::1"
        env["no_proxy"] = env["NO_PROXY"]
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            env.pop(key, None)
        # The checked-in conformance worker for etcd is built in the pinned local
        # Docker runtime; the generic WSL worker is Redis-only.
        env["DILOCK_CPP_DOCKER_WORKER"] = "1"
        for sdk in sdk_names:
            config = run_dir / f"{sdk}.json"
            config.write_text(json.dumps(etcd_sdk_config(endpoint, namespace, epoch)) + "\n", encoding="utf-8")
            worker, log = await _launch_worker(sdk, config, manifest, run_dir, env)
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        previous_value = None
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire", resource=resource_name, wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
            )
            cell = {"sdk": sdk, "status": "passed", "acquire_result": acquired.get("status")}
            if acquired.get("status") != "acquired":
                cell.update(status="failed", error=f"fencing acquisition failed: {acquired}")
                cells.append(cell)
                continue
            token = acquired.get("fence")
            if not isinstance(token, dict) or token.get("epoch") != epoch:
                cell.update(status="failed", error=f"SDK returned no matching etcd fence: {token}")
                cells.append(cell)
                await workers[sdk].request("release", lease_id=acquired["lease_id"])
                continue
            try:
                revision = int(token.get("value", "0"))
            except (TypeError, ValueError):
                revision = 0
            cell["fence"] = {"epoch": token.get("epoch"), "value": str(revision)}
            if revision <= 0 or (history and revision <= history[-1]["revision"]):
                cell.update(status="failed", error="etcd create_revision did not increase")
            else:
                accepted = protected.write(epoch, revision, sdk)
                cell["resource_write"] = accepted
                if not accepted["accepted"]:
                    cell.update(status="failed", error=f"current fence was rejected: {accepted}")
                else:
                    cell["current_resource_value"] = protected.current_value()
                    if cell["current_resource_value"] != {
                        "owner": sdk, "epoch": epoch, "revision": revision,
                    }:
                        cell.update(status="failed", error="protected value mismatch after commit")
            if previous_value is not None:
                stale = protected.write(
                    previous_value["epoch"], previous_value["revision"], f"late-{previous_value['sdk']}"
                )
                cell["previous_owner_late_write"] = stale
                cell["value_after_stale_write"] = protected.current_value()
                if stale.get("accepted") or stale.get("reason") != "stale_fence":
                    cell.update(status="failed", error=f"PostgreSQL accepted stale predecessor fence: {stale}")
                expected = {"owner": sdk, "epoch": epoch, "revision": revision}
                if cell["value_after_stale_write"] != expected:
                    cell.update(status="failed", error="stale write changed the committed protected value")
                stale_retry = protected.write(
                    previous_value["epoch"], previous_value["revision"], f"late-{previous_value['sdk']}"
                )
                cell["repeated_stale_write"] = stale_retry
                cell["value_after_repeated_stale_write"] = protected.current_value()
                if stale_retry.get("accepted") or stale_retry.get("reason") != "stale_fence":
                    cell.update(status="failed", error=f"PostgreSQL accepted a retried stale fence: {stale_retry}")
                if cell["value_after_repeated_stale_write"] != expected:
                    cell.update(status="failed", error="repeated stale write changed the committed value")
            wrong_epoch = protected.write("unapproved-epoch", revision + 1, f"wrong-epoch-{sdk}")
            cell["wrong_epoch_write"] = wrong_epoch
            if wrong_epoch.get("accepted") or wrong_epoch.get("reason") != "epoch_mismatch":
                cell.update(status="failed", error=f"PostgreSQL accepted a wrong-epoch fence: {wrong_epoch}")
            if cell.get("status") == "passed":
                history.append({"sdk": sdk, "epoch": epoch, "revision": revision})
                previous_value = history[-1]
            released = await workers[sdk].request("release", lease_id=acquired["lease_id"])
            cell["release_result"] = released.get("status")
            if released.get("status") != "released":
                cell.update(status="failed", error=f"SDK lease release failed: {released}")
            cells.append(cell)
        status = "passed" if len(cells) == len(sdk_names) and all(
            cell["status"] == "passed" for cell in cells
        ) and len(history) == len(sdk_names) else "failed"
        final_resource_value = protected.current_value()
    except FileNotFoundError as exc:
        error = f"{type(exc).__name__}: {exc}"
        status = "blocked"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        status = "failed"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        if service is not None:
            try:
                await service.stop()
            except Exception as exc:
                cleanup_errors.append(f"etcd: {type(exc).__name__}: {exc}")
        for log in logs:
            log.close()
        if container_created:
            try:
                docker("rm", "--force", container_name)
            except Exception as exc:
                cleanup_errors.append(f"postgres container: {type(exc).__name__}: {exc}")
        if network_created:
            try:
                docker("network", "rm", network_name)
            except Exception as exc:
                cleanup_errors.append(f"docker network: {type(exc).__name__}: {exc}")
        if cleanup_errors:
            status = "failed"
        report = {
            "id": "M6-ETCD-SEVEN-SDK-POSTGRES-FENCING",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "Each SDK acquires a shared etcd lock in sequence and submits its epoch/create_revision token to a disposable PostgreSQL resource. A row-locked transaction registers only a strictly newer fence and updates the protected value atomically; it rejects repeated late writes from the previous owner and a wrong epoch without mutation.",
            "coordinator": "etcd 3.7.2 single member managed by the existing local WSL runner",
            "protected_resource": {
                "engine": image,
                "image_id": image_id,
                "client_library": f"psycopg2 {psycopg2.__version__}",
                "disposable_docker_network": network_name,
                "transaction": "READ COMMITTED transaction with SELECT ... FOR UPDATE plus conditional high-watermark UPDATE and protected-value UPSERT",
            },
            "endpoint": safe_endpoint(endpoint) if endpoint else None,
            "namespace": namespace,
            "resource": resource_name,
            "participants": sdk_names,
            "command": "python conformance/etcd_fencing_postgres.py --output validation/implementation/m6-etcd-fencing-postgres-seven-sdk-20261007.json",
            "real_processes": True,
            "real_backend": True,
            "cells": cells,
            "accepted_fence_history": history,
            "final_resource_value": final_resource_value,
            "docker_network_created_and_removed": network_created and not cleanup_errors,
            "docker_container_created_and_removed": container_created and not cleanup_errors,
            "error": error,
            "cleanup_errors": cleanup_errors,
            "run_directory": str(run_dir),
            "not_run": [
                "atomic commit spanning etcd and PostgreSQL",
                "PostgreSQL failover, replication, recovery or concurrent multi-session schedule exploration",
                "external/production database configuration, access control, TLS or schema migration",
                "etcd quorum/leader failure and snapshot restore",
            ],
            "limitations": [
                "This demonstrates one PostgreSQL protected-resource transaction consuming real SDK-produced etcd fences; it does not make lock acquisition and the database transaction atomic.",
                "Fencing rejects stale writes only after the protected resource has registered a newer authorized fence. A lease does not stop a paused process.",
                "This local, finite test is not a production database certification or an exactly-once guarantee.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument("--wsl-distribution", default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--postgres-image", default="postgres:16-alpine")
    parser.add_argument("--lease-ms", type=int, default=4000)
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--rpc-ms", type=int, default=1500)
    parser.add_argument("--output", default="validation/implementation/m6-etcd-fencing-postgres-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
