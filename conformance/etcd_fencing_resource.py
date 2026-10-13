"""Exercise etcd fencing tokens against a local transactional protected resource."""
from __future__ import annotations

import argparse
import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
import json
import os
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path

from conformance.etcd_lost_ack import etcd_sdk_config
from conformance.etcd_outage_recovery import LocalEtcdProcess
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, process_scope, safe_endpoint
from conformance.run import Worker


class FencedResource:
    """A local SQLite resource that atomically enforces epoch and high watermark."""

    def __init__(self, path: Path, resource: str, epoch: str):
        self.path = path
        self.resource = resource
        self.epoch = epoch
        with self._connection() as connection:
            connection.execute(
                "CREATE TABLE fence_state (resource TEXT PRIMARY KEY, epoch TEXT NOT NULL, high INTEGER NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE protected_value (resource TEXT PRIMARY KEY, owner TEXT NOT NULL, "
                "epoch TEXT NOT NULL, revision INTEGER NOT NULL)"
            )
            connection.execute(
                "INSERT INTO fence_state(resource, epoch, high) VALUES (?, ?, 0)",
                (resource, epoch),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def write(self, token_epoch: str, revision: int, owner: str) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT epoch, high FROM fence_state WHERE resource = ?", (self.resource,)
            ).fetchone()
            if row is None:
                raise RuntimeError("protected resource has no initialized fence state")
            current_epoch, high = row
            if token_epoch != current_epoch:
                connection.execute("ROLLBACK")
                return {"accepted": False, "reason": "epoch_mismatch", "high": high}
            if revision <= high:
                connection.execute("ROLLBACK")
                return {"accepted": False, "reason": "stale_fence", "high": high}
            connection.execute(
                "UPDATE fence_state SET high = ? WHERE resource = ?", (revision, self.resource)
            )
            connection.execute(
                "INSERT INTO protected_value(resource, owner, epoch, revision) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(resource) DO UPDATE SET owner=excluded.owner, epoch=excluded.epoch, "
                "revision=excluded.revision",
                (self.resource, owner, token_epoch, revision),
            )
            connection.execute("COMMIT")
            return {"accepted": True, "reason": "newer_fence", "high": revision}
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def current_value(self) -> dict | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT owner, epoch, revision FROM protected_value WHERE resource = ?",
                (self.resource,),
            ).fetchone()
        if row is None:
            return None
        return {"owner": row[0], "epoch": row[1], "revision": row[2]}


async def run_paused_owner_fencing(
    workers: dict[str, Worker], protected: FencedResource, epoch: str,
    resource: str, lease_ms: int = 4000,
) -> dict:
    """Resume an expired owner and submit its stale token to a fenced resource."""
    if len(workers) < 2:
        return {"status": "skipped", "reason": "two live SDK workers are required"}
    names = list(workers)
    owner_name, successor_name = names[:2]
    owner, successor = workers[owner_name], workers[successor_name]
    owner_scope = process_scope(owner.process.pid)
    successor_scope = process_scope(successor.process.pid)
    if owner_scope["kind"] == "windows_descendants_of_worker":
        if set(owner_scope["process_ids"]) & set(successor_scope["process_ids"]):
            raise RuntimeError("paused owner and successor process trees overlap")
    elif owner_scope["process_group_id"] == successor_scope["process_group_id"]:
        raise RuntimeError("paused owner and successor do not have separate process groups")

    fields = dict(resource=resource, lease_ms=lease_ms, rpc_ms=1500, auto_renew=False)
    old = None
    successor_lease = None
    paused = False
    suspended_units = 0
    resumed_units = 0
    try:
        old = await owner.request("acquire", wait_ms=5000, **fields)
        if old.get("status") != "acquired":
            raise RuntimeError(f"initial owner acquisition failed: {old}")
        old_fence = old.get("fence")
        if not isinstance(old_fence, dict) or old_fence.get("epoch") != epoch:
            raise RuntimeError(f"initial owner returned no matching fence: {old_fence}")
        old_revision = int(old_fence.get("value", "0"))
        if old_revision <= 0:
            raise RuntimeError(f"initial owner returned invalid fence revision: {old_fence}")
        initial_write = protected.write(epoch, old_revision, owner_name)
        if not initial_write.get("accepted"):
            raise RuntimeError(f"protected resource rejected initial owner: {initial_write}")

        suspended_units = owner.suspend_tree()
        paused = True
        await asyncio.sleep(lease_ms / 1000 + 1.5)
        successor_lease = await successor.request(
            "acquire", wait_ms=lease_ms + 8000, **fields
        )
        if successor_lease.get("status") != "acquired":
            raise RuntimeError(f"successor acquisition after expiry failed: {successor_lease}")
        successor_fence = successor_lease.get("fence")
        if not isinstance(successor_fence, dict) or successor_fence.get("epoch") != epoch:
            raise RuntimeError(f"successor returned no matching fence: {successor_fence}")
        successor_revision = int(successor_fence.get("value", "0"))
        if successor_revision <= old_revision:
            raise RuntimeError(
                f"successor fence {successor_revision} is not newer than {old_revision}"
            )
        successor_write = protected.write(epoch, successor_revision, successor_name)
        if not successor_write.get("accepted"):
            raise RuntimeError(f"protected resource rejected newer owner: {successor_write}")
        value_after_successor = protected.current_value()

        resumed_units = owner.resume_tree()
        paused = False
        if owner.process.returncode is not None:
            raise RuntimeError("paused owner process exited instead of resuming")
        inspection = await owner.request("inspect", lease_id=old["lease_id"])
        if inspection.get("status") == "lost" and "state" not in inspection:
            resumed_state = "lost"
        elif inspection.get("status") == "ok":
            resumed_state = inspection.get("state")
        else:
            raise RuntimeError(f"resumed owner inspection failed: {inspection}")
        if resumed_state != "lost":
            raise RuntimeError(f"resumed owner must report lost, got {resumed_state!r}")

        stale_write = protected.write(
            old_fence["epoch"], old_revision, f"late-{owner_name}"
        )
        value_after_stale_write = protected.current_value()
        expected = {"owner": successor_name, "epoch": epoch, "revision": successor_revision}
        if stale_write.get("accepted") or stale_write.get("reason") != "stale_fence":
            raise RuntimeError(f"protected resource accepted the resumed stale token: {stale_write}")
        if value_after_successor != expected or value_after_stale_write != expected:
            raise RuntimeError(
                "stale write changed the successor's protected value: "
                f"before={value_after_successor}, after={value_after_stale_write}"
            )
        return {
            "status": "passed", "owner_sdk": owner_name, "successor_sdk": successor_name,
            "lease_ms": lease_ms, "old_fence": old_fence,
            "successor_fence": successor_fence, "owner_process_resumed": True,
            "resumed_owner_state": resumed_state,
            "stale_token_submitted_after_resume": True,
            "stale_write_result": stale_write,
            "protected_value_unchanged": value_after_stale_write == expected,
            "process_ownership_disjoint": True, "owner_process_scope": owner_scope,
            "successor_process_scope": successor_scope,
            "suspended_units": suspended_units, "resumed_units": resumed_units,
            "sdk_does_not_stop_resumed_application_effects": True,
        }
    finally:
        if paused:
            owner.resume_tree()
        if successor_lease is not None and successor_lease.get("lease_id"):
            try:
                await successor.request(
                    "release", lease_id=successor_lease["lease_id"], timeout=5
                )
            except Exception:
                pass
        if old is not None and old.get("lease_id"):
            try:
                await owner.request("release", lease_id=old["lease_id"], timeout=5)
            except Exception:
                pass


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import provision_etcd

    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    namespace = "m6-etcd-fencing-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-fencing-local-1"
    resource = "m6/fenced-resource/shared"
    run_dir = ROOT / "validation/implementation" / ("etcd-fencing-resource-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    service = LocalEtcdProcess(run_dir, args.wsl_distribution, args.endpoint, args.client_port)
    protected = FencedResource(run_dir / "protected-resource.sqlite3", resource, epoch)
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    paused_fencing: dict = {"status": "not_run"}
    error = None
    cleanup_error = None
    status = "failed"
    try:
        await service.start()
        provision_etcd(args.endpoint, namespace, epoch)
        env = os.environ.copy()
        env["NO_PROXY"] = "localhost,127.0.0.1,::1"
        env["no_proxy"] = env["NO_PROXY"]
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            env.pop(key, None)
        for sdk in sdk_names:
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(args.endpoint, namespace, epoch)) + "\n", encoding="utf-8"
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, env)
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        history: list[dict] = []
        previous_value = None
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire", resource=resource, wait_ms=args.wait_ms,
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
            except (ValueError, TypeError):
                revision = 0
            cell["fence"] = {"epoch": token.get("epoch"), "value": str(revision)}
            if revision <= 0 or (history and revision <= history[-1]["revision"]):
                cell.update(status="failed", error="etcd create_revision did not increase for the shared lock key")
            else:
                accepted = protected.write(epoch, revision, sdk)
                cell["resource_write"] = accepted
                if not accepted["accepted"]:
                    cell.update(status="failed", error=f"current fence was rejected: {accepted}")
                else:
                    current = protected.current_value()
                    cell["current_resource_value"] = current
                    if current != {"owner": sdk, "epoch": epoch, "revision": revision}:
                        cell.update(status="failed", error=f"protected value mismatch: {current}")
            if previous_value is not None:
                stale = protected.write(previous_value["epoch"], previous_value["revision"],
                                        f"late-{previous_value['sdk']}")
                cell["previous_owner_late_write"] = stale
                cell["value_after_stale_write"] = protected.current_value()
                if stale.get("accepted") or stale.get("reason") != "stale_fence":
                    cell.update(status="failed", error=f"resource accepted a stale predecessor fence: {stale}")
                expected = {"owner": sdk, "epoch": epoch, "revision": revision}
                if cell["value_after_stale_write"] != expected:
                    cell.update(status="failed", error="stale write changed the protected value")
            wrong_epoch = protected.write("unapproved-epoch", revision + 1, f"wrong-epoch-{sdk}")
            cell["wrong_epoch_write"] = wrong_epoch
            if wrong_epoch.get("accepted") or wrong_epoch.get("reason") != "epoch_mismatch":
                cell.update(status="failed", error=f"resource accepted a fence from another epoch: {wrong_epoch}")
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
        if status == "passed":
            paused_cases = []
            for index, owner_name in enumerate(sdk_names):
                successor_name = sdk_names[(index + 1) % len(sdk_names)]
                paused_resource = f"{resource}/paused/{owner_name}"
                paused_protected = FencedResource(
                    run_dir / f"paused-protected-resource-{owner_name}.sqlite3",
                    paused_resource,
                    epoch,
                )
                case = await run_paused_owner_fencing(
                    {
                        owner_name: workers[owner_name],
                        successor_name: workers[successor_name],
                    },
                    paused_protected,
                    epoch,
                    paused_resource,
                    args.lease_ms,
                )
                case["resource"] = paused_resource
                paused_cases.append(case)
            paused_fencing = {
                "status": "passed" if len(paused_cases) == len(sdk_names)
                and all(case.get("status") == "passed" for case in paused_cases)
                else "failed",
                "owner_sdk_coverage": [case.get("owner_sdk") for case in paused_cases],
                "successor_sdk_coverage": [case.get("successor_sdk") for case in paused_cases],
                "cases": paused_cases,
            }
            if paused_fencing["status"] != "passed":
                status = "failed"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        try:
            await service.stop()
        except Exception as exc:
            cleanup_error = f"{type(exc).__name__}: {exc}"
            status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": "M6-ETCD-SEVEN-SDK-RESOURCE-FENCING",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "Each SDK acquires the same etcd lock in sequence and submits its epoch/create_revision token to a local SQLite resource that atomically enforces the approved epoch and strictly increasing high watermark. The resource must reject the previous owner's late write and any wrong-epoch token without changing the current value.",
            "backend": "etcd 3.7.2 Linux binary under WSL Ubuntu 26.04; one disposable member.",
            "endpoint": safe_endpoint(args.endpoint),
            "namespace": namespace,
            "resource": resource,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "protected_resource": "local SQLite database using BEGIN IMMEDIATE and conditional high-watermark updates; test fixture only",
            "cells": cells,
            "paused_owner_fencing": paused_fencing,
            "accepted_fence_history": history if "history" in locals() else [],
            "final_resource_value": protected.current_value(),
            "resource_database": str(protected.path),
            "error": error,
            "cleanup_error": cleanup_error,
            "run_directory": str(run_dir),
            "not_run": [
                "production database or external resource enforcement",
                "atomicity across coordinator and protected resource",
                "snapshot restore, epoch migration and fencing after restore",
                "multi-member quorum or leader failure",
            ],
            "limitations": [
                "This verifies SDK fence production and the demonstrated SQLite consumer transaction only; each real resource must implement and validate its own atomic high-watermark rule.",
                "A fence token does not stop a stale process, make work exactly once, or make a downstream effect transactional with etcd.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:24779")
    parser.add_argument("--client-port", type=int, default=24779)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument("--wsl-distribution", default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--lease-ms", type=int, default=4000)
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--rpc-ms", type=int, default=1500)
    parser.add_argument("--output", default="validation/implementation/m6-etcd-fencing-resource-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
