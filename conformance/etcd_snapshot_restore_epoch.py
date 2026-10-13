"""Exercise etcd snapshot rollback guards and explicit epoch migration for seven SDKs."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import uuid
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conformance.etcd_fencing_resource import FencedResource
from conformance.etcd_lost_ack import etcd_sdk_config
from conformance.etcd_outage_recovery import LocalEtcdProcess
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker


SDK_NAMES = ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]


def run_checked(argv: list[str], *, timeout: int = 30) -> str:
    result = subprocess.run(
        argv, check=True, capture_output=True, text=True, timeout=timeout
    )
    return result.stdout.strip()


def require_free_loopback_port(port: int) -> None:
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as exc:
            raise RuntimeError(
                f"refusing to use occupied loopback port {port}: {exc}"
            ) from exc


async def launch_workers(
    sdk_names: list[str],
    namespace: str,
    epoch: str,
    endpoint: str,
    manifest: dict,
    run_dir: Path,
    env: dict[str, str],
) -> tuple[dict[str, Worker], list]:
    workers: dict[str, Worker] = {}
    logs = []
    for sdk in sdk_names:
        config_path = run_dir / f"{sdk}-{epoch}.json"
        config_path.write_text(
            json.dumps(etcd_sdk_config(endpoint, namespace, epoch)) + "\n",
            encoding="utf-8",
        )
        worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, env)
        workers[sdk] = worker
        logs.append(log)
        hello = await worker.request("hello", timeout=45)
        if hello.get("status") != "ok":
            raise RuntimeError(f"{sdk} worker for epoch {epoch} did not start: {hello}")
    return workers, logs


async def close_workers(workers: dict[str, Worker]) -> None:
    await asyncio.gather(
        *(worker.close() for worker in workers.values()), return_exceptions=True
    )


def approve_resource_epoch(resource: FencedResource, new_epoch: str) -> None:
    """Model the resource-owner's explicit epoch transition in one fixture transaction."""
    connection = resource._connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT epoch FROM fence_state WHERE resource = ?", (resource.resource,)
        ).fetchone()
        if row is None or row[0] != resource.epoch:
            raise RuntimeError(
                "protected-resource epoch changed outside the approved migration"
            )
        connection.execute(
            "UPDATE fence_state SET epoch = ?, high = 0 WHERE resource = ?",
            (new_epoch, resource.resource),
        )
        connection.execute("COMMIT")
        resource.epoch = new_epoch
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import ROOT as REPO_ROOT, provision_etcd

    endpoint = f"http://127.0.0.1:{args.client_port}"
    if args.endpoint != endpoint:
        raise ValueError("endpoint must be the loopback URL matching --client-port")
    require_free_loopback_port(args.client_port)
    require_free_loopback_port(args.client_port + 1)
    etcdctl = os.environ.get(
        "ETCDCTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe")
    )
    etcdutl = os.environ.get(
        "ETCDUTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdutl.exe")
    )
    for executable in (etcdctl, etcdutl):
        if not Path(executable).is_file():
            raise FileNotFoundError(
                f"required local etcd tool is missing: {executable}"
            )

    sdk_names = args.sdks or SDK_NAMES
    if sdk_names != SDK_NAMES:
        raise ValueError(
            "the restore migration scenario requires all seven SDKs in canonical order"
        )
    namespace = "m6-etcd-restore-" + uuid.uuid4().hex[:18]
    epoch_a = "m6-etcd-restore-epoch-a-" + uuid.uuid4().hex[:8]
    epoch_b = "m6-etcd-restore-epoch-b-" + uuid.uuid4().hex[:8]
    resource_name = "m6/restore-fenced-resource/shared"
    run_dir = (
        ROOT
        / "validation/implementation"
        / ("etcd-snapshot-restore-" + uuid.uuid4().hex)
    )
    run_dir.mkdir(parents=True)
    original_dir = run_dir / "original"
    restored_dir = run_dir / "restored"
    original_dir.mkdir()
    restored_dir.mkdir()
    snapshot = run_dir / "etcd-snapshot.db"
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    original = LocalEtcdProcess(
        original_dir, args.wsl_distribution, endpoint, args.client_port
    )
    restored = LocalEtcdProcess(
        restored_dir, args.wsl_distribution, endpoint, args.client_port
    )
    protected = FencedResource(
        run_dir / "protected-resource.sqlite3", resource_name, epoch_a
    )
    workers: dict[str, Worker] = {}
    logs = []
    cells = {sdk: {"sdk": sdk, "status": "passed"} for sdk in sdk_names}
    old_history: list[dict] = []
    new_history: list[dict] = []
    snapshot_status = None
    restore_status = None
    error = None
    cleanup_error = None
    status = "failed"
    attempt_history = [
        "Initial direct invocation stopped before starting services because the repository root was absent from sys.path (ModuleNotFoundError: conformance); the harness now adds its resolved repository root before importing local modules.",
        "A WSL invocation reached worker startup but timed out because the default C++ launcher selected a Redis-only executable. Its retained stderr says 'dilock-cpp-worker initialization failed: the C++ worker currently supports the Redis profile only'; the harness now selects the staged C++ etcd worker and its library path explicitly.",
    ]
    migration = {
        "approved": False,
        "authority": "test harness explicit local maintenance action",
        "old_epoch": epoch_a,
        "new_epoch": epoch_b,
        "resource_high_watermark_reset": False,
        "namespace_metadata_updated": False,
    }
    try:
        await original.start()
        provision_etcd(endpoint, namespace, epoch_a)
        env = os.environ.copy()
        env["NO_PROXY"] = "localhost,127.0.0.1,::1"
        env["no_proxy"] = env["NO_PROXY"]
        env["DILOCK_CPP_WSL_WORKER"] = str(
            ROOT / ".tools/dilock-cpp-etcd/dilock_cpp_worker"
        )
        env["DILOCK_CPP_WSL_LD_LIBRARY_PATH"] = (
            "/mnt/c/codex/dilock/.tools/grpc-root/usr/lib/x86_64-linux-gnu:"
            "/mnt/c/codex/dilock/.tools/cppinstall/lib:"
            "/mnt/c/codex/dilock/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu"
        )
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            env.pop(key, None)
        workers, worker_logs = await launch_workers(
            sdk_names, namespace, epoch_a, endpoint, manifest, run_dir, env
        )
        logs.extend(worker_logs)

        # Seed a fence that is definitely included in the saved snapshot.
        seed_sdk = sdk_names[0]
        seed = await workers[seed_sdk].request(
            "acquire",
            resource=resource_name,
            wait_ms=args.wait_ms,
            lease_ms=args.lease_ms,
            rpc_ms=args.rpc_ms,
            auto_renew=False,
        )
        if seed.get("status") != "acquired":
            raise AssertionError(f"pre-snapshot seed acquisition failed: {seed}")
        seed_fence = seed.get("fence") or {}
        seed_revision = int(seed_fence.get("value", "0"))
        if seed_fence.get("epoch") != epoch_a or seed_revision <= 0:
            raise AssertionError(f"invalid seed fence: {seed_fence}")
        accepted = protected.write(epoch_a, seed_revision, f"pre-snapshot-{seed_sdk}")
        if not accepted.get("accepted"):
            raise AssertionError(
                f"protected resource rejected pre-snapshot fence: {accepted}"
            )
        old_history.append(
            {"sdk": seed_sdk, "epoch": epoch_a, "revision": seed_revision}
        )
        released = await workers[seed_sdk].request("release", lease_id=seed["lease_id"])
        if released.get("status") != "released":
            raise AssertionError(f"pre-snapshot seed release failed: {released}")

        snapshot_status = run_checked(
            [etcdctl, f"--endpoints={endpoint}", "snapshot", "save", str(snapshot)],
            timeout=60,
        )
        if not snapshot.is_file() or snapshot.stat().st_size <= 0:
            raise AssertionError(
                "etcdctl reported snapshot save but produced no non-empty file"
            )

        # Advance resource high-watermark beyond the saved snapshot revision.
        for cycle in range(5):
            for sdk in sdk_names:
                acquired = await workers[sdk].request(
                    "acquire",
                    resource=resource_name,
                    wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms,
                    rpc_ms=args.rpc_ms,
                    auto_renew=False,
                )
                if acquired.get("status") != "acquired":
                    cells[sdk].update(status="failed", post_snapshot_acquire=acquired)
                    raise AssertionError(
                        f"{sdk} post-snapshot acquisition failed: {acquired}"
                    )
                token = acquired.get("fence") or {}
                revision = int(token.get("value", "0"))
                if (
                    token.get("epoch") != epoch_a
                    or revision <= old_history[-1]["revision"]
                ):
                    raise AssertionError(
                        f"{sdk} pre-restore fence did not advance: {token}"
                    )
                result = protected.write(epoch_a, revision, f"post-snapshot-{sdk}")
                if not result.get("accepted"):
                    raise AssertionError(
                        f"protected resource rejected advancing fence: {result}"
                    )
                old_history.append({"sdk": sdk, "epoch": epoch_a, "revision": revision})
                cells[sdk]["pre_restore_fence"] = {
                    "epoch": epoch_a,
                    "value": str(revision),
                }
                release = await workers[sdk].request(
                    "release", lease_id=acquired["lease_id"]
                )
                if release.get("status") != "released":
                    raise AssertionError(f"{sdk} pre-restore release failed: {release}")

        max_old_revision = old_history[-1]["revision"]
        await close_workers(workers)
        workers = {}
        await original.stop()

        peer = f"http://127.0.0.1:{args.client_port + 1}"
        restore_data = str(restored.data_dir)
        snapshot_native = str(snapshot)
        restore_status = run_checked(
            [
                etcdutl,
                "snapshot",
                "restore",
                snapshot_native,
                "--name",
                "m6-restore",
                "--initial-cluster",
                f"m6-restore={peer}",
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster-token",
                "m6-local-restore-" + uuid.uuid4().hex,
                "--data-dir",
                restore_data,
            ],
            timeout=60,
        )
        await restored.start()
        provisioned_status = run_checked(
            [etcdctl, f"--endpoints={endpoint}", "endpoint", "health"], timeout=20
        )
        if "is healthy" not in provisioned_status:
            raise AssertionError(
                f"restored etcd endpoint did not become healthy: {provisioned_status}"
            )
        for sdk in sdk_names:
            cells[sdk]["restore_legacy_client_status"] = "not_run"

        # Start old-epoch workers against the restored snapshot. They may acquire a
        # valid etcd lock, but their rolled-back revision must be rejected by the
        # resource's pre-restore high-watermark. Release remains owner-scoped.
        workers, worker_logs = await launch_workers(
            sdk_names, namespace, epoch_a, endpoint, manifest, run_dir, env
        )
        logs.extend(worker_logs)
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire",
                resource=resource_name,
                wait_ms=args.wait_ms,
                lease_ms=args.lease_ms,
                rpc_ms=args.rpc_ms,
                auto_renew=False,
            )
            cell = cells[sdk]
            cell["restore_legacy_client_status"] = acquired.get("status")
            if acquired.get("status") != "acquired":
                cell.update(
                    status="failed",
                    error=f"legacy epoch did not reach fencing check: {acquired}",
                )
                continue
            token = acquired.get("fence") or {}
            revision = int(token.get("value", "0"))
            cell["restored_legacy_fence"] = {
                "epoch": token.get("epoch"),
                "value": str(revision),
            }
            if token.get("epoch") != epoch_a:
                cell.update(
                    status="failed",
                    error="restored legacy worker saw an unexpected epoch",
                )
            elif revision >= max_old_revision:
                cell.update(
                    status="failed",
                    error="restore did not roll revision below the pre-restore high-watermark",
                )
            else:
                rejected = protected.write(epoch_a, revision, f"restored-stale-{sdk}")
                cell["resource_rejects_rollback"] = rejected
                if rejected.get("accepted") or rejected.get("reason") != "stale_fence":
                    cell.update(
                        status="failed",
                        error=f"resource accepted a rolled-back fence: {rejected}",
                    )
            release = await workers[sdk].request(
                "release", lease_id=acquired["lease_id"]
            )
            cell["restored_legacy_release"] = release.get("status")
            if release.get("status") != "released":
                cell.update(
                    status="failed",
                    error=f"restored legacy lease release failed: {release}",
                )

        await close_workers(workers)
        workers = {}

        # Explicit local migration: update both the namespace metadata and the
        # protected resource's approved epoch as one operator-controlled test step.
        # The fixture keeps an audit record and changes the current high-watermark
        # scope only after the old epoch has been denied on every SDK cell.
        if any(
            cells[sdk].get("resource_rejects_rollback", {}).get("reason")
            != "stale_fence"
            for sdk in sdk_names
        ):
            raise AssertionError(
                "cannot approve epoch migration before all old-epoch rollback fences are rejected"
            )
        metadata = json.dumps(
            {
                "protocol_major": 1,
                "identity_scheme": "sha256-framed-utf8-v1",
                "profile": "etcd-native-lock-v1",
                "coordination_epoch": epoch_b,
                "fixed_lease_ms": None,
            },
            separators=(",", ":"),
        )
        metadata_key = f"/dilock/namespaces/{namespace}/meta"
        run_checked([etcdctl, f"--endpoints={endpoint}", "put", metadata_key, metadata])
        migration["namespace_metadata_updated"] = True
        approve_resource_epoch(protected, epoch_b)
        migration["approved"] = True
        migration["resource_high_watermark_reset"] = True
        workers, worker_logs = await launch_workers(
            sdk_names, namespace, epoch_b, endpoint, manifest, run_dir, env
        )
        logs.extend(worker_logs)
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire",
                resource=resource_name,
                wait_ms=args.wait_ms,
                lease_ms=args.lease_ms,
                rpc_ms=args.rpc_ms,
                auto_renew=False,
            )
            cell = cells[sdk]
            cell["new_epoch_acquire"] = acquired.get("status")
            if acquired.get("status") != "acquired":
                cell.update(
                    status="failed",
                    error=f"new approved epoch acquire failed: {acquired}",
                )
                continue
            token = acquired.get("fence") or {}
            revision = int(token.get("value", "0"))
            cell["new_epoch_fence"] = {
                "epoch": token.get("epoch"),
                "value": str(revision),
            }
            if token.get("epoch") != epoch_b or revision <= 0:
                cell.update(
                    status="failed", error=f"new epoch fence was malformed: {token}"
                )
            else:
                result = protected.write(epoch_b, revision, f"approved-new-epoch-{sdk}")
                cell["new_epoch_resource_write"] = result
                if not result.get("accepted"):
                    cell.update(
                        status="failed",
                        error=f"resource rejected approved new-epoch fence: {result}",
                    )
                else:
                    new_history.append(
                        {"sdk": sdk, "epoch": epoch_b, "revision": revision}
                    )
                    stale = protected.write(
                        epoch_a, max_old_revision + 1000, f"old-epoch-stale-{sdk}"
                    )
                    cell["old_epoch_after_migration"] = stale
                    if stale.get("accepted") or stale.get("reason") != "epoch_mismatch":
                        cell.update(
                            status="failed",
                            error=f"resource accepted a token from retired epoch: {stale}",
                        )
            release = await workers[sdk].request(
                "release", lease_id=acquired["lease_id"]
            )
            cell["new_epoch_release"] = release.get("status")
            if release.get("status") != "released":
                cell.update(
                    status="failed", error=f"new-epoch release failed: {release}"
                )
        status = (
            "passed"
            if all(cell["status"] == "passed" for cell in cells.values())
            and len(old_history) == len(sdk_names) * 5 + 1
            and len(new_history) == len(sdk_names)
            and migration["approved"]
            else "failed"
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        attempt_history.append(
            f"Harness attempt terminated with this recorded error: {error}"
        )
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        await close_workers(workers)
        for service in (original, restored):
            try:
                await service.stop()
            except Exception as exc:
                cleanup_error = f"{type(exc).__name__}: {exc}"
                status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": "M6-ETCD-SNAPSHOT-RESTORE-EPOCH-MIGRATION-SEVEN-SDK",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "A real etcd snapshot is restored into a fresh local data directory. Seven SDKs demonstrate that restored create_revision values roll below a protected-resource high watermark and are rejected under the old epoch; only after an explicit local maintenance action changes namespace metadata and the resource-approved epoch can the seven new-epoch clients write. Retired-epoch tokens remain rejected.",
            "backend": "etcd 3.7.2 single member; original and restored instances run under WSL from unique workspace-local data directories.",
            "endpoint": safe_endpoint(endpoint),
            "namespace": namespace,
            "resource": resource_name,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "snapshot_path": str(snapshot),
            "snapshot_bytes": snapshot.stat().st_size if snapshot.exists() else None,
            "snapshot_save_output": snapshot_status,
            "snapshot_restore_output": restore_status,
            "original_starts": original.start_count,
            "original_stops": original.stop_count,
            "restored_starts": restored.start_count,
            "restored_stops": restored.stop_count,
            "original_data_directory": str(original.data_dir),
            "restored_data_directory": str(restored.data_dir),
            "protected_resource": "local SQLite BEGIN IMMEDIATE fixture with epoch-scoped high-watermark and explicit approved-epoch transition; test only",
            "protected_resource_database": str(protected.path),
            "migration": migration,
            "pre_restore_fence_history": old_history,
            "new_epoch_fence_history": new_history,
            "cells": list(cells.values()),
            "final_protected_value": protected.current_value(),
            "error": error,
            "cleanup_error": cleanup_error,
            "attempt_history": attempt_history,
            "run_directory": str(run_dir),
            "not_run": [
                "production database migration and operator authorization controls",
                "atomicity spanning etcd metadata and an external production resource",
                "multi-member snapshot restore, quorum reconfiguration or disaster recovery",
                "automatic SDK endpoint discovery or coordinator switching",
                "Redis restore/failover fencing; Redis profile has no strong fence",
            ],
            "limitations": [
                "An etcd create_revision is not globally monotonic across snapshot restore. The old epoch token is demonstrably rejected only because the protected-resource fixture enforces its epoch and high-watermark contract.",
                "Epoch rotation is a coordinated operator procedure represented by two local test-fixture updates; this does not prove atomic migration, authorization workflow, rollback policy or production-resource behavior.",
                "No lease expiry or fencing token stops a stale process. A real protected resource must atomically validate its approved epoch and fence before committing side effects.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:25779")
    parser.add_argument("--client-port", type=int, default=25779)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument(
        "--wsl-distribution",
        default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"),
    )
    parser.add_argument("--lease-ms", type=int, default=5000)
    parser.add_argument("--wait-ms", type=int, default=6000)
    parser.add_argument("--rpc-ms", type=int, default=2000)
    parser.add_argument(
        "--output",
        default="validation/implementation/m6-etcd-snapshot-restore-epoch-seven-sdk.json",
    )
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
