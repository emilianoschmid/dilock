"""Exercise seven native SDK workers across a local etcd stop and restart."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import sys
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

from conformance.etcd_lost_ack import etcd_sdk_config, lock_key_count, lock_prefix
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker


class LocalEtcdProcess:
    """Manage one workspace-local etcd binary under WSL without touching other processes."""

    def __init__(self, run_dir: Path, distro: str, endpoint: str, client_port: int):
        self.run_dir = run_dir
        self.distro = distro
        self.endpoint = endpoint
        self.client_port = client_port
        self.peer_port = client_port + 1
        self.binary = "/mnt/c/codex/dilock/.tools/etcd-linux/etcd"
        self.data_dir = run_dir / "etcd-data"
        self.log_path = run_dir / "etcd.log"
        self.process: asyncio.subprocess.Process | None = None
        self.pid: int | None = None
        self.start_count = 0
        self.stop_count = 0

    async def start(self) -> None:
        if self.process is not None and self.process.returncode is None:
            raise RuntimeError("etcd process is already running")
        command = shlex.join([
            self.binary,
            "--name", "m6-outage", "--data-dir", self._wsl_path(self.data_dir),
            "--listen-client-urls", self.endpoint,
            "--advertise-client-urls", self.endpoint,
            "--listen-peer-urls", f"http://127.0.0.1:{self.peer_port}",
            "--initial-advertise-peer-urls", f"http://127.0.0.1:{self.peer_port}",
            "--initial-cluster", f"m6-outage=http://127.0.0.1:{self.peer_port}",
            "--initial-cluster-state", "new",
        ])
        script = f"echo $$; exec {command} >>{shlex.quote(self._wsl_path(self.log_path))} 2>&1"
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", "bash", "-c", script,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert self.process.stdout is not None
        pid_line = await asyncio.wait_for(self.process.stdout.readline(), timeout=15)
        if not pid_line:
            raise RuntimeError("etcd did not report its WSL process id")
        self.pid = int(pid_line.decode("ascii").strip())
        await self.wait_healthy()
        self.start_count += 1

    async def wait_healthy(self) -> None:
        from tools.provision_etcd import ROOT as REPO_ROOT

        executable = os.environ.get(
            "ETCDCTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe")
        )
        deadline = asyncio.get_running_loop().time() + 25
        last_error = "not attempted"
        while asyncio.get_running_loop().time() < deadline:
            if self.process is None or self.process.returncode is not None:
                detail = ""
                if self.process is not None and self.process.stderr is not None:
                    detail = (await self.process.stderr.read()).decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"etcd stopped before becoming healthy: {last_error}; {detail}")
            try:
                result = await asyncio.to_thread(
                    __import__("subprocess").run,
                    [executable, f"--endpoints={self.endpoint}", "endpoint", "health"],
                    check=True, capture_output=True, text=True, timeout=3,
                )
                if "is healthy" in result.stdout:
                    return
                last_error = result.stdout.strip()
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(0.2)
        raise TimeoutError(f"etcd did not become healthy: {last_error}")

    async def wait_same_member_after_restart(self) -> list[dict[str, str]]:
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            history = self.member_history()
            if len(history) >= 2:
                first, second = history[0], history[1]
                if first["cluster_id"] == second["cluster_id"] and first["member_id"] == second["member_id"]:
                    return history[:2]
                raise AssertionError(f"etcd identity changed across restart: {history[:2]}")
            await asyncio.sleep(0.1)
        raise TimeoutError("etcd startup log did not confirm the same cluster/member identity after restart")

    def member_history(self) -> list[dict[str, str]]:
        rows = []
        try:
            for line in self.log_path.read_text(encoding="utf-8").splitlines():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        except FileNotFoundError:
            return []
        starts = [i for i, row in enumerate(rows) if row.get("msg") == "starting an etcd server"]
        history = []
        for index, start in enumerate(starts):
            end = starts[index + 1] if index + 1 < len(starts) else len(rows)
            identities = {
                (row.get("cluster-id"), row.get("local-member-id"))
                for row in rows[start:end]
                if row.get("cluster-id") and row.get("local-member-id")
            }
            if len(identities) == 1:
                cluster_id, member_id = next(iter(identities))
                history.append({"cluster_id": cluster_id, "member_id": member_id})
        return history

    async def stop(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        assert self.pid is not None
        stop = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", "kill", "-TERM", str(self.pid),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if await stop.wait() != 0:
            raise RuntimeError("could not signal the owned local etcd process")
        try:
            await asyncio.wait_for(self.process.wait(), timeout=15)
        except asyncio.TimeoutError:
            force = await asyncio.create_subprocess_exec(
                "wsl.exe", "-d", self.distro, "--", "kill", "-KILL", str(self.pid),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await force.wait()
            await asyncio.wait_for(self.process.wait(), timeout=10)
        self.stop_count += 1

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import provision_etcd

    parsed = urlsplit(args.endpoint)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("the harness requires an http loopback endpoint")
    if parsed.port != args.client_port or parsed.path not in ("", "/") or parsed.username:
        raise ValueError("endpoint must match --client-port and contain no credentials or path")

    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    namespace = "m6-etcd-outage-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-outage-local-1"
    run_dir = ROOT / "validation/implementation" / ("etcd-outage-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    service = LocalEtcdProcess(run_dir, args.wsl_distribution, args.endpoint, args.client_port)
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    status = "failed"
    error = None
    try:
        await service.start()
        provision_etcd(args.endpoint, namespace, epoch)
        for sdk in sdk_names:
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(args.endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, os.environ.copy())
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {sdk: f"m6/etcd-outage/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names}
        await service.stop()
        async def attempt_while_offline(sdk: str) -> tuple[str, dict]:
            try:
                response = await workers[sdk].request(
                    "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                    timeout=args.wait_ms / 1000 + 10,
                )
            except Exception as exc:
                response = {"status": "worker_error", "error": f"{type(exc).__name__}: {exc}"}
            return sdk, response

        offline = await asyncio.gather(*(attempt_while_offline(sdk) for sdk in sdk_names))
        for sdk, response in offline:
            offline_status = response.get("status")
            cell = {
                "sdk": sdk,
                "status": "passed",
                "offline_result": offline_status,
                "remote_outcome_unknown": response.get("remote_outcome_unknown"),
            }
            if offline_status == "timeout":
                cell["offline_outcome_class"] = "wait_deadline_elapsed_while_backend_stopped"
            elif offline_status == "backend_unavailable":
                cell["offline_outcome_class"] = "backend_failure"
            elif offline_status == "acquisition_unknown":
                cell["offline_outcome_class"] = "explicit_remote_uncertainty"
            else:
                cell["status"] = "failed"
                cell["error"] = "offline etcd returned acquisition, ordinary contention, cancellation, or an unclassified result"
            cells.append(cell)

        await service.start()
        await service.wait_same_member_after_restart()
        lock_counts = await asyncio.gather(*(
            asyncio.to_thread(lock_key_count, args.endpoint, lock_prefix(namespace, resources[sdk]))
            for sdk in sdk_names
        ))
        for sdk, count in zip(sdk_names, lock_counts):
            cell = cells[sdk_names.index(sdk)]
            cell["lock_keys_after_outage"] = count
            if count != 0:
                cell["status"] = "failed"
                cell["error"] = "an offline failure left an etcd lock key"

        for sdk in sdk_names:
            response = await workers[sdk].request(
                "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                timeout=args.wait_ms / 1000 + 10,
            )
            cell = cells[sdk_names.index(sdk)]
            cell["recovery_result"] = response.get("status")
            if response.get("status") == "acquisition_unknown":
                # Never retry an ambiguous Lock immediately. Wait out the configured
                # lease and confirm the hashed resource prefix is empty first.
                cell["recovery_uncertainty"] = "waited_for_attempt_lease_expiry_before_successor"
                await asyncio.sleep(args.lease_ms / 1000 + 1)
                count = await asyncio.to_thread(
                    lock_key_count, args.endpoint, lock_prefix(namespace, resources[sdk])
                )
                cell["lock_keys_after_unknown_recovery"] = count
                if count != 0:
                    cell["status"] = "failed"
                    cell["error"] = "ambiguous recovery attempt left a lock after its lease deadline"
                    continue
                response = await workers[sdk].request(
                    "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                    timeout=args.wait_ms / 1000 + 10,
                )
                cell["successor_after_unknown_result"] = response.get("status")
            if response.get("status") != "acquired":
                cell["status"] = "failed"
                cell["error"] = f"same SDK could not safely acquire after etcd restart: {response}"
                continue
            released = await workers[sdk].request("release", lease_id=response["lease_id"])
            cell["released_after_recovery"] = released.get("status")
            if released.get("status") != "released":
                cell["status"] = "failed"
                cell["error"] = f"recovered lease did not release: {released}"
        status = "passed" if all(cell["status"] == "passed" for cell in cells) else "failed"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        cleanup_error = None
        try:
            await service.stop()
        except Exception as exc:
            cleanup_error = f"{type(exc).__name__}: {exc}"
            status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": "M6-ETCD-BACKEND-OUTAGE-RECOVERY",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "All selected native worker processes remain alive while their single-member etcd backend is stopped. Each operation must avoid acquired/busy and return a backend error, explicit uncertainty or elapsed wait deadline without leaving a lock; each same worker must acquire/release after the same data directory is restarted.",
            "backend": "etcd 3.7.2 Linux binary under WSL Ubuntu 26.04; disposable single member and data directory.",
            "endpoint": safe_endpoint(args.endpoint),
            "namespace": namespace,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "backend_process_starts": service.start_count,
            "graceful_stops": service.stop_count,
            "data_directory": str(service.data_dir),
            "etcd_log": str(service.log_path),
            "member_identity_history": service.member_history(),
            "cells": cells,
            "error": error,
            "cleanup_error": cleanup_error,
            "attempt_history": [
                "Harness startup attempts initially failed because the WSL shell did not receive a positional log path; the runner now uses a quoted absolute WSL path and retains both process logs. An early full run returned acquisition_unknown from TypeScript immediately after restart; the final runner waits the configured lease TTL, confirms the lock prefix is empty, then makes one successor attempt. A misplaced pre-restart identity assertion was also corrected before the final run.",
            ],
            "run_directory": str(run_dir),
            "not_run": [
                "packet-level client or quorum partitions",
                "multi-member leader changes and quorum recovery",
                "snapshot restore and epoch migration",
                "external protected-resource fencing",
            ],
            "limitations": [
                "This is a full local etcd process outage and same-data-directory restart, not a packet-level network partition.",
                "Single-member recovery does not prove quorum, leader-change, snapshot restore, or production safety.",
                "Lease expiry or process survival is not a fencing mechanism; protected resources must enforce fencing independently.",
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
    parser.add_argument("--wait-ms", type=int, default=4000)
    parser.add_argument("--rpc-ms", type=int, default=1200)
    parser.add_argument("--output", default="validation/implementation/m6-etcd-outage-recovery-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
