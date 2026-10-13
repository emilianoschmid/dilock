"""Stop and rejoin an etcd leader while seven SDK workers stay connected to a survivor."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
import uuid
from datetime import date
from pathlib import Path

from conformance.etcd_lost_ack import etcd_sdk_config, lock_key_count, lock_prefix
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker


class LocalEtcdMember:
    """Own one local WSL etcd process and retain its data and logs for review."""

    def __init__(self, name: str, client_port: int, peer_port: int, run_dir: Path,
                 distro: str, cluster: str):
        self.name = name
        self.client_port = client_port
        self.peer_port = peer_port
        self.run_dir = run_dir
        self.distro = distro
        self.cluster = cluster
        self.data_dir = run_dir / f"{name}-data"
        self.log_path = run_dir / f"{name}.log"
        self.process: asyncio.subprocess.Process | None = None
        self.pid: int | None = None
        self.starts = 0
        self.stops = 0

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.client_port}"

    @property
    def peer_url(self) -> str:
        return f"http://127.0.0.1:{self.peer_port}"

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    async def start(self) -> None:
        if self.process is not None and self.process.returncode is None:
            raise RuntimeError(f"etcd member {self.name} is already running")
        binary = self._wsl_path(ROOT / ".tools/etcd-linux/etcd")
        args = [
            binary, "--name", self.name, "--data-dir", self._wsl_path(self.data_dir),
            "--listen-client-urls", self.endpoint, "--advertise-client-urls", self.endpoint,
            "--listen-peer-urls", self.peer_url, "--initial-advertise-peer-urls", self.peer_url,
            "--initial-cluster", self.cluster, "--initial-cluster-state", "new",
        ]
        script = f"echo $$; exec {shlex.join(args)} >>{shlex.quote(self._wsl_path(self.log_path))} 2>&1"
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", "bash", "-c", script,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert self.process.stdout is not None
        line = await asyncio.wait_for(self.process.stdout.readline(), timeout=15)
        if not line:
            raise RuntimeError(f"etcd member {self.name} did not report its WSL PID")
        self.pid = int(line.decode("ascii").strip())
        self.starts += 1

    async def wait_listening(self, etcdctl: str, timeout: float = 25) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_error = "not attempted"
        while asyncio.get_running_loop().time() < deadline:
            if self.process is None or self.process.returncode is not None:
                raise RuntimeError(f"etcd member {self.name} stopped during startup: {last_error}")
            try:
                result = await asyncio.to_thread(
                    subprocess.run,
                    [etcdctl, f"--endpoints={self.endpoint}", "endpoint", "health"],
                    check=True, capture_output=True, text=True, timeout=3,
                )
                if "is healthy" in result.stdout:
                    return
                last_error = result.stdout.strip()
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(0.2)
        raise TimeoutError(f"etcd member {self.name} did not become healthy: {last_error}")

    async def stop(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        if self.pid is None:
            raise RuntimeError(f"etcd member {self.name} has no owned WSL PID")
        stop = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", "kill", "-TERM", str(self.pid),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if await stop.wait() != 0:
            raise RuntimeError(f"could not signal the owned etcd member {self.name}")
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
        self.stops += 1


def etcdctl_json(etcdctl: str, endpoints: list[str], *args: str) -> list[dict]:
    result = subprocess.run(
        [etcdctl, f"--endpoints={','.join(endpoints)}", "--write-out=json", *args],
        check=True, capture_output=True, text=True, timeout=10,
    )
    value = json.loads(result.stdout)
    return value if isinstance(value, list) else [value]


def cluster_status(etcdctl: str, members: list[LocalEtcdMember]) -> list[dict]:
    rows = etcdctl_json(etcdctl, [member.endpoint for member in members], "endpoint", "status")
    statuses = []
    for row in rows:
        status = row.get("Status") or row.get("status") or {}
        header = status.get("header") or {}
        statuses.append({
            "endpoint": row.get("Endpoint") or row.get("endpoint"),
            "member_id": str(header.get("member_id") or header.get("memberId") or ""),
            "cluster_id": str(header.get("cluster_id") or header.get("clusterId") or ""),
            "leader_id": str(status.get("leader") or status.get("leader_id") or ""),
            "raft_term": int(status.get("raftTerm") or status.get("raft_term") or 0),
            "raft_index": int(status.get("raftIndex") or status.get("raft_index") or 0),
            "raft_applied_index": int(
                status.get("raftAppliedIndex") or status.get("raft_applied_index") or 0
            ),
        })
    return statuses


async def wait_consistent_leader(etcdctl: str, members: list[LocalEtcdMember],
                                 timeout: float = 20) -> list[dict]:
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = "no status observed"
    while asyncio.get_running_loop().time() < deadline:
        running = [member for member in members if member.process and member.process.returncode is None]
        try:
            for member in running:
                await member.wait_listening(etcdctl, timeout=1)
            statuses = await asyncio.to_thread(cluster_status, etcdctl, running)
            leader_ids = {row["leader_id"] for row in statuses}
            cluster_ids = {row["cluster_id"] for row in statuses}
            applied_indexes = {row["raft_applied_index"] for row in statuses}
            if len(statuses) == len(running) and len(leader_ids) == 1 and "" not in leader_ids \
                    and len(cluster_ids) == 1 and "" not in cluster_ids and len(applied_indexes) == 1 \
                    and applied_indexes != {0}:
                return statuses
            last_error = f"inconsistent member views: {statuses}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        await asyncio.sleep(0.25)
    raise TimeoutError(f"etcd cluster did not converge on one leader: {last_error}")


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import ROOT as REPO_ROOT, provision_etcd

    etcdctl = os.environ.get(
        "ETCDCTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe")
    )
    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    run_dir = ROOT / "validation/implementation" / ("etcd-leader-failover-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    ports = [args.base_port, args.base_port + 10, args.base_port + 20]
    names = ["m6-s1", "m6-s2", "m6-s3"]
    cluster = ",".join(f"{name}=http://127.0.0.1:{port + 1}" for name, port in zip(names, ports))
    members = [LocalEtcdMember(name, port, port + 1, run_dir, args.wsl_distribution, cluster)
               for name, port in zip(names, ports)]
    namespace = "m6-etcd-leader-failover-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-leader-failover-local-1"
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    initial_status: list[dict] = []
    failover_status: list[dict] = []
    restored_status: list[dict] = []
    stopped_member: str | None = None
    anchor: LocalEtcdMember | None = None
    error = None
    cleanup_error = None
    status = "failed"
    try:
        for member in members:
            await member.start()
        for member in members:
            await member.wait_listening(etcdctl)
        initial_status = await wait_consistent_leader(etcdctl, members)
        leader_id = initial_status[0]["leader_id"]
        endpoint_to_member = {member.endpoint: member for member in members}
        leader = next(endpoint_to_member[row["endpoint"]] for row in initial_status
                      if row["member_id"] == leader_id)
        anchor = next(member for member in members if member.name != leader.name)
        survivors = [member for member in members if member.name != leader.name]
        stopped_member = leader.name
        provision_etcd(anchor.endpoint, namespace, epoch)

        env = os.environ.copy()
        env["NO_PROXY"] = "localhost,127.0.0.1,::1"
        env["no_proxy"] = env["NO_PROXY"]
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            env.pop(key, None)
        for sdk in sdk_names:
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(anchor.endpoint, namespace, epoch)) + "\n", encoding="utf-8"
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, env)
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {sdk: f"m6/etcd-leader-failover/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names}
        prior_leases = {}
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
            )
            if acquired.get("status") != "acquired":
                raise AssertionError(f"{sdk} baseline acquire via surviving member failed: {acquired}")
            prior_leases[sdk] = acquired["lease_id"]

        await leader.stop()
        failover_status = await wait_consistent_leader(etcdctl, survivors, args.failover_timeout_seconds)
        new_leader = failover_status[0]["leader_id"]
        if new_leader == leader_id:
            raise AssertionError(f"stopped leader {leader_id} remained reported as leader")
        ids = {row["member_id"] for row in failover_status}
        if len(ids) != 2 or leader_id in ids:
            raise AssertionError(f"unexpected surviving membership after leader stop: {failover_status}")

        for sdk in sdk_names:
            key = lock_prefix(namespace, resources[sdk])
            count_before_release = await asyncio.to_thread(lock_key_count, anchor.endpoint, key)
            response = await workers[sdk].request(
                "release", lease_id=prior_leases[sdk], timeout=args.rpc_ms / 1000 + 10,
            )
            cell = {
                "sdk": sdk,
                "status": "passed",
                "endpoint_member": anchor.name,
                "lock_keys_before_release_after_leader_stop": count_before_release,
                "old_owner_release_after_leader_stop": response.get("status"),
                "release_remote_outcome_unknown": response.get("remote_outcome_unknown"),
            }
            if count_before_release != 1 or response.get("status") != "released":
                cell.update(
                    status="failed",
                    error=f"old owner release after leader change was not acknowledged cleanly: {response}",
                )
                cells.append(cell)
                continue
            count_after_old_release = await asyncio.to_thread(lock_key_count, anchor.endpoint, key)
            cell["lock_keys_after_old_owner_release"] = count_after_old_release
            if count_after_old_release != 0:
                cell.update(status="failed", error="lock key remained after acknowledged old-owner release")
                cells.append(cell)
                continue

            successor = await workers[sdk].request(
                "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                timeout=args.wait_ms / 1000 + 10,
            )
            cell["successor_acquire_after_failover"] = successor.get("status")
            if successor.get("status") != "acquired":
                cell.update(status="failed", error=f"successor acquire after leader change failed: {successor}")
                cells.append(cell)
                continue

            stale_release = await workers[sdk].request(
                "release", lease_id=prior_leases[sdk], timeout=args.rpc_ms / 1000 + 10,
            )
            cell["stale_old_owner_release_while_successor_held"] = stale_release.get("status")
            count_while_successor_held = await asyncio.to_thread(lock_key_count, anchor.endpoint, key)
            cell["lock_keys_while_successor_held"] = count_while_successor_held
            if stale_release.get("status") not in ("released", "already_not_owned") or count_while_successor_held != 1:
                cell.update(
                    status="failed",
                    error=f"stale old-owner release changed or obscured successor ownership: {stale_release}",
                )

            released_successor = await workers[sdk].request(
                "release", lease_id=successor["lease_id"], timeout=args.rpc_ms / 1000 + 10,
            )
            cell["successor_release_after_failover"] = released_successor.get("status")
            count_after_successor_release = await asyncio.to_thread(lock_key_count, anchor.endpoint, key)
            cell["lock_keys_after_successor_release"] = count_after_successor_release
            if released_successor.get("status") != "released" or count_after_successor_release != 0:
                cell.update(status="failed", error=f"successor release/cleanup failed: {released_successor}")
            cells.append(cell)

        await leader.start()
        for member in members:
            await member.wait_listening(etcdctl)
        restored_status = await wait_consistent_leader(etcdctl, members, args.rejoin_timeout_seconds)
        restored_ids = {row["member_id"] for row in restored_status}
        if len(restored_ids) != 3:
            raise AssertionError(f"stopped member did not rejoin the original cluster: {restored_status}")
        initial_cluster_ids = {row["cluster_id"] for row in initial_status}
        restored_cluster_ids = {row["cluster_id"] for row in restored_status}
        if initial_cluster_ids != restored_cluster_ids:
            raise AssertionError("cluster identity changed when the stopped member rejoined")
        initial_member_ids = {row["member_id"] for row in initial_status}
        if restored_ids != initial_member_ids:
            raise AssertionError("member identity changed when the stopped member rejoined")
        status = "passed" if all(cell["status"] == "passed" for cell in cells) else "failed"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        for member in reversed(members):
            try:
                await member.stop()
            except Exception as exc:
                cleanup_error = f"{member.name} {type(exc).__name__}: {exc}"
                status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": "M6-ETCD-THREE-MEMBER-LEADER-FAILOVER",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "Seven live SDK workers acquire leases before the elected etcd leader is stopped. After the two survivors elect a leader, each old owner releases, a successor acquires, a repeated stale release must preserve the successor key, the successor releases, and the stopped member rejoins with its original cluster identity.",
            "backend": "etcd 3.7.2 Linux binary under WSL Ubuntu 26.04; three local members; no peer traffic proxy or external service.",
            "namespace": namespace,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "members": [{
                "name": member.name,
                "client_endpoint": safe_endpoint(member.endpoint),
                "peer_endpoint": safe_endpoint(member.peer_url),
                "start_count": member.starts,
                "stop_count": member.stops,
                "data_directory": str(member.data_dir),
                "log": str(member.log_path),
            } for member in members],
            "initial_status": initial_status,
            "stopped_leader": stopped_member,
            "sdk_endpoint_member": anchor.name if anchor else None,
            "quorum_status_after_leader_stop": failover_status,
            "cells": cells,
            "rejoined_cluster_status": restored_status,
            "error": error,
            "cleanup_error": cleanup_error,
            "run_directory": str(run_dir),
            "not_run": [
                "peer-to-peer packet partition; this scenario stops a process cleanly",
                "two-of-three quorum loss",
                "snapshot restore and epoch migration",
                "Redis failover",
                "external protected-resource fencing",
            ],
            "limitations": [
                "All SDKs are configured with one client endpoint on a surviving member; multi-endpoint client discovery and failover are not tested.",
                "Stopping the leader tests graceful member loss and Raft re-election, not network partition, process crash, quorum loss, or restore.",
                "Lease expiry and leader change do not stop stale processes or replace resource-side fencing.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-port", type=int, default=25179)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument("--wsl-distribution", default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--lease-ms", type=int, default=4000)
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--rpc-ms", type=int, default=1500)
    parser.add_argument("--failover-timeout-seconds", type=float, default=20)
    parser.add_argument("--rejoin-timeout-seconds", type=float, default=30)
    parser.add_argument("--output", default="validation/implementation/m6-etcd-leader-failover-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
