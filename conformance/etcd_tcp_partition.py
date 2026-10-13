"""Cut and restore the seven SDKs' client TCP path to a local etcd member."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
import json
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from conformance.etcd_lost_ack import etcd_sdk_config, lock_key_count, lock_prefix
from conformance.etcd_outage_recovery import LocalEtcdProcess
from conformance.lost_ack import _launch_worker
from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker


class WslTcpPartitionProxy:
    """Forward etcd's raw TCP/gRPC stream and close flows during a client-path partition."""

    def __init__(self, run_dir: Path, distro: str, upstream: str):
        self.run_dir = run_dir
        self.distro = distro
        self.upstream = upstream
        self.ready_path = run_dir / "proxy-ready.json"
        self.state_path = run_dir / "proxy-state.json"
        self.result_path = run_dir / "proxy-result.json"
        self.partition_path = run_dir / "partition-active"
        self.stop_path = run_dir / "proxy-stop"
        self.process: asyncio.subprocess.Process | None = None
        self.port: int | None = None

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    async def start(self) -> None:
        binary = self._wsl_path(ROOT / ".tools/m6-etcd-tcp-partition-proxy-linux")
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", binary,
            "--listen-host", "127.0.0.1",
            "--upstream", self.upstream,
            "--partition-file", self._wsl_path(self.partition_path),
            "--ready-file", self._wsl_path(self.ready_path),
            "--state-file", self._wsl_path(self.state_path),
            "--result-file", self._wsl_path(self.result_path),
            "--stop-file", self._wsl_path(self.stop_path),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready_path.exists() and self.state_path.exists():
                self.port = int(json.loads(self.ready_path.read_text(encoding="utf-8"))["port"])
                return
            if self.process.returncode is not None:
                detail = ""
                if self.process.stderr is not None:
                    detail = (await self.process.stderr.read()).decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"TCP partition proxy exited at startup: {self.process.returncode}; {detail}")
            await asyncio.sleep(0.05)
        raise TimeoutError("TCP partition proxy did not publish readiness")

    @property
    def endpoint(self) -> str:
        if self.port is None:
            raise RuntimeError("TCP partition proxy is not ready")
        return f"http://127.0.0.1:{self.port}"

    async def wait_state(self, predicate, timeout: float = 8) -> dict:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            try:
                state = json.loads(self.state_path.read_text(encoding="utf-8"))
                if predicate(state):
                    return state
            except (OSError, json.JSONDecodeError):
                pass
            if self.process is not None and self.process.returncode is not None:
                raise RuntimeError(f"TCP partition proxy exited: {self.process.returncode}")
            await asyncio.sleep(0.025)
        raise TimeoutError("TCP proxy did not reach the requested partition state")

    async def close(self) -> dict | None:
        if self.process is None or self.process.returncode is not None:
            return None
        self.stop_path.write_text("stop\n", encoding="utf-8")
        try:
            await asyncio.wait_for(self.process.wait(), timeout=8)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()
        if self.result_path.exists():
            return json.loads(self.result_path.read_text(encoding="utf-8"))
        return None


async def wait_proxy_health(endpoint: str, timeout: float) -> dict:
    """Require the healed forwarding path to answer etcd health before SDK checks."""
    from tools.provision_etcd import ROOT as REPO_ROOT

    executable = os.environ.get(
        "ETCDCTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe")
    )
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = "not attempted"
    while asyncio.get_running_loop().time() < deadline:
        try:
            result = await asyncio.to_thread(
                __import__("subprocess").run,
                [executable, f"--endpoints={endpoint}", "endpoint", "health"],
                check=True, capture_output=True, text=True, timeout=3,
            )
            if "is healthy" in result.stdout:
                return {"status": "healthy", "output": result.stdout.strip()}
            last_error = result.stdout.strip()
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        await asyncio.sleep(0.1)
    raise TimeoutError(f"healed etcd path did not become healthy through TCP proxy: {last_error}")


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import provision_etcd

    backend = urlsplit(args.backend_endpoint)
    if backend.scheme != "http" or backend.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("the harness requires an http loopback etcd endpoint")
    if backend.port != args.client_port or backend.path not in ("", "/") or backend.username:
        raise ValueError("backend endpoint must match --client-port and contain no credentials or path")

    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    namespace = "m6-etcd-tcp-partition-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-tcp-partition-local-1"
    run_dir = ROOT / "validation/implementation" / ("etcd-tcp-partition-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    service = LocalEtcdProcess(run_dir, args.wsl_distribution, args.backend_endpoint, args.client_port)
    proxy = WslTcpPartitionProxy(run_dir, args.wsl_distribution, f"127.0.0.1:{args.client_port}")
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    proxy_result = None
    status = "failed"
    error = None
    cleanup_error = None
    try:
        await service.start()
        provision_etcd(args.backend_endpoint, namespace, epoch)
        await proxy.start()
        for sdk in sdk_names:
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(proxy.endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, os.environ.copy())
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {sdk: f"m6/etcd-tcp-partition/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names}
        for sdk in sdk_names:
            acquired = await workers[sdk].request(
                "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
            )
            if acquired.get("status") != "acquired":
                raise AssertionError(f"{sdk} baseline acquire through TCP proxy failed: {acquired}")
            released = await workers[sdk].request("release", lease_id=acquired["lease_id"])
            if released.get("status") != "released":
                raise AssertionError(f"{sdk} baseline release through TCP proxy failed: {released}")

        proxy.partition_path.write_text("partition\n", encoding="utf-8")
        partition_state = await proxy.wait_state(
            lambda value: value.get("partitioned") is True and value.get("active_connections") == 0
        )

        async def acquire_while_partitioned(sdk: str) -> tuple[str, dict]:
            try:
                response = await workers[sdk].request(
                    "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                    timeout=args.wait_ms / 1000 + 10,
                )
            except Exception as exc:
                response = {"status": "worker_error", "error": f"{type(exc).__name__}: {exc}"}
            return sdk, response

        partitioned_results = await asyncio.gather(*(
            acquire_while_partitioned(sdk) for sdk in sdk_names
        ))
        for sdk, response in partitioned_results:
            result = response.get("status")
            cell = {
                "sdk": sdk,
                "status": "passed",
                "partition_result": result,
                "remote_outcome_unknown": response.get("remote_outcome_unknown"),
            }
            if result not in ("backend_unavailable", "acquisition_unknown", "timeout"):
                cell["status"] = "failed"
                cell["error"] = "partition returned acquired, busy, cancelled, or an unclassified result"
            cells.append(cell)
        partition_state = await proxy.wait_state(
            lambda value: value.get("partitioned") is True and value.get("blocked_connections", 0) > 0
        )
        direct_counts = await asyncio.gather(*(
            asyncio.to_thread(lock_key_count, args.backend_endpoint, lock_prefix(namespace, resources[sdk]))
            for sdk in sdk_names
        ))
        for sdk, count in zip(sdk_names, direct_counts):
            cell = cells[sdk_names.index(sdk)]
            cell["lock_keys_during_partition"] = count
            if count != 0:
                cell["status"] = "failed"
                cell["error"] = "partitioned request created an etcd lock key"

        proxy.partition_path.unlink(missing_ok=True)
        healed_state = await proxy.wait_state(lambda value: value.get("partitioned") is False)
        # A recovered TCP listener does not mean long-lived gRPC channels have
        # completed their reconnect backoff. Confirm the forwarded endpoint and
        # allow that backoff to settle before judging SDK recovery. No acquisition
        # is retried here: each SDK still gets one recovery request.
        health = await wait_proxy_health(proxy.endpoint, args.heal_timeout_seconds)
        await asyncio.sleep(args.heal_grace_ms / 1000)
        for sdk in sdk_names:
            response = await workers[sdk].request(
                "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                timeout=args.wait_ms / 1000 + 10,
            )
            cell = cells[sdk_names.index(sdk)]
            cell["heal_result"] = response.get("status")
            if response.get("status") == "acquisition_unknown":
                cell["heal_uncertainty"] = "waited_for_attempt_lease_expiry_before_successor"
                await asyncio.sleep(args.lease_ms / 1000 + 1)
                count = await asyncio.to_thread(
                    lock_key_count, args.backend_endpoint, lock_prefix(namespace, resources[sdk])
                )
                cell["lock_keys_after_unknown_heal"] = count
                if count != 0:
                    cell["status"] = "failed"
                    cell["error"] = "ambiguous heal attempt left a lock after its lease deadline"
                    continue
                response = await workers[sdk].request(
                    "acquire", resource=resources[sdk], wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms, rpc_ms=args.rpc_ms, auto_renew=False,
                    timeout=args.wait_ms / 1000 + 10,
                )
                cell["successor_after_unknown_heal"] = response.get("status")
            if response.get("status") != "acquired":
                cell["status"] = "failed"
                cell["error"] = f"{sdk} could not acquire after the TCP path healed: {response}"
                continue
            released = await workers[sdk].request("release", lease_id=response["lease_id"])
            cell["released_after_heal"] = released.get("status")
            if released.get("status") != "released":
                cell["status"] = "failed"
                cell["error"] = f"{sdk} did not release after recovery: {released}"

        status = "passed" if all(cell["status"] == "passed" for cell in cells) else "failed"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        try:
            proxy_result = await proxy.close()
        except Exception as exc:
            cleanup_error = f"proxy {type(exc).__name__}: {exc}"
            status = "failed"
        try:
            await service.stop()
        except Exception as exc:
            cleanup_error = f"etcd {type(exc).__name__}: {exc}"
            status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": "M6-ETCD-CLIENT-TCP-PARTITION",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "A local TCP fault proxy cuts every existing SDK-to-etcd flow and rejects new connections while the etcd authority remains healthy. Each SDK must avoid acquired/busy/cancelled, leave no lock key, then acquire/release after the TCP path heals.",
            "backend": "etcd 3.7.2 Linux binary under WSL Ubuntu 26.04; single member. TCP proxy is a separate local WSL process.",
            "endpoint": safe_endpoint(args.backend_endpoint),
            "namespace": namespace,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "partition_barrier": partition_state if "partition_state" in locals() else None,
            "healed_state": healed_state if "healed_state" in locals() else None,
            "healed_endpoint_health": health if "health" in locals() else None,
            "heal_grace_ms": args.heal_grace_ms,
            "proxy_result": proxy_result,
            "cells": cells,
            "error": error,
            "cleanup_error": cleanup_error,
            "attempt_history": [
                "The first proxy build used a Windows PE binary under WSL and failed before SDK startup; no SDK cell ran. Rebuilding with GOOS=linux produced the ELF proxy used by the passing run.",
                "The first seven-SDK run without the documented DILOCK_CPP_WSL_WORKER and DILOCK_CPP_WSL_LD_LIBRARY_PATH overrides selected a Redis-only C++ worker and stopped before test cells. The corrected run used the staged etcd worker and libraries.",
                "An earlier correctly configured seven-SDK run recorded Python backend_unavailable on its first immediate recovery request after the path healed. The final harness first verified etcd health through the healed proxy and allowed 1500 ms for existing gRPC channels to reconnect; it still issued exactly one recovery acquire per SDK, and all seven passed.",
            ],
            "run_directory": str(run_dir),
            "not_run": [
                "peer-to-peer quorum partition and leader change",
                "snapshot restore and epoch migration",
                "Redis failover",
                "external protected-resource fencing",
            ],
            "limitations": [
                "This cuts the client TCP path through a local proxy; it does not partition etcd peer traffic or simulate kernel packet loss.",
                "The test uses one etcd member and does not establish quorum or leader-change behavior.",
                "Lease expiry and process recovery do not stop stale processes or replace resource-side fencing.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-endpoint", default="http://127.0.0.1:24779")
    parser.add_argument("--client-port", type=int, default=24779)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument("--wsl-distribution", default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--lease-ms", type=int, default=4000)
    parser.add_argument("--wait-ms", type=int, default=4000)
    parser.add_argument("--rpc-ms", type=int, default=1200)
    parser.add_argument("--heal-grace-ms", type=int, default=1500)
    parser.add_argument("--heal-timeout-seconds", type=float, default=10)
    parser.add_argument("--output", default="validation/implementation/m6-etcd-tcp-partition-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
