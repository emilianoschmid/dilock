"""Inject an etcd Lock response loss after the server commits the Lock RPC."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
import hashlib
import ipaddress
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from conformance.nway import ROOT, safe_endpoint
from conformance.run import Worker
from conformance.lost_ack import _launch_worker


class WslEtcdGrpcLostAckProxy:
    """Use the test-only Go generic gRPC proxy inside the etcd/Cpp WSL host."""

    def __init__(self, run_dir: Path, upstream: str, distro: str, bind_host: str = "127.0.0.1",
                 proxy_binary: Path | None = None):
        self.run_dir = run_dir
        self.upstream = upstream
        self.distro = distro
        self.bind_host = bind_host
        self.endpoint_host = bind_host
        self.proxy_binary = proxy_binary or (ROOT / ".tools/m6-etcd-grpc-fault-proxy-linux")
        self.ready_path = run_dir / "ready.json"
        self.result_path = run_dir / "result.json"
        self.stop_path = run_dir / "stop"
        self.process: asyncio.subprocess.Process | None = None
        self._port: int | None = None
        self.lock_requests = 0
        self.successful_lock_responses = 0
        self.dropped_acknowledgements = 0

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("WSL gRPC proxy is not ready")
        return self._port

    async def start(self, *, upstream_host: str, upstream_port: int) -> WslEtcdGrpcLostAckProxy:
        binary = self._wsl_path(self.proxy_binary)
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", binary,
            "--listen-host", self.bind_host,
            "--upstream", f"{upstream_host}:{upstream_port}",
            "--ready-file", self._wsl_path(self.ready_path),
            "--result-file", self._wsl_path(self.result_path),
            "--stop-file", self._wsl_path(self.stop_path),
            "--retry-observation", "2s",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready_path.exists():
                self._port = int(json.loads(self.ready_path.read_text(encoding="utf-8"))["port"])
                return self
            if self.process.returncode is not None:
                raise RuntimeError(f"WSL gRPC proxy exited at startup: {self.process.returncode}")
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL gRPC proxy did not publish readiness")

    def arm(self) -> None:
        # The proxy only drops a Lock response; metadata and lease-grant calls pass through.
        return None

    async def wait_result(self) -> None:
        deadline = asyncio.get_running_loop().time() + 12
        while asyncio.get_running_loop().time() < deadline:
            if self.result_path.exists():
                data = json.loads(self.result_path.read_text(encoding="utf-8"))
                self.lock_requests = int(data["lock_requests"])
                self.successful_lock_responses = int(data["successful_lock_responses"])
                self.dropped_acknowledgements = int(data["dropped_acknowledgements"])
                if self.process is not None:
                    await self.process.wait()
                return
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL gRPC proxy did not write counters")

    async def close(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        self.stop_path.write_text("stop\n", encoding="utf-8")
        try:
            await asyncio.wait_for(self.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()


def lock_prefix(namespace: str, resource: str) -> str:
    from tools.reference_identity import resource_id

    return "/dilock/locks/" + resource_id(namespace, resource)


def etcd_sdk_config(endpoint: str, namespace: str, epoch: str) -> dict:
    """Build the shared etcd worker config consumed by all native SDKs."""
    return {
        "backend": "etcd",
        "endpoint": endpoint,
        "namespace": namespace,
        "epoch": epoch,
    }


def lock_key_count(endpoint: str, prefix: str) -> int:
    from tools.provision_etcd import ROOT as REPO_ROOT

    configured = os.environ.get("ETCDCTL")
    bundled = REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe"
    executable = configured or str(bundled)
    completed = subprocess.run(
        [executable, f"--endpoints={endpoint}", "get", prefix, "--prefix", "--write-out=json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return len(json.loads(completed.stdout).get("kvs", []))


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import provision_etcd

    endpoint = args.endpoint
    parsed = urlsplit(endpoint)
    if parsed.scheme not in ("http", "https") or parsed.hostname is None or parsed.port is None:
        raise ValueError("the test requires an etcd HTTP(S) endpoint with an explicit port")
    if parsed.username or parsed.password or parsed.path not in ("", "/"):
        raise ValueError("the test endpoint must not contain credentials or a path")
    loopback_hosts = ("127.0.0.1", "localhost", "::1")
    interface_host = args.wsl_interface_ip
    if parsed.hostname not in loopback_hosts:
        if interface_host is None or parsed.hostname != interface_host:
            raise ValueError("a non-loopback endpoint must exactly match --wsl-interface-ip")
        address = ipaddress.ip_address(interface_host)
        if not address.is_private or address.is_loopback or address.is_link_local:
            raise ValueError("--wsl-interface-ip must be a private, non-loopback, non-link-local address")

    namespace = "m6-etcd-lost-ack-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-lost-ack-local-1"
    lease_ms = args.lease_ms
    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    run_dir = ROOT / "validation/implementation" / ("etcd-lost-ack-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    proxies: dict[str, WslEtcdGrpcLostAckProxy] = {}
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    observer: Worker | None = None
    observer_log = None
    status = "failed"
    error_message = None

    try:
        provision_etcd(endpoint, namespace, epoch)
        for sdk in sdk_names:
            proxy_dir = run_dir / f"proxy-{sdk}"
            proxy_dir.mkdir()
            proxy = WslEtcdGrpcLostAckProxy(
                proxy_dir, endpoint, args.cpp_wsl_distribution,
                bind_host=interface_host or "127.0.0.1",
                proxy_binary=(ROOT / args.fault_proxy_binary).resolve(),
            )
            await proxy.start(upstream_host=args.wsl_upstream_host, upstream_port=parsed.port)
            proxies[sdk] = proxy
            proxy_endpoint = f"http://{proxy.endpoint_host}:{proxy.port}"
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(proxy_endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, os.environ.copy())
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {sdk: f"m6/etcd-lost-ack/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names}
        pending = {}
        for sdk in sdk_names:
            proxies[sdk].arm()
            request_id, future = await workers[sdk].start_request(
                "acquire",
                resource=resources[sdk],
                wait_ms=8000,
                lease_ms=lease_ms,
                rpc_ms=2000,
                auto_renew=False,
            )
            pending[sdk] = (request_id, future)

        for sdk, (request_id, future) in pending.items():
            try:
                response = await asyncio.wait_for(future, timeout=20)
            finally:
                workers[sdk].responses.pop(request_id, None)
            proxy = proxies[sdk]
            await proxy.wait_result()
            cell = {
                "sdk": sdk,
                "status": "passed",
                "result": response.get("status"),
                "remote_outcome_unknown": response.get("remote_outcome_unknown"),
                "lock_requests": proxy.lock_requests,
                "successful_lock_responses": proxy.successful_lock_responses,
                "dropped_successful_lock_acknowledgements": proxy.dropped_acknowledgements,
            }
            cells.append(cell)
            if response.get("status") in ("acquired", "busy", "timeout"):
                cell["status"] = "failed"
                cell["error"] = "lost successful Lock response became acquisition or ordinary contention"
            if proxy.lock_requests != 1 or proxy.successful_lock_responses != 1 or proxy.dropped_acknowledgements != 1:
                cell["status"] = "failed"
                cell["error"] = "proxy did not observe and drop exactly one successful native Lock response"
            if cell["status"] != "passed":
                raise AssertionError(f"{sdk} etcd lost-ACK cell failed: {cell}")

        # Query only the hashed lock-key prefix; owner values are never written to evidence.
        prefixes = {sdk: lock_prefix(namespace, resource) for sdk, resource in resources.items()}
        counts = await asyncio.gather(*(asyncio.to_thread(lock_key_count, endpoint, prefix) for prefix in prefixes.values()))
        for sdk, count in zip(sdk_names, counts):
            cells[sdk_names.index(sdk)]["lock_keys_after_lost_ack"] = count
            cells[sdk_names.index(sdk)]["state_after_lost_ack"] = "owner_cleanup_removed_lock" if count == 0 else "orphan_remains_until_lease_expiry"

        observer_config = run_dir / "observer.json"
        observer_config.write_text(
            json.dumps(etcd_sdk_config(endpoint, namespace, epoch)) + "\n",
            encoding="utf-8",
        )
        observer, observer_log = await _launch_worker("dotnet", observer_config, manifest, run_dir, os.environ.copy())
        hello = await observer.request("hello", timeout=30)
        if hello.get("status") != "ok":
            raise RuntimeError(f"direct etcd observer did not start: {hello}")

        async def acquire_successor(sdk: str) -> tuple[str, str]:
            successor = await observer.request(
                "acquire",
                resource=resources[sdk],
                wait_ms=lease_ms + 4000,
                lease_ms=lease_ms,
                rpc_ms=2000,
                auto_renew=False,
                timeout=lease_ms / 1000 + 8,
            )
            if successor.get("status") != "acquired":
                raise AssertionError(f"{sdk} successor failed after cleanup/lease expiry: {successor}")
            await observer.request("release", lease_id=successor["lease_id"])
            return sdk, "acquired_and_released"

        recovered = await asyncio.gather(*(acquire_successor(sdk) for sdk in sdk_names))
        for sdk, outcome in recovered:
            cells[sdk_names.index(sdk)]["successor_after_cleanup_or_lease_expiry"] = outcome
        status = "passed"
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
        else:
            status = "failed"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        if observer is not None:
            await observer.close()
        await asyncio.gather(*(proxy.close() for proxy in proxies.values()), return_exceptions=True)
        for log in logs:
            log.close()
        if observer_log is not None:
            observer_log.close()
        report = {
            "id": "M6-FAULT-ETCD-LOST-LOCK-ACK",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "The etcd native Lock RPC succeeds upstream, but the generic gRPC proxy withholds its successful response. SDKs must not report acquisition or ordinary contention; successor acquisition is checked after owner-checked lease cleanup or lease expiry.",
            "backend": "etcd 3.7.2; one disposable single-member instance.",
            "endpoint": safe_endpoint(endpoint),
            "namespace": namespace,
            "lease_ms": lease_ms,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "fault_proxy": {
                "source": "conformance/etcd_fault_proxy.go",
                "binary": str((ROOT / args.fault_proxy_binary).resolve()),
                "sha256": hashlib.sha256((ROOT / args.fault_proxy_binary).resolve().read_bytes()).hexdigest(),
                "listen_host": interface_host or "127.0.0.1",
            },
            "cells": cells,
            "error": error_message,
            "run_directory": str(run_dir),
            "not_run": [
                "network partitions and leader changes",
                "stop/resume and snapshot restore",
                "Redis failover",
                "external protected-resource fencing",
            ] + ([] if status == "passed" else ["one or more SDK cells or successor-cleanup checks did not complete"]),
            "limitations": [
                "This injects a successful etcd Lock response loss only; it does not test quorum partitions, leader changes, snapshot restore, Redis failover or external protected-resource fencing.",
                "Lease expiry does not prove that an expired process stopped; an unknown-result caller must not enter protected work.",
                "C++ and Windows-native workers may reach a WSL-hosted proxy bound to a private interface; this does not model production network boundaries.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Disposable loopback etcd endpoint.")
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-cancel-workers-seven.json")
    parser.add_argument("--lease-ms", type=int, default=5000)
    parser.add_argument("--cpp-wsl-distribution", default=os.environ.get("DILOCK_CPP_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--wsl-upstream-host", default=os.environ.get("DILOCK_ETCD_WSL_UPSTREAM_HOST", "127.0.0.1"),
                        help="etcd address as seen from the WSL-hosted proxy.")
    parser.add_argument("--wsl-interface-ip", help="Private WSL interface IP used for etcd and proxy binds when Windows loopback forwarding is unavailable.")
    parser.add_argument("--fault-proxy-binary", default=".tools/m6-etcd-grpc-fault-proxy-linux",
                        help="Test-only generic gRPC proxy executable. Must be runnable inside the selected WSL distribution.")
    parser.add_argument("--output", default="validation/implementation/m6-etcd-lost-ack-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
