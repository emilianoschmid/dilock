"""Real-process Redis test that drops successful SET replies after commit."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from conformance.nway import ROOT, safe_endpoint
from conformance.redis_fault_proxy import RedisLostAckProxy
from conformance.run import Worker, spawn_process


async def _launch_worker(
    sdk: str,
    config_path: Path,
    manifest: dict,
    run_dir: Path,
    env: dict,
    *,
    popen_on_windows: bool = False,
):
    argv = manifest[sdk]["argv"] + [str(config_path)]
    log = (run_dir / f"{sdk}.stderr.log").open("wb")
    process = await spawn_process(
        *argv,
        popen_on_windows=popen_on_windows,
        cwd=ROOT,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=log,
        start_new_session=(os.name != "nt"),
    )
    return Worker(process), log


class WslRedisLostAckProxy:
    """Run the RESP proxy inside WSL so a WSL-native worker uses loopback."""

    endpoint_host = "127.0.0.1"

    def __init__(self, run_dir: Path, upstream_host: str, upstream_port: int, distro: str):
        self.run_dir = run_dir
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.distro = distro
        self.ready_path = run_dir / "cpp-proxy-ready.json"
        self.result_path = run_dir / "cpp-proxy-result.json"
        self.stop_path = run_dir / "cpp-proxy-stop"
        self.process: asyncio.subprocess.Process | None = None
        self._port: int | None = None
        self.set_attempts = 0
        self.successful_sets = 0
        self.dropped_acknowledgements = 0
        self.errors: list[str] = []

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("WSL proxy is not ready")
        return self._port

    async def start(self) -> WslRedisLostAckProxy:
        script = self._wsl_path(ROOT / "conformance/redis_fault_proxy.py")
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe", "-d", self.distro, "--", "python3", script,
            "--upstream-host", self.upstream_host,
            "--upstream-port", str(self.upstream_port),
            "--ready-file", self._wsl_path(self.ready_path),
            "--result-file", self._wsl_path(self.result_path),
            "--stop-file", self._wsl_path(self.stop_path),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready_path.exists():
                data = json.loads(self.ready_path.read_text(encoding="utf-8"))
                self._port = int(data["port"])
                return self
            if self.process.returncode is not None:
                raise RuntimeError(f"WSL fault proxy exited during startup: {self.process.returncode}")
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL fault proxy did not publish its ready file")

    def arm(self) -> None:
        # The WSL proxy arms itself at startup; namespace provisioning bypasses it.
        return None

    async def wait_result(self) -> None:
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            if self.result_path.exists():
                result = json.loads(self.result_path.read_text(encoding="utf-8"))
                self.set_attempts = int(result["set_attempts"])
                self.successful_sets = int(result["successful_sets"])
                self.dropped_acknowledgements = int(result["dropped_acknowledgements"])
                self.errors = list(result["errors"])
                if self.process is not None:
                    await self.process.wait()
                return
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL fault proxy did not write its result")

    async def close(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        self.stop_path.write_text("stop\n", encoding="utf-8")
        try:
            await asyncio.wait_for(self.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()


def _sdk_config(sdk: str, endpoint: str, namespace: str, epoch: str) -> dict:
    if sdk != "cpp":
        return {"backend": "redis", "endpoint": endpoint, "namespace": namespace, "epoch": epoch}
    parsed = urlsplit(endpoint)
    return {
        "backend": "redis",
        "redis": {
            "host": parsed.hostname,
            "port": parsed.port or 6379,
            "username": "",
            "password": "",
            "database": int(parsed.path.lstrip("/") or "0"),
            "namespace": namespace,
            "coordination_epoch": epoch,
            "rpc_timeout_ms": 1000,
        },
    }


async def main(args: argparse.Namespace) -> int:
    from redis.asyncio import Redis
    from tools.provision import provision
    from tools.reference_identity import redis_key

    endpoint = args.endpoint
    upstream = urlsplit(endpoint)
    if upstream.scheme != "redis" or upstream.hostname is None:
        raise ValueError("the test requires an uncredentialed redis:// endpoint")
    if upstream.username or upstream.password or upstream.path not in ("", "/", "/0"):
        raise ValueError("the test endpoint must not contain credentials or a nonzero database")

    namespace = "m6-lost-ack-" + uuid.uuid4().hex[:20]
    epoch = "m6-lost-ack-local-1"
    lease_ms = args.lease_ms
    sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    run_dir = ROOT / "validation/implementation" / ("lost-ack-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    proxies: dict[str, RedisLostAckProxy | WslRedisLostAckProxy] = {}
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict] = []
    observer: Worker | None = None
    observer_log = None
    status = "failed"
    result_error = None
    try:
        await provision(endpoint, namespace, epoch)
        for sdk in sdk_names:
            if sdk == "cpp":
                proxy = await WslRedisLostAckProxy(
                    run_dir, upstream.hostname, upstream.port or 6379,
                    args.cpp_wsl_distribution,
                ).start()
            else:
                proxy = await RedisLostAckProxy(
                    upstream.hostname, upstream.port or 6379
                ).start()
            proxies[sdk] = proxy
            proxy_endpoint_host = proxy.endpoint_host if isinstance(proxy, WslRedisLostAckProxy) else "127.0.0.1"
            proxy_endpoint = f"redis://{proxy_endpoint_host}:{proxy.port}"
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(_sdk_config(sdk, proxy_endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(sdk, config_path, manifest, run_dir, os.environ.copy())
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {sdk: f"m6/lost-ack/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names}
        pending = {}
        for sdk in sdk_names:
            proxies[sdk].arm()
            request_id, future = await workers[sdk].start_request(
                "try_acquire",
                resource=resources[sdk],
                lease_ms=lease_ms,
                rpc_ms=1000,
                auto_renew=False,
            )
            pending[sdk] = (request_id, future)

        started = time.monotonic()
        for sdk in sdk_names:
            request_id, future = pending[sdk]
            try:
                response = await asyncio.wait_for(future, timeout=20)
            finally:
                workers[sdk].responses.pop(request_id, None)
            proxy = proxies[sdk]
            if isinstance(proxy, WslRedisLostAckProxy):
                await proxy.wait_result()
            cell = {
                "sdk": sdk,
                "status": "passed",
                "result": response.get("status"),
                "remote_outcome_unknown": response.get("remote_outcome_unknown"),
                "successful_set_ack_dropped": proxy.dropped_acknowledgements,
                "set_attempts_after_injection_armed": proxy.set_attempts,
                "proxy_errors": proxy.errors,
            }
            cells.append(cell)
            if response.get("status") != "acquisition_unknown" or response.get("remote_outcome_unknown") is False:
                cell["status"] = "failed"
                cell["error"] = "the lost successful SET response was not classified as acquisition_unknown"
            if proxy.dropped_acknowledgements != 1 or proxy.successful_sets != 1:
                cell["status"] = "failed"
                cell["error"] = "proxy did not drop exactly one successful SET acknowledgement"
            if proxy.set_attempts != 1:
                cell["status"] = "failed"
                cell["error"] = "SDK resent the ambiguous SET instead of returning its uncertain outcome"
            if proxy.errors:
                cell["status"] = "failed"
            if cell["status"] != "passed":
                raise AssertionError(f"{sdk} lost-ack cell failed: {cell}")

        for sdk in sdk_names:
            await workers[sdk].close()
        workers.clear()

        observer_config = run_dir / "observer.json"
        observer_config.write_text(
            json.dumps(_sdk_config("dotnet", endpoint, namespace, epoch)) + "\n",
            encoding="utf-8",
        )
        observer, observer_log = await _launch_worker("dotnet", observer_config, manifest, run_dir, os.environ.copy())
        hello = await observer.request("hello", timeout=30)
        if hello.get("status") != "ok":
            raise RuntimeError(f"direct Redis observer did not start: {hello}")

        direct_redis = Redis.from_url(endpoint)
        try:
            for sdk in sdk_names:
                key = redis_key(namespace, resources[sdk])
                raw = await direct_redis.get(key)
                ttl = await direct_redis.pttl(key) if raw is not None else -2
                next_try = await observer.request(
                    "try_acquire",
                    resource=resources[sdk],
                    lease_ms=lease_ms,
                    rpc_ms=1000,
                    auto_renew=False,
                )
                if next_try.get("status") not in ("busy", "acquired"):
                    raise AssertionError(f"direct observer saw unexpected state after {sdk}: {next_try}")
                cells[sdk_names.index(sdk)]["state_after_lost_ack"] = (
                    "owner_checked_cleanup_completed" if raw is None else "orphan_remains_until_ttl"
                )
                cells[sdk_names.index(sdk)]["remaining_ttl_ms_at_inspection"] = ttl
                if raw is not None and not 0 < ttl <= lease_ms:
                    raise AssertionError(f"{sdk} orphan has invalid remaining Redis TTL: {ttl}")
                if next_try.get("status") == "acquired":
                    await observer.request("release", lease_id=next_try["lease_id"])
        finally:
            await direct_redis.aclose()

        await asyncio.sleep(max(0, lease_ms / 1000 + 0.75 - (time.monotonic() - started)))
        for sdk in sdk_names:
            successor = await observer.request(
                "try_acquire",
                resource=resources[sdk],
                lease_ms=lease_ms,
                rpc_ms=1000,
                auto_renew=False,
            )
            if successor.get("status") != "acquired":
                raise AssertionError(f"{sdk} resource did not become acquirable after cleanup/TTL: {successor}")
            await observer.request("release", lease_id=successor["lease_id"])
            cells[sdk_names.index(sdk)]["successor_after_cleanup_or_ttl"] = "acquired_and_released"
        status = "passed"
    except Exception as exc:
        result_error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
        else:
            status = "failed"
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        if observer is not None:
            await observer.close()
        for proxy in proxies.values():
            await proxy.close()
        for log in logs:
            log.close()
        if observer_log is not None:
            observer_log.close()
        report = {
            "id": "M6-FAULT-REDIS-LOST-ACK",
            "date": "2026-10-03",
            "status": status,
            "scenario": "A real Redis SET commits, its successful reply is dropped by a TCP proxy, and each SDK must report an unknown acquisition without retrying as contention. The successor acquires after owner-checked cleanup or TTL expiry.",
            "backend": "Redis 8.10.2; one disposable instance; persistence disabled by the caller.",
            "endpoint": safe_endpoint(endpoint),
            "namespace": namespace,
            "lease_ms": lease_ms,
            "participants": sdk_names,
            "real_processes": True,
            "real_backend": True,
            "cells": cells,
            "error": result_error,
            "run_directory": str(run_dir),
            "not_run": [] if status == "passed" else ["Remaining SDK cells and/or successor checks are listed by their absence from cells."],
            "limitations": [
                "This injects a Redis SET acknowledgement loss only; it does not test etcd gRPC response loss, network partitions, Redis failover, or production conditions.",
                "TTL recovery is not proof that an expired process has stopped; the unknown-result caller must not enter the protected work.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Local, disposable redis:// endpoint.")
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-cancel-workers-seven.json")
    parser.add_argument("--lease-ms", type=int, default=5000)
    parser.add_argument("--cpp-wsl-distribution", default=os.environ.get("DILOCK_CPP_WSL_DISTRIBUTION", "Ubuntu-26.04"))
    parser.add_argument("--output", default="validation/implementation/m6-redis-lost-ack-seven-sdk.json")
    sys.path.insert(0, str(ROOT))
    sys.exit(asyncio.run(main(parser.parse_args())))
