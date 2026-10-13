"""Exercise delayed Redis acquisition commands against real Dilock workers.

The proxy consumes and drops the first target SET before Redis sees it.  It
records any client-level retry and later replays the delayed wire command only
after a successor owns the resource.  This proves both retry behavior and the
Redis NX protection against a stale acquisition arriving out of order.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from conformance.nway import safe_endpoint  # noqa: E402
from conformance.redis_fault_proxy import command_name, read_resp_frame  # noqa: E402
from conformance.run import Worker  # noqa: E402


class RedisOfflineQueueProxy:
    """Drop target SETs before Redis and retain exact bytes for delayed replay."""

    def __init__(self, upstream_host: str, upstream_port: int, target_key: bytes):
        self.upstream_host, self.upstream_port = upstream_host, upstream_port
        self.target_key = target_key
        self.server: asyncio.AbstractServer | None = None
        self.set_attempts = 0
        self.delayed: list[bytes] = []
        self.errors: list[str] = []
        self._writers: set[asyncio.StreamWriter] = set()
        self.first_target = asyncio.Event()

    @property
    def port(self) -> int:
        if self.server is None or not self.server.sockets:
            raise RuntimeError("proxy has not started")
        return int(self.server.sockets[0].getsockname()[1])

    async def start(self) -> RedisOfflineQueueProxy:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        writers = tuple(self._writers)
        for writer in writers:
            writer.close()
        await asyncio.gather(
            *(w.wait_closed() for w in writers), return_exceptions=True
        )
        self._writers.clear()

    async def replay(self) -> list[str]:
        """Send every captured target command after the caller has a successor."""
        outcomes: list[str] = []
        for raw in self.delayed:
            reader, writer = await asyncio.open_connection(
                self.upstream_host, self.upstream_port
            )
            try:
                writer.write(raw)
                await writer.drain()
                reply = await asyncio.wait_for(read_resp_frame(reader), timeout=3)
                if reply is None:
                    raise ConnectionError("Redis closed during delayed command replay")
                outcomes.append(f"{reply.kind.decode('ascii')}:{str(reply.value)!r}")
            finally:
                writer.close()
                await writer.wait_closed()
        return outcomes

    async def _handle(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(client_writer)
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.upstream_host, self.upstream_port
            )
            self._writers.add(upstream_writer)
            while frame := await read_resp_frame(client_reader):
                name = command_name(frame)
                values = frame.value if isinstance(frame.value, list) else []
                is_target_set = (
                    name == b"SET" and len(values) > 1 and values[1] == self.target_key
                )
                if is_target_set:
                    self.set_attempts += 1
                    self.delayed.append(frame.raw)
                    self.first_target.set()
                    # The command is accepted by the TCP proxy but is never sent
                    # upstream on this connection.  Closing forces the SDK's
                    # actual Redis client to handle a lost connection.
                    break
                upstream_writer.write(frame.raw)
                await upstream_writer.drain()
                reply = await read_resp_frame(upstream_reader)
                if reply is None:
                    break
                client_writer.write(reply.raw)
                await client_writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, BrokenPipeError):
            pass
        except Exception as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            client_writer.close()
            if upstream_writer is not None:
                upstream_writer.close()
            await asyncio.gather(
                client_writer.wait_closed(),
                *(
                    [upstream_writer.wait_closed()]
                    if upstream_writer is not None
                    else []
                ),
                return_exceptions=True,
            )
            self._writers.discard(client_writer)
            if upstream_writer is not None:
                self._writers.discard(upstream_writer)


class WslOfflineQueueProxy:
    """Host wrapper for a proxy colocated with WSL-native C++ workers."""

    endpoint_host = "127.0.0.1"

    def __init__(
        self,
        run_dir: Path,
        upstream_host: str,
        upstream_port: int,
        key: bytes,
        distro: str,
    ):
        self.run_dir, self.upstream_host, self.upstream_port = (
            run_dir,
            upstream_host,
            upstream_port,
        )
        self.key, self.distro = key, distro
        self.ready, self.control, self.result = (
            run_dir / f"cpp-proxy-{name}.json"
            for name in ("ready", "control", "result")
        )
        self.process: asyncio.subprocess.Process | None = None
        self._port: int | None = None
        self.result_data: dict | None = None

    @staticmethod
    def wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("WSL proxy is not ready")
        return self._port

    async def start(self) -> WslOfflineQueueProxy:
        script = self.wsl_path(ROOT / "conformance/redis_offline_queue.py")
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe",
            "-d",
            self.distro,
            "--",
            "python3",
            script,
            "--proxy-only",
            "--upstream-host",
            self.upstream_host,
            "--upstream-port",
            str(self.upstream_port),
            "--target-key-b64",
            base64.b64encode(self.key).decode("ascii"),
            "--ready-file",
            self.wsl_path(self.ready),
            "--control-file",
            self.wsl_path(self.control),
            "--result-file",
            self.wsl_path(self.result),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready.exists():
                self._port = int(
                    json.loads(self.ready.read_text(encoding="utf-8"))["port"]
                )
                return self
            if self.process.returncode is not None:
                raise RuntimeError(
                    f"WSL proxy exited during startup: {self.process.returncode}"
                )
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL proxy did not publish readiness")

    async def wait_target(self, timeout: float) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if self.result.exists():
                return
            # The ready JSON is updated atomically with target counters.
            try:
                ready = json.loads(self.ready.read_text(encoding="utf-8"))
                if ready.get("set_attempts", 0) > 0:
                    self.result_data = ready
                    return
            except (OSError, json.JSONDecodeError):
                pass
            await asyncio.sleep(0.05)
        raise TimeoutError("WSL proxy did not capture acquisition SET")

    async def snapshot(self) -> dict:
        if self.result.exists():
            self.result_data = json.loads(self.result.read_text(encoding="utf-8"))
        else:
            self.result_data = json.loads(self.ready.read_text(encoding="utf-8"))
        return self.result_data

    async def replay(self) -> list[str]:
        self.control.write_text(
            json.dumps({"action": "replay"}) + "\n", encoding="utf-8"
        )
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            try:
                result = json.loads(self.result.read_text(encoding="utf-8"))
                self.result_data = result
                return list(result["replay_outcomes"])
            except (OSError, json.JSONDecodeError, KeyError):
                await asyncio.sleep(0.05)
        raise TimeoutError("WSL proxy did not report delayed-command replay")

    async def close(self) -> None:
        if self.process is None or self.process.returncode is not None:
            return
        self.control.write_text(json.dumps({"action": "stop"}) + "\n", encoding="utf-8")
        try:
            await asyncio.wait_for(self.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()
        if self.result.exists():
            self.result_data = json.loads(self.result.read_text(encoding="utf-8"))


async def _launch_worker(
    sdk: str, config_path: Path, manifest: dict, run_dir: Path, env: dict
):
    argv = manifest[sdk]["argv"] + [str(config_path)]
    log = (run_dir / f"{sdk}.stderr.log").open("wb")
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=ROOT,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=log,
        start_new_session=(os.name != "nt"),
    )
    return Worker(process), log


def _sdk_config(sdk: str, endpoint: str, namespace: str, epoch: str) -> dict:
    if sdk != "cpp":
        return {
            "backend": "redis",
            "endpoint": endpoint,
            "namespace": namespace,
            "epoch": epoch,
        }
    parsed = urlsplit(endpoint)
    return {
        "backend": "redis",
        "redis": {
            "host": parsed.hostname,
            "port": parsed.port or 6379,
            "username": "",
            "password": "",
            "database": 0,
            "namespace": namespace,
            "coordination_epoch": epoch,
            "rpc_timeout_ms": 1000,
        },
    }


def _proxy_config(sdk: str, endpoint: str, namespace: str, epoch: str) -> dict:
    result = _sdk_config(sdk, endpoint, namespace, epoch)
    if sdk == "cpp":
        result["redis"]["rpc_timeout_ms"] = 2500
    return result


async def main(args: argparse.Namespace) -> int:
    from redis.asyncio import Redis
    from tools.provision import provision
    from tools.reference_identity import redis_key

    endpoint = args.endpoint
    upstream = urlsplit(endpoint)
    if upstream.scheme != "redis" or upstream.hostname not in (
        "127.0.0.1",
        "localhost",
        "::1",
    ):
        raise ValueError(
            "RDS-004 accepts only an uncredentialed loopback redis:// endpoint"
        )
    if upstream.username or upstream.password or upstream.path not in ("", "/", "/0"):
        raise ValueError(
            "test endpoint must not contain credentials or a nonzero database"
        )

    namespace, epoch = "rds004-" + uuid.uuid4().hex[:18], "rds004-local-1"
    lease_ms = args.lease_ms
    all_sdks = ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
    selected = args.sdks or all_sdks
    run_dir = ROOT / "validation/implementation" / ("m6-rds004-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    direct_endpoint = f"redis://{upstream.hostname}:{upstream.port or 6379}"
    proxies, workers, logs = {}, {}, []
    cells: list[dict] = []
    status, error = "failed", None
    redis_version = "unknown"
    resources = {sdk: "rds004/" + sdk + "/" + uuid.uuid4().hex for sdk in selected}
    try:
        await provision(endpoint, namespace, epoch)
        for sdk in selected:
            key = redis_key(namespace, resources[sdk]).encode("utf-8")
            if sdk == "cpp":
                proxy = await WslOfflineQueueProxy(
                    run_dir,
                    "127.0.0.1",
                    upstream.port or 6379,
                    key,
                    args.cpp_wsl_distribution,
                ).start()
            else:
                proxy = await RedisOfflineQueueProxy(
                    upstream.hostname, upstream.port or 6379, key
                ).start()
            proxies[sdk] = proxy
            proxy_endpoint = f"redis://127.0.0.1:{proxy.port}"
            config = run_dir / f"{sdk}.json"
            config.write_text(
                json.dumps(_proxy_config(sdk, proxy_endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(
                sdk, config, manifest, run_dir, os.environ.copy()
            )
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=60)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        observer_config = run_dir / "observer.json"
        observer_config.write_text(
            json.dumps(_sdk_config("dotnet", direct_endpoint, namespace, epoch)) + "\n",
            encoding="utf-8",
        )
        observer, observer_log = await _launch_worker(
            "dotnet", observer_config, manifest, run_dir, os.environ.copy()
        )
        logs.append(observer_log)
        hello = await observer.request("hello", timeout=30)
        if hello.get("status") != "ok":
            raise RuntimeError("direct successor observer failed to start")

        direct = Redis.from_url(endpoint)
        try:
            redis_info = await direct.info("server")
            redis_version = str(redis_info.get("redis_version", "unknown"))
            for sdk in selected:
                worker, proxy = workers[sdk], proxies[sdk]
                resource, key = resources[sdk], redis_key(namespace, resources[sdk])
                request_id, future = await worker.start_request(
                    "try_acquire",
                    resource=resource,
                    lease_ms=lease_ms,
                    rpc_ms=2500,
                    auto_renew=False,
                )
                if isinstance(proxy, WslOfflineQueueProxy):
                    await proxy.wait_target(8)
                else:
                    await asyncio.wait_for(proxy.first_target.wait(), timeout=8)
                # Do not wait for an arbitrary SDK retry policy to exhaust: the
                # network fault has already been observed at the target command.
                try:
                    old = await asyncio.wait_for(future, timeout=8)
                finally:
                    worker.responses.pop(request_id, None)
                await asyncio.sleep(args.retry_observation_seconds)

                cell = {
                    "sdk": sdk,
                    "status": "passed",
                    "old_attempt_status": old.get("status"),
                    "old_attempt_created_local_lease": bool(old.get("lease_id")),
                    "request_id": request_id,
                    "resource": resource,
                }
                if old.get("status") not in (
                    "acquisition_unknown",
                    "backend_unavailable",
                    "timeout",
                ):
                    cell.update(
                        status="failed",
                        error="faulted acquisition was incorrectly reported as success/ordinary busy",
                    )
                if old.get("lease_id"):
                    cell.update(
                        status="failed",
                        error="an unacknowledged acquisition created a locally usable lease",
                    )
                if isinstance(proxy, WslOfflineQueueProxy):
                    snapshot = await proxy.snapshot()
                    replay_frames = int(snapshot.get("set_attempts", 0))
                    retry_count = replay_frames
                    proxy_errors = list(snapshot.get("errors", []))
                else:
                    retry_count, proxy_errors = proxy.set_attempts, proxy.errors
                cell["target_set_attempts_before_successor"] = retry_count
                cell["proxy_errors"] = proxy_errors
                if retry_count is None or retry_count < 1:
                    cell.update(
                        status="failed",
                        error="no target SET was captured by the fault proxy",
                    )
                if retry_count is not None and retry_count > 1:
                    cell.update(
                        status="failed",
                        error="Redis client resent the same ambiguous acquisition after disconnect",
                    )
                if proxy_errors:
                    cell.update(
                        status="failed",
                        error="proxy reported protocol or forwarding errors",
                    )

                # Since the SDK did not receive an acquisition acknowledgement,
                # a direct instance acquires as successor before queued bytes are
                # replayed.  The exact captured SET must receive nil under NX.
                successor = await observer.request(
                    "try_acquire",
                    resource=resource,
                    lease_ms=lease_ms,
                    rpc_ms=1500,
                    auto_renew=False,
                    timeout=8,
                )
                if successor.get("status") != "acquired":
                    cell.update(
                        status="failed",
                        successor_status=successor.get("status"),
                        error="successor could not acquire while delayed command was held",
                    )
                else:
                    before_ttl = await direct.pttl(key)
                    if isinstance(proxy, WslOfflineQueueProxy):
                        outcomes = await proxy.replay()
                    else:
                        outcomes = await proxy.replay()
                    after_ttl = await direct.pttl(key)
                    after_value = await direct.get(key)
                    cell["delayed_command_replay"] = outcomes
                    cell["successor_ttl_ms_before_replay"] = before_ttl
                    cell["successor_ttl_ms_after_replay"] = after_ttl
                    cell["successor_value_preserved"] = (
                        after_value is not None
                        and before_ttl > 0
                        and after_ttl > 0
                        and after_ttl <= before_ttl
                    )
                    if not outcomes or any(
                        outcome.startswith(":1") or outcome.startswith("+OK")
                        for outcome in outcomes
                    ):
                        cell.update(
                            status="failed",
                            error="delayed acquisition command unexpectedly changed Redis ownership",
                        )
                    if not cell["successor_value_preserved"]:
                        cell.update(
                            status="failed",
                            error="successor owner or TTL changed after delayed command replay",
                        )
                    await asyncio.sleep(args.retry_observation_seconds)
                    if isinstance(proxy, WslOfflineQueueProxy):
                        final_set_attempts = int(
                            (await proxy.snapshot()).get("set_attempts", 0)
                        )
                    else:
                        final_set_attempts = proxy.set_attempts
                    cell["target_set_attempts_after_delayed_replay"] = (
                        final_set_attempts
                    )
                    if final_set_attempts != retry_count:
                        cell.update(
                            status="failed",
                            error="Redis client sent another acquisition after successor ownership was established",
                        )
                    release = await observer.request(
                        "release", lease_id=successor["lease_id"], timeout=8
                    )
                    cell["successor_release"] = release.get("status")
                    if release.get("status") != "released":
                        cell.update(
                            status="failed", error="successor cleanup did not complete"
                        )
                cells.append(cell)
                print(json.dumps(cell), flush=True)
                if cell["status"] != "passed":
                    # Continue with independent SDK cells while preserving this failure.
                    continue
            status = (
                "passed"
                if len(cells) == len(selected)
                and all(c["status"] == "passed" for c in cells)
                else "failed"
            )
        finally:
            await direct.aclose()
            await observer.close()
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        status = (
            "blocked"
            if isinstance(exc, (FileNotFoundError, PermissionError))
            else "failed"
        )
    finally:
        await asyncio.gather(
            *(worker.close() for worker in workers.values()), return_exceptions=True
        )
        for proxy in proxies.values():
            await proxy.close()
        for log in logs:
            log.close()
        report = {
            "id": "RDS-004-REDIS-OFFLINE-QUEUE-RETRY-SEVEN-SDK",
            "date": time.strftime("%Y-%m-%d"),
            "status": status,
            "scenario": "Drop target acquisition SETs before Redis sees them, observe client retries, acquire a successor, then replay captured stale bytes and prove SET NX leaves successor value/TTL intact.",
            "backend": f"Redis {redis_version}; one disposable standalone instance.",
            "endpoint": safe_endpoint(endpoint),
            "namespace": namespace,
            "lease_ms": lease_ms,
            "participants": selected,
            "real_processes": True,
            "real_backend": True,
            "cells": cells,
            "sdk_statuses": {
                sdk: next(
                    (cell["status"] for cell in cells if cell["sdk"] == sdk), "not-run"
                )
                for sdk in all_sdks
            },
            "error": error,
            "run_directory": str(run_dir),
            "not_run": [
                sdk for sdk in selected if sdk not in {c["sdk"] for c in cells}
            ],
            "skipped": [],
            "blocked": [],
            "diagnostic_attempts": [
                {
                    "attempt": "initial direct script invocation",
                    "status": "failed then corrected",
                    "error": "ModuleNotFoundError: conformance; the script directory, not repository root, was on sys.path",
                    "fix": "inserted the resolved repository root before importing conformance modules",
                },
                {
                    "attempt": "first fake-Redis proxy assertion",
                    "status": "failed then corrected",
                    "error": "nullable RESP bulk parsed as Python None but the expected repr was incorrectly formatted",
                    "fix": "updated the deterministic assertion to match the parser's captured value representation",
                },
                {
                    "attempt": "initial Ruff check",
                    "status": "failed then corrected",
                    "error": "E402 for repository imports after explicit sys.path setup",
                    "fix": "marked the required post-bootstrap imports with E402 noqa comments",
                },
            ],
            "limitations": [
                "The proxy stores exact SET frames after the client sends them and replays those frames after a successor acquires; it does not introspect undocumented client queue internals.",
                "This tests one loopback standalone Redis authority and one connection-loss point per SDK; it does not establish Redis failover safety or production network behavior.",
                "A client reporting acquisition_unknown or backend_unavailable is not authorized to perform protected work; TTL/owner checks remain required for application fencing.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


async def proxy_only(args: argparse.Namespace) -> int:
    key = base64.b64decode(args.target_key_b64)
    proxy = await RedisOfflineQueueProxy(
        args.upstream_host, args.upstream_port, key
    ).start()
    ready = Path(args.ready_file)
    control, result = Path(args.control_file), Path(args.result_file)
    try:
        ready.write_text(
            json.dumps({"port": proxy.port, "set_attempts": 0}) + "\n", encoding="utf-8"
        )
        action_seen = ""
        while action_seen != "stop":
            if control.exists():
                try:
                    action_seen = json.loads(control.read_text(encoding="utf-8")).get(
                        "action", ""
                    )
                except (OSError, json.JSONDecodeError):
                    action_seen = ""
                if action_seen == "replay":
                    outcomes = await proxy.replay()
                    result.write_text(
                        json.dumps(
                            {
                                "set_attempts": proxy.set_attempts,
                                "replay_outcomes": outcomes,
                                "errors": proxy.errors,
                            },
                            indent=2,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    control.unlink(missing_ok=True)
            ready.write_text(
                json.dumps({"port": proxy.port, "set_attempts": proxy.set_attempts})
                + "\n",
                encoding="utf-8",
            )
            await asyncio.sleep(0.05)
    finally:
        await proxy.close()
        if not result.exists():
            result.write_text(
                json.dumps(
                    {
                        "set_attempts": proxy.set_attempts,
                        "errors": proxy.errors,
                        "replay_outcomes": [],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", help="Local disposable Redis endpoint")
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default=".tools/m6-cancel-workers-seven.json")
    parser.add_argument("--lease-ms", type=int, default=8000)
    parser.add_argument("--retry-observation-seconds", type=float, default=0.4)
    parser.add_argument(
        "--cpp-wsl-distribution",
        default=os.environ.get("DILOCK_CPP_WSL_DISTRIBUTION", "Ubuntu-26.04"),
    )
    parser.add_argument(
        "--output",
        default="validation/implementation/m6-rds004-redis-offline-queue-seven-sdk.json",
    )
    parser.add_argument("--proxy-only", action="store_true")
    parser.add_argument("--upstream-host")
    parser.add_argument("--upstream-port", type=int)
    parser.add_argument("--target-key-b64")
    parser.add_argument("--ready-file")
    parser.add_argument("--control-file")
    parser.add_argument("--result-file")
    parsed = parser.parse_args()
    if parsed.proxy_only:
        sys.exit(asyncio.run(proxy_only(parsed)))
    if not parsed.endpoint:
        parser.error("--endpoint is required unless --proxy-only is set")
    sys.exit(asyncio.run(main(parsed)))
