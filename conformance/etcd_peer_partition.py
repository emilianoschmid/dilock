"""Exercise etcd peer partitions, including an identity-verified directed split."""

from __future__ import annotations

import argparse
import asyncio
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import ssl
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
IS_WINDOWS = os.name == "nt"
sys.path.insert(0, str(ROOT))

# The path bootstrap above supports direct execution as `python conformance/...py`.
# ruff: noqa: E402
from conformance.etcd_leader_failover import cluster_status, wait_consistent_leader
from conformance.etcd_lost_ack import etcd_sdk_config, lock_key_count, lock_prefix
from conformance.lost_ack import _launch_worker
from conformance.nway import safe_endpoint
from conformance.run import Worker, spawn_process


ETCD_WINDOWS_DIR = ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64"
ETCD_WINDOWS_BINARY = ETCD_WINDOWS_DIR / "etcd.exe"
ETCD_WINDOWS_CTL = ETCD_WINDOWS_DIR / "etcdctl.exe"
GO_WINDOWS_BINARY = ROOT / ".tools/go1.27.1/go/bin/go.exe"
WINDOWS_CPP_WORKERS = (
    ROOT / "build/cpp/Release/dilock_cpp_worker.exe",
    ROOT / "build/cpp/dilock_cpp_worker.exe",
)
DOCKER_CPP_WORKER_IMAGE = "debian@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f"


def preflight_docker_cpp_worker(worker: Path) -> dict:
    """Fail closed unless the pinned runtime, current worker and its libraries exist locally."""
    if not worker.is_file():
        raise FileNotFoundError(f"Docker C++ etcd worker is missing: {worker}")
    required = (
        ROOT / ".tools/grpc-root/usr/lib/x86_64-linux-gnu",
        ROOT / ".tools/cppinstall/lib",
        ROOT / ".tools/cppdeps/usr/lib/x86_64-linux-gnu",
        ROOT / ".tools/openssl-runtime/usr/lib/x86_64-linux-gnu",
    )
    missing = [str(path) for path in required if not path.is_dir()]
    if missing:
        raise FileNotFoundError(f"Docker C++ etcd runtime libraries are missing: {missing}")
    image = subprocess.run(
        ["docker", "image", "inspect", DOCKER_CPP_WORKER_IMAGE, "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if image.returncode != 0:
        raise RuntimeError(f"pinned local Docker image is unavailable: {image.stderr.strip()}")
    if image.stdout.strip() != "sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f":
        raise RuntimeError(f"Docker image resolved to an unexpected image ID: {image.stdout.strip()}")
    return {
        "status": "passed",
        "image": DOCKER_CPP_WORKER_IMAGE,
        "image_id": image.stdout.strip(),
        "worker": str(worker),
        "runtime_library_directories": [str(path) for path in required],
        "network": "Docker default bridge; Windows endpoints are reached via host.docker.internal",
    }


def windows_fixed_ports(base_port: int) -> list[int]:
    """Return the six fixed Windows listener ports after validating the range."""
    ports = [base_port, base_port + 10, base_port + 20]
    ports.extend(port + 1 for port in tuple(ports))
    if any(port < 1 or port > 65535 for port in ports) or len(set(ports)) != 6:
        raise ValueError("base port must leave six distinct ports in the TCP range")
    return ports


def preflight_windows_ports(base_port: int) -> list[int]:
    """Read-only availability probe: exclusively bind, then immediately release each port."""
    ports = windows_fixed_ports(base_port)
    occupied: list[int] = []
    for port in ports:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            probe.bind(("127.0.0.1", port))
        except OSError:
            occupied.append(port)
        finally:
            probe.close()
    if occupied:
        raise OSError(f"refusing to start cluster; Windows loopback ports are occupied: {occupied}")
    return ports


def preflight_windows_toolchain(
    base_port: int,
    etcd_binary: Path = ETCD_WINDOWS_BINARY,
    etcdctl: Path = ETCD_WINDOWS_CTL,
    go_binary: Path = GO_WINDOWS_BINARY,
) -> dict:
    """Validate installed Windows executables and flags without starting services."""
    required = (etcd_binary, etcdctl, go_binary)
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"required Windows test binaries are missing: {missing}")

    version = subprocess.run(
        [str(etcd_binary), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if "etcd Version: 3.7.2" not in version or "Go OS/Arch: windows/amd64" not in version:
        raise RuntimeError(f"unsupported Windows etcd binary: {version}")
    ctl_version = subprocess.run(
        [str(etcdctl), "version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if "etcdctl version: 3.7.2" not in ctl_version:
        raise RuntimeError(f"unsupported Windows etcdctl binary: {ctl_version}")
    peer_help = subprocess.run(
        [str(etcd_binary), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    help_text = peer_help.stdout + peer_help.stderr
    peer_flags = (
        "--listen-peer-urls",
        "--initial-advertise-peer-urls",
        "--initial-cluster-state",
        "--peer-cert-file",
        "--peer-key-file",
        "--peer-trusted-ca-file",
        "--peer-client-cert-auth",
    )
    missing_flags = [flag for flag in peer_flags if flag not in help_text]
    if missing_flags:
        raise RuntimeError(f"Windows etcd lacks required peer flags: {missing_flags}")
    go_version = subprocess.run(
        [str(go_binary), "version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    ports = preflight_windows_ports(base_port)
    return {
        "status": "passed",
        "backend": "windows-native",
        "etcd_version": version,
        "etcdctl_version": ctl_version,
        "go_version": go_version,
        "peer_mtls_flags": list(peer_flags),
        "fixed_loopback_ports": ports,
        "services_started": False,
    }


class WslPeerPartitionProxy:
    """Etcd-peer TCP proxy controlled by a synchronous localhost JSON channel."""

    def __init__(
        self,
        run_dir: Path,
        distro: str,
        upstream: str,
        binary_path: Path | None = None,
        identity_member: str | None = None,
        identity_peer_ids: list[str] | None = None,
        identity_cert_dir: Path | None = None,
    ):
        self.run_dir = run_dir
        self.distro = distro
        self.upstream = upstream
        self.binary_path = binary_path or ROOT / ".tools/m6-etcd-peer-partition-proxy-linux"
        self.identity_member = identity_member
        self.identity_peer_ids = identity_peer_ids or []
        self.identity_cert_dir = identity_cert_dir
        self.ready_path = run_dir / "proxy-ready.json"
        self.state_path = run_dir / "proxy-state.json"
        self.result_path = run_dir / "proxy-result.json"
        self.stop_path = run_dir / "proxy-stop"
        self.process: asyncio.subprocess.Process | None = None
        self.port: int | None = None
        self.control_port: int | None = None
        self.last_snapshot: dict | None = None
        self.exit_detail: str | None = None

    async def _exit_reason(self) -> str:
        if self.exit_detail is None:
            detail = ""
            if self.process is not None and self.process.stderr is not None:
                detail = (await self.process.stderr.read()).decode(
                    "utf-8", errors="replace"
                ).strip()[-4000:]
            self.exit_detail = detail or "stderr was empty"
        return self.exit_detail

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    async def start(self) -> None:
        binary = self._wsl_path(self.binary_path)
        command = [
            "wsl.exe",
            "-d",
            self.distro,
            "--",
            binary,
            "--listen-host",
            "127.0.0.1",
            "--control-host",
            "127.0.0.1",
            "--upstream",
            self.upstream,
            "--ready-file",
            self._wsl_path(self.ready_path),
            "--state-file",
            self._wsl_path(self.state_path),
            "--result-file",
            self._wsl_path(self.result_path),
            "--stop-file",
            self._wsl_path(self.stop_path),
        ]
        if self.identity_member is not None:
            if self.identity_cert_dir is None or not self.identity_peer_ids:
                raise RuntimeError("identity proxy requires source IDs and its ephemeral certificate directory")
            command.extend(
                [
                    "--identity-member-id",
                    self.identity_member,
                    "--identity-peer-ids",
                    ",".join(self.identity_peer_ids),
                    "--identity-ca-file",
                    self._wsl_path(self.identity_cert_dir / "ca.crt"),
                    "--identity-cert-dir",
                    self._wsl_path(self.identity_cert_dir),
                ]
            )
        self.process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready_path.exists() and self.state_path.exists():
                ready = json.loads(self.ready_path.read_text(encoding="utf-8"))
                self.port = int(ready["port"])
                self.control_port = int(ready["control_port"])
                self.last_snapshot = json.loads(
                    self.state_path.read_text(encoding="utf-8")
                )
                return
            if self.process.returncode is not None:
                detail = ""
                if self.process.stderr is not None:
                    detail = (
                        await self.process.stderr.read()
                    ).decode("utf-8", errors="replace").strip()
                raise RuntimeError(
                    f"etcd peer proxy exited at startup: {self.process.returncode}; {detail}"
                )
            await asyncio.sleep(0.05)
        raise TimeoutError("etcd peer proxy did not publish readiness")

    @property
    def endpoint(self) -> str:
        if self.port is None:
            raise RuntimeError("etcd peer proxy is not ready")
        scheme = "https" if self.identity_member is not None else "http"
        return f"{scheme}://127.0.0.1:{self.port}"

    def _with_snapshot_peer_events(self, state: dict) -> dict:
        """Use the bounded authenticated event tail returned by the proxy control channel."""
        if not getattr(self, "identity_member", None):
            return state
        events = state.get("peer_events")
        if not isinstance(events, list):
            raise RuntimeError(
                f"identity proxy control reply omitted peer events for {self.identity_member}"
            )
        return {**state, "peer_events": events}

    async def control(self, command: str, peer_id: str | None = None) -> dict:
        if self.control_port is None:
            raise RuntimeError("etcd peer proxy control channel is not ready")
        if self.process is not None and self.process.returncode is not None:
            detail = await self._exit_reason()
            raise RuntimeError(
                f"etcd peer proxy exited: {self.process.returncode}; {detail}"
            )
        request_id = uuid.uuid4().hex
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", self.control_port), timeout=3
        )
        try:
            request = {"request_id": request_id, "command": command}
            if peer_id is not None:
                request["peer_id"] = peer_id
            writer.write(json.dumps(request).encode() + b"\n")
            await writer.drain()
            raw = await asyncio.wait_for(reader.readline(), timeout=3)
            if not raw:
                raise RuntimeError("peer proxy control channel closed without acknowledgment")
            response = json.loads(raw)
            if response.get("request_id") != request_id:
                raise RuntimeError(
                    f"peer proxy control acknowledgment ID mismatch: {response}"
                )
            if response.get("status") != "ok":
                raise RuntimeError(f"peer proxy rejected control command: {response}")
            if not isinstance(response.get("state"), dict):
                raise RuntimeError(f"peer proxy acknowledgment omitted state: {response}")
            self.last_snapshot = self._with_snapshot_peer_events(response["state"])
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    async def wait_state(self, predicate, timeout: float = 8) -> dict:
        deadline = asyncio.get_running_loop().time() + timeout
        last_error = "not attempted"
        while asyncio.get_running_loop().time() < deadline:
            try:
                response = await self.control("status")
                state = self._with_snapshot_peer_events(response["state"])
                self.last_snapshot = state
                if predicate(state):
                    return state
                last_error = json.dumps(state, sort_keys=True)
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if self.process is not None and self.process.returncode is not None:
                detail = await self._exit_reason()
                raise RuntimeError(
                    f"etcd peer proxy exited: {self.process.returncode}; {detail}"
                )
            await asyncio.sleep(0.025)
        raise TimeoutError(f"etcd peer proxy state did not converge: {last_error}")

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


class WindowsPeerPartitionProxy(WslPeerPartitionProxy):
    """Run the same identity-aware proxy as a directly owned Windows child process."""

    async def start(self) -> None:
        if not IS_WINDOWS:
            raise RuntimeError("the Windows-native etcd harness requires Windows")
        if self.binary_path is None or not self.binary_path.is_file():
            raise FileNotFoundError(f"Windows peer proxy binary is missing: {self.binary_path}")
        command = [
            str(self.binary_path),
            "--listen-host", "127.0.0.1",
            "--control-host", "127.0.0.1",
            "--upstream", self.upstream,
            "--ready-file", str(self.ready_path),
            "--state-file", str(self.state_path),
            "--result-file", str(self.result_path),
            "--stop-file", str(self.stop_path),
        ]
        if self.identity_member is not None:
            if self.identity_cert_dir is None or not self.identity_peer_ids:
                raise RuntimeError("identity proxy requires source IDs and its ephemeral certificate directory")
            command.extend([
                "--identity-member-id", self.identity_member,
                "--identity-peer-ids", ",".join(self.identity_peer_ids),
                "--identity-ca-file", str(self.identity_cert_dir / "ca.crt"),
                "--identity-cert-dir", str(self.identity_cert_dir),
            ])
        self.process = await spawn_process(
            *command,
            popen_on_windows=True,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            if self.ready_path.exists() and self.state_path.exists():
                ready = json.loads(self.ready_path.read_text(encoding="utf-8"))
                self.port = int(ready["port"])
                self.control_port = int(ready["control_port"])
                self.last_snapshot = json.loads(self.state_path.read_text(encoding="utf-8"))
                return
            if self.process.returncode is not None:
                detail = ""
                if self.process.stderr is not None:
                    detail = (await self.process.stderr.read()).decode("utf-8", errors="replace").strip()
                raise RuntimeError(
                    f"Windows etcd peer proxy exited at startup: {self.process.returncode}; {detail}"
                )
            await asyncio.sleep(0.05)
        raise TimeoutError("Windows etcd peer proxy did not publish readiness")


class PeerPartitionMember:
    """Own one local etcd member with a proxied, independently cuttable peer URL."""

    def __init__(
        self,
        name: str,
        client_port: int,
        peer_port: int,
        run_dir: Path,
        distro: str,
        cluster: str,
        advertised_peer_url: str,
        peer_tls: bool = False,
        peer_cert_dir: Path | None = None,
        backend: str = "wsl",
        etcd_binary: Path | None = None,
    ):
        self.name = name
        self.client_port = client_port
        self.peer_port = peer_port
        self.run_dir = run_dir
        self.distro = distro
        self.cluster = cluster
        self.advertised_peer_url = advertised_peer_url
        self.peer_tls = peer_tls
        self.peer_cert_dir = peer_cert_dir
        self.backend = backend
        self.etcd_binary = etcd_binary
        self.data_dir = run_dir / f"{name}-data"
        self.log_path = run_dir / f"{name}.log"
        self.process: asyncio.subprocess.Process | None = None
        self.pid: int | None = None
        self.starts = 0
        self.stops = 0
        self.log_handle = None

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.client_port}"

    @property
    def peer_listen_url(self) -> str:
        scheme = "https" if self.peer_tls else "http"
        return f"{scheme}://127.0.0.1:{self.peer_port}"

    @staticmethod
    def _wsl_path(path: Path) -> str:
        drive = path.drive.rstrip(":").lower()
        tail = path.as_posix().split(":", 1)[1].lstrip("/")
        return f"/mnt/{drive}/{tail}"

    async def start(self) -> None:
        if self.process is not None and self.process.returncode is None:
            raise RuntimeError(f"etcd member {self.name} is already running")
        if self.backend == "windows":
            if not IS_WINDOWS:
                raise RuntimeError("the Windows-native etcd harness requires Windows")
            if self.etcd_binary is None or not self.etcd_binary.is_file():
                raise FileNotFoundError(f"Windows etcd binary is missing: {self.etcd_binary}")
            args = [
                str(self.etcd_binary),
                "--name", self.name,
                "--data-dir", str(self.data_dir),
                "--listen-client-urls", self.endpoint,
                "--advertise-client-urls", self.endpoint,
                "--listen-peer-urls", self.peer_listen_url,
                "--initial-advertise-peer-urls", self.advertised_peer_url,
                "--initial-cluster", self.cluster,
                "--initial-cluster-state", "new",
            ]
            if self.peer_tls:
                if self.peer_cert_dir is None:
                    raise RuntimeError(
                        f"etcd member {self.name} requires an ephemeral peer certificate directory"
                    )
                args.extend([
                    "--peer-cert-file", str(self.peer_cert_dir / f"{self.name}.crt"),
                    "--peer-key-file", str(self.peer_cert_dir / f"{self.name}.key"),
                    "--peer-trusted-ca-file", str(self.peer_cert_dir / "ca.crt"),
                    "--peer-client-cert-auth=true",
                ])
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.log_handle = self.log_path.open("ab")
            try:
                self.process = await spawn_process(
                    *args,
                    popen_on_windows=True,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=self.log_handle,
                    stderr=asyncio.subprocess.STDOUT,
                )
            except BaseException:
                self.log_handle.close()
                self.log_handle = None
                raise
            self.pid = self.process.pid
            self.starts += 1
            return
        binary = self._wsl_path(ROOT / ".tools/etcd-linux/etcd")
        args = [
            binary,
            "--name",
            self.name,
            "--data-dir",
            self._wsl_path(self.data_dir),
            "--listen-client-urls",
            self.endpoint,
            "--advertise-client-urls",
            self.endpoint,
            "--listen-peer-urls",
            self.peer_listen_url,
            "--initial-advertise-peer-urls",
            self.advertised_peer_url,
            "--initial-cluster",
            self.cluster,
            "--initial-cluster-state",
            "new",
        ]
        if self.peer_tls:
            if self.peer_cert_dir is None:
                raise RuntimeError(
                    f"etcd member {self.name} requires an ephemeral peer certificate directory"
                )
            args.extend(
                [
                    "--peer-cert-file",
                    self._wsl_path(self.peer_cert_dir / f"{self.name}.crt"),
                    "--peer-key-file",
                    self._wsl_path(self.peer_cert_dir / f"{self.name}.key"),
                    "--peer-trusted-ca-file",
                    self._wsl_path(self.peer_cert_dir / "ca.crt"),
                    "--peer-client-cert-auth=true",
                ]
            )
        script = f"echo $$; exec {shlex.join(args)} >>{shlex.quote(self._wsl_path(self.log_path))} 2>&1"
        self.process = await asyncio.create_subprocess_exec(
            "wsl.exe",
            "-d",
            self.distro,
            "--",
            "bash",
            "-c",
            script,
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
                raise RuntimeError(
                    f"etcd member {self.name} stopped during startup: {last_error}"
                )
            try:
                result = await asyncio.to_thread(
                    subprocess.run,
                    [etcdctl, f"--endpoints={self.endpoint}", "endpoint", "health"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                if "is healthy" in result.stdout:
                    return
                last_error = result.stdout.strip()
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(0.2)
        raise TimeoutError(
            f"etcd member {self.name} did not become healthy: {last_error}"
        )

    async def stop(self) -> None:
        if self.process is None:
            return
        if self.backend == "windows":
            # This is the directly spawned etcd.exe child, so terminate/kill are scoped
            # to the process owned by this harness; no PID-wide taskkill is used.
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout=15)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await asyncio.wait_for(self.process.wait(), timeout=10)
                self.stops += 1
            if self.log_handle is not None:
                self.log_handle.close()
                self.log_handle = None
            return
        if self.process.returncode is not None:
            return
        if self.pid is None:
            raise RuntimeError(f"etcd member {self.name} has no owned WSL PID")
        stop = await asyncio.create_subprocess_exec(
            "wsl.exe",
            "-d",
            self.distro,
            "--",
            "kill",
            "-TERM",
            str(self.pid),
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
                "wsl.exe",
                "-d",
                self.distro,
                "--",
                "kill",
                "-KILL",
                str(self.pid),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await force.wait()
            await asyncio.wait_for(self.process.wait(), timeout=10)
        self.stops += 1


async def wait_partitioned(
    proxies: list[WslPeerPartitionProxy], timeout: float
) -> list[dict]:
    states = []
    for proxy in proxies:
        states.append(
            await proxy.wait_state(
                lambda value: (
                    value.get("partitioned") is True
                    and value.get("active_connections") == 0
                ),
                timeout=timeout,
            )
        )
    return states


async def wait_reconnects_rejected(
    proxies: list[WslPeerPartitionProxy], timeout: float
) -> list[dict]:
    """Observe a fresh Raft peer connection rejected by every cut proxy."""
    states = []
    for proxy in proxies:
        states.append(
            await proxy.wait_state(
                lambda value: (
                    value.get("partitioned") is True
                    and value.get("active_connections") == 0
                    and value.get("blocked_connections", 0) > 0
                ),
                timeout=timeout,
            )
        )
    return states


async def set_peer_partition(proxy: WslPeerPartitionProxy, enabled: bool) -> dict:
    """Apply a peer-path fault and require a synchronous proxy-state acknowledgment."""
    command = "partition" if enabled else "heal"
    acknowledged = await proxy.control(command)
    snapshot = acknowledged["state"]
    if snapshot.get("partitioned") is not enabled:
        raise RuntimeError(
            f"peer proxy acknowledged {command} with unexpected state: {acknowledged}"
        )
    if enabled:
        snapshot = await proxy.wait_state(
            lambda value: value.get("active_connections") == 0,
            timeout=8,
        )
    return {
        "action": command,
        "request_id": acknowledged["request_id"],
        "acknowledged": True,
        "state": snapshot,
    }


async def set_peer_acl(
    proxy: WslPeerPartitionProxy, source_member: str, blocked: bool
) -> dict:
    """Change one source-to-destination edge and require the exact ACL snapshot."""
    command = "block-peer" if blocked else "allow-peer"
    response = await proxy.control(command, peer_id=source_member)
    state = response["state"]
    if state.get("identity_member") != proxy.identity_member:
        raise RuntimeError(
            f"peer proxy identity does not match directed ACL destination: {response}"
        )
    allowed = state.get("allowed_peers")
    if not isinstance(allowed, list):
        raise RuntimeError(f"identity proxy omitted allowed peer identities: {response}")
    is_allowed = source_member in allowed
    if is_allowed == blocked:
        raise RuntimeError(
            f"proxy {proxy.identity_member} acknowledged {command} but ACL disagrees: {response}"
        )
    if blocked:
        state = await proxy.wait_state(
            lambda value: value.get("active_by_peer", {}).get(source_member) == 0,
            timeout=8,
        )
    return {
        "source": source_member,
        "destination": proxy.identity_member,
        "action": command,
        "request_id": response["request_id"],
        "acknowledged": True,
        "allowed_peers": allowed,
        "state": state,
    }


async def probe_directed_peer_edge(
    proxy: WslPeerPartitionProxy,
    source_member: str,
    destination_member: str,
    cert_dir: Path,
    expected_fingerprint: str,
    should_forward: bool,
    timeout: float,
) -> dict:
    """Exercise a real mTLS front handshake and await its authenticated proxy event."""
    if source_member == destination_member:
        raise ValueError("self peer links are invalid and are not part of the matrix")
    if proxy.identity_member != destination_member or proxy.port is None:
        raise RuntimeError("directed peer probe was given the wrong or unready destination proxy")
    before = proxy.last_snapshot or {}
    before_events = before.get("peer_events", [])
    before_sequence = max(
        (event.get("sequence", 0) for event in before_events), default=0
    )
    counters = before.get("allowed_by_peer" if should_forward else "blocked_by_peer", {})
    before_count = counters.get(source_member, 0) if isinstance(counters, dict) else 0

    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile=str(cert_dir / "ca.crt")
    )
    context.load_cert_chain(
        certfile=str(cert_dir / f"{source_member}.crt"),
        keyfile=str(cert_dir / f"{source_member}.key"),
    )
    context.set_alpn_protocols(["h2"])
    handshake_result = "completed"
    handshake_error = None
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                "127.0.0.1",
                proxy.port,
                ssl=context,
                server_hostname=destination_member,
                ssl_handshake_timeout=5,
            ),
            timeout=6,
        )
        # The proxy must complete its upstream TLS identity check before this
        # short probe closes; no etcd protocol bytes are sent.
        await asyncio.sleep(0.05)
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, ssl.SSLError):
            pass
    except (OSError, ssl.SSLError, asyncio.TimeoutError) as exc:
        handshake_result = "rejected_or_closed"
        handshake_error = f"{type(exc).__name__}: {exc}"

    expected_decision = "forwarded" if should_forward else "blocked"

    def observed(state: dict) -> bool:
        counters = state.get("allowed_by_peer" if should_forward else "blocked_by_peer", {})
        count = counters.get(source_member, 0) if isinstance(counters, dict) else 0
        events = state.get("peer_events", [])
        return count > before_count and any(
            event.get("sequence", 0) > before_sequence
            and event.get("destination") == destination_member
            and event.get("identity") == source_member
            and event.get("certificate_sha256") == expected_fingerprint
            and event.get("identity_verified") is True
            and event.get("decision") == expected_decision
            for event in events
        )

    state = await proxy.wait_state(observed, timeout=timeout)
    event = next(
        event
        for event in reversed(state["peer_events"])
        if event.get("sequence", 0) > before_sequence
        and event.get("destination") == destination_member
        and event.get("identity") == source_member
        and event.get("certificate_sha256") == expected_fingerprint
        and event.get("decision") == expected_decision
    )
    return {
        "source": source_member,
        "destination": destination_member,
        "status": "passed",
        "expected": expected_decision,
        "decision": event["decision"],
        "identity_verified": event["identity_verified"],
        "certificate_sha256": event["certificate_sha256"],
        "event_sequence": event["sequence"],
        "front_handshake": handshake_result,
        "front_handshake_error": handshake_error,
    }


async def verify_directed_peer_matrix(
    proxies: list[WslPeerPartitionProxy],
    names: list[str],
    cert_dir: Path,
    fingerprints: dict[str, str],
    expected: dict[tuple[str, str], bool],
    timeout: float,
) -> dict:
    """Require exactly the six non-self directed edges for a three-member cluster."""
    if len(names) != 3 or len(proxies) != 3:
        raise RuntimeError("majority/minority peer matrix requires exactly three members and proxies")
    if len(expected) != 6 or any(source == destination for source, destination in expected):
        raise RuntimeError("directed peer matrix must contain exactly six non-self edges")
    complete_edges = {
        (source, destination)
        for source in names
        for destination in names
        if source != destination
    }
    if set(expected) != complete_edges:
        raise RuntimeError("directed peer matrix must cover each non-self member pair exactly once")
    forwarded = {edge for edge, allowed in expected.items() if allowed}
    if len(forwarded) not in (2, 6):
        raise RuntimeError("matrix must be a two-edge majority partition or a fully healed mesh")
    if len(forwarded) == 2:
        (source_a, destination_a), (source_b, destination_b) = forwarded
        if source_a != destination_b or destination_a != source_b:
            raise RuntimeError("majority partition must forward only one reciprocal pair")
    by_destination = {proxy.identity_member: proxy for proxy in proxies}
    if set(by_destination) != set(names):
        raise RuntimeError("proxy destinations do not match the three cluster members")
    results = []
    for source in names:
        for destination in names:
            if source == destination:
                continue
            if (source, destination) not in expected:
                raise RuntimeError(f"directed edge {source}->{destination} is absent from the expected matrix")
            results.append(
                await probe_directed_peer_edge(
                    by_destination[destination],
                    source,
                    destination,
                    cert_dir,
                    fingerprints[source],
                    expected[(source, destination)],
                    timeout,
                )
            )
    observed_edges = {(row["source"], row["destination"]) for row in results}
    if observed_edges != set(expected) or any(row["status"] != "passed" for row in results):
        raise AssertionError(f"observed directed peer matrix differs from expectation: {results}")
    return {
        "status": "passed",
        "self_links": "invalid_excluded",
        "valid_directed_edge_count": 6,
        "observed_directed_edge_count": len(observed_edges),
        "expected_forwarded_edges": [
            {"source": source, "destination": destination}
            for (source, destination), allowed in expected.items()
            if allowed
        ],
        "expected_blocked_edges": [
            {"source": source, "destination": destination}
            for (source, destination), allowed in expected.items()
            if not allowed
        ],
        "edges": results,
    }


def quorum_write_probe(
    etcdctl: str,
    endpoint: str,
    key: str,
    timeout_seconds: float,
    endpoint_status_observed: bool = False,
) -> dict:
    """A unique write proves that an isolated member cannot commit without quorum."""
    try:
        result = subprocess.run(
            [
                etcdctl,
                f"--endpoints={endpoint}",
                f"--command-timeout={timeout_seconds}s",
                "put",
                key,
                "partition-probe",
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 3,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode(errors="replace")
            if isinstance(exc.stdout, bytes)
            else exc.stdout
        )
        stderr = (
            exc.stderr.decode(errors="replace")
            if isinstance(exc.stderr, bytes)
            else exc.stderr
        )
        return {
            "status": "timed_out",
            "failure_class": (
                "no_quorum_or_leader_timeout"
                if endpoint_status_observed
                else "unverified_command_timeout"
            ),
            "stdout": stdout,
            "stderr": stderr,
        }
    combined = (result.stdout + "\n" + result.stderr).lower()
    quorum_markers = (
        "context deadline exceeded",
        "etcdserver: request timed out",
        "etcdserver: no leader",
        "etcdserver: leader changed",
        "etcdserver: unhealthy cluster",
        "etcdserver: not capable",
    )
    failure_class = (
        "committed"
        if result.returncode == 0
        else (
            "no_quorum_or_leader"
            if any(marker in combined for marker in quorum_markers)
            else "unrelated_command_failure"
        )
    )
    return {
        "status": "committed" if result.returncode == 0 else "failed",
        "failure_class": failure_class,
        "returncode": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


async def main(args: argparse.Namespace) -> int:
    from tools.provision_etcd import ROOT as REPO_ROOT, provision_etcd

    backend = getattr(args, "backend", "wsl")
    if backend not in ("wsl", "windows"):
        raise ValueError(f"unsupported etcd peer-partition backend: {backend}")
    etcdctl = os.environ.get(
        "ETCDCTL", str(REPO_ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe")
    )
    etcd_binary = ETCD_WINDOWS_BINARY if backend == "windows" else ROOT / ".tools/etcd-linux/etcd"
    go_binary = GO_WINDOWS_BINARY
    if backend == "windows":
        if not IS_WINDOWS:
            raise RuntimeError("--backend windows is only supported on Windows")
        toolchain = preflight_windows_toolchain(
            args.base_port, etcd_binary=etcd_binary, etcdctl=Path(etcdctl), go_binary=go_binary
        )
        sdk_names = args.sdks or ["dotnet", "cpp", "python", "typescript", "go", "rust", "java"]
        # The existing C++ launcher falls back to a WSL-built worker when no
        # Windows executable exists. Refuse that implicit cross-platform path.
        cpp_worker = next((path for path in WINDOWS_CPP_WORKERS if path.is_file()), None)
        sdk_blockers = []
        cpp_launcher = getattr(args, "cpp_launcher", "windows")
        docker_cpp_worker = Path(
            getattr(args, "cpp_worker_path", None)
            or os.environ.get("DILOCK_CPP_DOCKER_WORKER_PATH", "")
            or ROOT / ".tools/dilock-cpp-etcd-docker-build/dilock_cpp_worker"
        ).resolve()
        docker_cpp_toolchain = None
        if "cpp" in sdk_names:
            if cpp_launcher == "docker":
                try:
                    docker_cpp_toolchain = preflight_docker_cpp_worker(docker_cpp_worker)
                except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
                    sdk_blockers.append(f"Docker C++ etcd worker preflight failed: {exc}")
            elif cpp_worker is None:
                sdk_blockers.append(
                    "Windows-native C++ etcd worker missing; expected one of: "
                    + ", ".join(str(path) for path in WINDOWS_CPP_WORKERS)
                )
        toolchain["sdk_blockers"] = sdk_blockers
        if cpp_launcher == "docker":
            toolchain["cpp_docker_worker"] = docker_cpp_toolchain or {"status": "blocked"}
        if getattr(args, "preflight_only", False):
            toolchain["status"] = "blocked" if sdk_blockers else "passed"
            toolchain["sdk_workers"] = "preflight_only_not_started"
            print(json.dumps(toolchain, indent=2))
            return int(bool(sdk_blockers))
        if sdk_blockers:
            raise RuntimeError("; ".join(sdk_blockers))
    else:
        toolchain = None
        if getattr(args, "preflight_only", False):
            raise ValueError("--preflight-only is supported only with --backend windows")
    required_binaries = [Path(etcdctl), etcd_binary]
    if not args.proxy_binary:
        required_binaries.append(go_binary)
    for required in required_binaries:
        if not required.is_file():
            raise FileNotFoundError(
                f"required local test binary is missing: {required}"
            )

    sdk_names = args.sdks or [
        "dotnet",
        "cpp",
        "python",
        "typescript",
        "go",
        "rust",
        "java",
    ]
    run_dir = (
        ROOT / "validation/implementation" / ("etcd-peer-partition-" + uuid.uuid4().hex)
    )
    run_dir.mkdir(parents=True)
    proxy_binary = (
        Path(args.proxy_binary).resolve()
        if args.proxy_binary
        else run_dir
        / (
            "peerpartitionproxy-windows-amd64.exe"
            if backend == "windows"
            else "peerpartitionproxy-linux-amd64"
        )
    )
    ports = [args.base_port, args.base_port + 10, args.base_port + 20]
    names = ["m6-p1", "m6-p2", "m6-p3"]
    namespace = "m6-etcd-peer-partition-" + uuid.uuid4().hex[:18]
    epoch = "m6-etcd-peer-partition-local-1"
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    workers: dict[str, Worker] = {}
    logs = []
    proxies: list[WslPeerPartitionProxy] = []
    members: list[PeerPartitionMember] = []
    cells: list[dict] = []
    initial_status: list[dict] = []
    recovered_status: list[dict] = []
    partition_states: list[dict] = []
    rejected_states: list[dict] = []
    partition_control_events: list[dict] = []
    proxy_results: list[dict | None] = []
    quorum_probe = None
    majority_stabilization: dict | None = None
    error = None
    cleanup_error = None
    status = "failed"
    phase = "preflight"
    failure_proxy_snapshots: list[dict] = []
    cert_dir: Path | None = None
    certificate_fingerprints: dict[str, str] = {}
    directed_matrix: dict | None = None
    recovered_directed_matrix: dict | None = None
    minority_index = 2
    probe_key = f"/dilock/conformance/{namespace}/quorum-probe"
    try:
        if args.proxy_binary:
            if backend != "windows":
                raise ValueError("--proxy-binary currently supports only --backend windows")
            if not proxy_binary.is_file():
                raise FileNotFoundError(f"prebuilt peer proxy does not exist: {proxy_binary}")
        else:
            build_env = os.environ.copy()
            target_os = "windows" if backend == "windows" else "linux"
            build_env.update(
                {"GOOS": target_os, "GOARCH": "amd64", "CGO_ENABLED": "0", "GO111MODULE": "off"}
            )
            build = await asyncio.to_thread(
                subprocess.run,
                [str(go_binary), "build", "-o", str(proxy_binary), "./conformance/peerpartitionproxy"],
                cwd=ROOT,
                env=build_env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if build.returncode != 0:
                raise RuntimeError(
                    f"could not build the local {target_os} peer proxy: {build.stderr.strip()}"
                )

        if backend == "windows":
            # Repeat the port check immediately before owning listeners; preflight-only
            # is separate and never builds a proxy or creates run artifacts.
            preflight_windows_ports(args.base_port)
        else:
            # Validate fixed listener ports inside the same WSL distro that will own them.
            check = await asyncio.create_subprocess_exec(
                "wsl.exe", "-d", args.wsl_distribution, "--", "bash", "-lc", "ss -H -ltn",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await check.communicate()
            if check.returncode != 0:
                detail = stderr.decode(errors="replace")
                if "E_ACCESSDENIED" in detail or "Access is denied" in detail:
                    raise PermissionError(f"WSL distribution access denied: {detail.strip()}")
                raise RuntimeError(f"could not inspect WSL listeners: {detail}")
            listeners = stdout.decode(errors="replace").splitlines()
            occupied = []
            for port in [*ports, *(port + 1 for port in ports)]:
                for line in listeners:
                    fields = line.split()
                    local_address = fields[3] if len(fields) >= 4 else ""
                    if local_address.rsplit(":", 1)[-1] == str(port):
                        occupied.append(port)
                        break
            if occupied:
                raise RuntimeError(
                    f"refusing to start cluster; WSL ports already listen: {occupied}"
                )

        if args.partition_shape == "majority-minority":
            phase = "generate_ephemeral_peer_certificates"
            cert_dir = ROOT / ".tools" / ("etcd-peer-identities-" + uuid.uuid4().hex)
            generate_command = (
                [str(proxy_binary)]
                if backend == "windows"
                else ["wsl.exe", "-d", args.wsl_distribution, "--", WslPeerPartitionProxy._wsl_path(proxy_binary)]
            )
            cert_args = [
                "--generate-identity-certificates",
                "--identity-cert-dir", str(cert_dir) if backend == "windows" else WslPeerPartitionProxy._wsl_path(cert_dir),
                "--identity-peer-ids", ",".join(names),
            ]
            if backend == "windows":
                generated = await asyncio.to_thread(
                    subprocess.run,
                    [*generate_command, *cert_args],
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                generate_returncode = generated.returncode
                cert_stderr = generated.stderr.encode("utf-8", errors="replace")
            else:
                generate = await asyncio.create_subprocess_exec(
                    *generate_command,
                    *cert_args,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _cert_stdout, cert_stderr = await generate.communicate()
                generate_returncode = generate.returncode
            if generate_returncode != 0:
                detail = cert_stderr.decode(errors="replace").strip()
                if "E_ACCESSDENIED" in detail or "Access is denied" in detail:
                    raise PermissionError(f"WSL certificate generation access denied: {detail}")
                raise RuntimeError(f"ephemeral peer certificate generation failed: {detail}")
            fingerprint_path = cert_dir / "fingerprints.json"
            if not fingerprint_path.is_file():
                raise RuntimeError("certificate generator omitted its fingerprint manifest")
            fingerprint_manifest = json.loads(fingerprint_path.read_text(encoding="utf-8"))
            certificate_fingerprints = fingerprint_manifest.get("peer_cert_sha256", {})
            if set(certificate_fingerprints) != set(names) or any(
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in certificate_fingerprints.values()
            ):
                raise RuntimeError(
                    f"invalid ephemeral certificate fingerprint manifest: {fingerprint_manifest}"
                )

        phase = "start_peer_proxies"
        for name, client_port, peer_port in zip(
            names, ports, (port + 1 for port in ports)
        ):
            source_peers = [peer for peer in names if peer != name]
            proxy_type = WindowsPeerPartitionProxy if backend == "windows" else WslPeerPartitionProxy
            proxy = proxy_type(
                run_dir / (name + "-peer-proxy"),
                args.wsl_distribution,
                f"127.0.0.1:{peer_port}",
                binary_path=proxy_binary,
                identity_member=(
                    name if args.partition_shape == "majority-minority" else None
                ),
                identity_peer_ids=(
                    source_peers if args.partition_shape == "majority-minority" else None
                ),
                identity_cert_dir=cert_dir,
            )
            proxy.run_dir.mkdir(parents=True, exist_ok=True)
            await proxy.start()
            proxies.append(proxy)
        advertised_urls = [
            proxy.endpoint.replace("http://", "http://", 1) for proxy in proxies
        ]
        cluster = ",".join(
            f"{name}={peer_url}" for name, peer_url in zip(names, advertised_urls)
        )
        members = [
            PeerPartitionMember(
                name,
                client_port,
                peer_port,
                run_dir,
                args.wsl_distribution,
                cluster,
                peer_url,
                peer_tls=args.partition_shape == "majority-minority",
                peer_cert_dir=cert_dir,
                backend=backend,
                etcd_binary=etcd_binary,
            )
            for name, client_port, peer_port, peer_url in zip(
                names, ports, (port + 1 for port in ports), advertised_urls
            )
        ]
        for member in members:
            await member.start()
        for member in members:
            await member.wait_listening(etcdctl)
        phase = "verify_initial_cluster"
        initial_status = await wait_consistent_leader(
            etcdctl, members, args.cluster_timeout_seconds
        )
        initial_cluster_ids = {row["cluster_id"] for row in initial_status}
        initial_member_ids = {row["member_id"] for row in initial_status}
        if len(initial_member_ids) != 3 or len(initial_cluster_ids) != 1:
            raise AssertionError(
                f"initial cluster identity was not a three-member cluster: {initial_status}"
        )
        provision_etcd(members[0].endpoint, namespace, epoch)

        # Establish each SDK's backend connection while the initial three-member
        # cluster has quorum. In the asymmetric fault, a provider connected only
        # after isolating the minority cannot perform its required linearizable
        # namespace-metadata read. Workers remain idle until the fault assertions.
        phase = "start_sdk_workers_on_healthy_cluster"
        env = os.environ.copy()
        env["NO_PROXY"] = "localhost,127.0.0.1,::1"
        env["no_proxy"] = env["NO_PROXY"]
        # The default C++ launcher target is Redis-only; the peer-partition
        # matrix must select the staged etcd-enabled worker and its local libs.
        if backend == "windows":
            # Require the native worker found above; never fall back into WSL.
            env.pop("DILOCK_CPP_WSL_WORKER", None)
            env.pop("DILOCK_CPP_WSL_LD_LIBRARY_PATH", None)
            if getattr(args, "cpp_launcher", "windows") == "docker" and "cpp" in sdk_names:
                env["DILOCK_CPP_DOCKER_WORKER"] = "1"
                env["DILOCK_CPP_DOCKER_WORKER_PATH"] = str(docker_cpp_worker)
            else:
                env.pop("DILOCK_CPP_DOCKER_WORKER", None)
                env.pop("DILOCK_CPP_DOCKER_WORKER_PATH", None)
        else:
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
        endpoints = {
            sdk: members[index % len(members)] for index, sdk in enumerate(sdk_names)
        }
        for sdk in sdk_names:
            member = endpoints[sdk]
            config_path = run_dir / f"{sdk}.json"
            config_path.write_text(
                json.dumps(etcd_sdk_config(member.endpoint, namespace, epoch)) + "\n",
                encoding="utf-8",
            )
            worker, log = await _launch_worker(
                sdk, config_path, manifest, run_dir, env,
                popen_on_windows=backend == "windows",
            )
            workers[sdk] = worker
            logs.append(log)
            hello = await worker.request("hello", timeout=45)
            if hello.get("status") != "ok":
                raise RuntimeError(f"{sdk} worker did not start: {hello}")

        resources = {
            sdk: f"m6/etcd-peer-partition/{sdk}/{uuid.uuid4().hex}" for sdk in sdk_names
        }
        if args.partition_shape == "total":
            phase = "baseline_sdk_acquire_release"
            for sdk in sdk_names:
                acquired = await workers[sdk].request(
                    "acquire",
                    resource=resources[sdk],
                    wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms,
                    rpc_ms=args.rpc_ms,
                    auto_renew=False,
                )
                if acquired.get("status") != "acquired":
                    raise AssertionError(f"{sdk} baseline acquire failed: {acquired}")
                released = await workers[sdk].request(
                    "release", lease_id=acquired["lease_id"]
                )
                if released.get("status") != "released":
                    raise AssertionError(f"{sdk} baseline release failed: {released}")

        partition_expected_edges: dict[tuple[str, str], bool] = {}
        if args.partition_shape == "majority-minority":
            majority = {names[index] for index in range(len(names)) if index != minority_index}
            partition_expected_edges = {
                (source, destination): source in majority and destination in majority
                for source in names
                for destination in names
                if source != destination
            }
            phase = "apply_directed_peer_acl"
            proxies_by_member = {proxy.identity_member: proxy for proxy in proxies}
            if set(proxies_by_member) != set(names):
                raise AssertionError("identity-aware proxy destinations do not match etcd members")
            for destination in names:
                proxy = proxies_by_member[destination]
                for source in names:
                    if source == destination:
                        continue
                    should_block = not partition_expected_edges[(source, destination)]
                    partition_control_events.append(
                        await set_peer_acl(proxy, source, should_block)
                    )
            expected_allowed_by_destination = {
                destination: sorted(
                    source
                    for source in names
                    if source != destination
                    and partition_expected_edges[(source, destination)]
                )
                for destination in names
            }
            for proxy in proxies:
                snapshot = await proxy.wait_state(
                    lambda state, dest=proxy.identity_member: state.get("allowed_peers")
                    == expected_allowed_by_destination[dest],
                    timeout=args.partition_barrier_timeout_seconds,
                )
                partition_states.append(snapshot)
            phase = "verify_directed_peer_matrix_before_sdk_fault_operations"
            assert cert_dir is not None
            directed_matrix = await verify_directed_peer_matrix(
                proxies,
                names,
                cert_dir,
                certificate_fingerprints,
                partition_expected_edges,
                args.partition_barrier_timeout_seconds,
            )
            if directed_matrix.get("status") != "passed":
                raise AssertionError(
                    f"directed connectivity matrix did not pass before SDK fault operations: {directed_matrix}"
                )

            # Applying the ACL can force a Raft election when the former leader
            # is isolated in the minority. Do not race SDK startup against that
            # election: require the two surviving members to agree on one leader
            # and commit a small write before SDK fault operations begin.
            phase = "verify_majority_quorum_before_sdk_fault_operations"
            majority_members = [
                member for index, member in enumerate(members) if index != minority_index
            ]
            majority_status = await wait_consistent_leader(
                etcdctl, majority_members, args.cluster_timeout_seconds
            )
            majority_probe = await asyncio.to_thread(
                quorum_write_probe,
                etcdctl,
                majority_members[0].endpoint,
                probe_key + "/pre-worker-majority",
                args.probe_timeout_seconds,
                True,
            )
            majority_stabilization = {
                "status": "passed" if majority_probe.get("failure_class") == "committed" else "failed",
                "majority_members": [member.name for member in majority_members],
                "cluster_status": majority_status,
                "write_probe": majority_probe,
            }
            if majority_stabilization["status"] != "passed":
                raise AssertionError(
                    f"majority quorum did not commit before SDK fault operations: {majority_probe}"
                )

        # Each proxy controls one member's advertised peer ingress URL. Total mode
        # cuts all links; majority-minority mode cuts the selected minority ingress
        # and requires independent writes to prove which side retained quorum.
        phase = "partition_control_acknowledgements"
        active_partition_indices = list(range(len(proxies)))
        partitioned_proxies = list(proxies)
        if args.partition_shape == "total":
            for index in active_partition_indices:
                partition_control_events.append(
                    await set_peer_partition(proxies[index], True)
                )
            phase = "wait_for_partition_quiescence"
            partition_states = await wait_partitioned(
                partitioned_proxies, args.partition_barrier_timeout_seconds
            )
        phase = "observe_partitioned_members"
        await asyncio.sleep(args.partition_seconds)
        partition_member_statuses = await asyncio.gather(
            *(
                asyncio.to_thread(cluster_status, etcdctl, [member])
                for member in members
            ),
            return_exceptions=True,
        )
        local_member_views = []
        for member, observed in zip(members, partition_member_statuses):
            if isinstance(observed, Exception):
                local_member_views.append(
                    {
                        "member": member.name,
                        "status": "unavailable",
                        "error": f"{type(observed).__name__}: {observed}",
                    }
                )
                continue
            row = observed[0] if observed else {}
            local_member_views.append(
                {"member": member.name, "status": "reachable", **row}
            )
        if any(row["status"] != "reachable" for row in local_member_views):
            raise AssertionError(
                f"could not directly observe all isolated member status RPCs: {local_member_views}"
            )
        if {row["member_id"] for row in local_member_views} != initial_member_ids:
            raise AssertionError(
                f"peer partition unexpectedly stopped a member: {local_member_views}"
            )
        phase = "direct_quorum_write_probes"
        probe_results = await asyncio.gather(
            *(
                asyncio.to_thread(
                    quorum_write_probe,
                    etcdctl,
                    member.endpoint,
                    probe_key,
                    args.probe_timeout_seconds,
                    True,
                )
                for member in members
            )
        )
        expected_probe_classes = [
            ("no_quorum_or_leader", "no_quorum_or_leader_timeout")
            if args.partition_shape == "total" or index == minority_index
            else ("committed",)
            for index in range(len(members))
        ]
        quorum_probe = {
            "attempted_on_each_member": True,
            "expected_by_member": {
                member.name: list(expected_probe_classes[index])
                for index, member in enumerate(members)
            },
            "partition_member_status": local_member_views,
            "results": probe_results,
            "validated_topology": all(
                row.get("failure_class") in expected_probe_classes[index]
                for index, row in enumerate(probe_results)
            ),
        }
        if not quorum_probe["validated_topology"]:
            raise AssertionError(
                f"per-member quorum probes did not match {args.partition_shape}: {probe_results}"
            )

        async def partition_acquire(sdk: str) -> tuple[str, dict]:
            try:
                response = await workers[sdk].request(
                    # etcd's native Lock API is blocking and does not provide a
                    # safe nonblocking try-acquire operation. Use the bounded
                    # acquire path in both shapes: majority members can acquire,
                    # while a minority-side wait must end as unavailable,
                    # ambiguous, or timeout (never ordinary contention).
                    "acquire",
                    resource=resources[sdk],
                    wait_ms=args.wait_ms,
                    lease_ms=args.lease_ms,
                    rpc_ms=args.rpc_ms,
                    auto_renew=False,
                    timeout=args.wait_ms / 1000 + args.worker_timeout_slack_seconds,
                )
            except Exception as exc:
                response = {
                    "status": "worker_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            return sdk, response

        phase = "sdk_partition_acquires"
        responses = await asyncio.gather(*(partition_acquire(sdk) for sdk in sdk_names))
        for sdk, response in responses:
            result = response.get("status")
            member_index = members.index(endpoints[sdk])
            expects_minority_failure = (
                args.partition_shape == "total" or member_index == minority_index
            )
            expected_results = (
                ("backend_unavailable", "acquisition_unknown", "timeout")
                if expects_minority_failure
                else ("acquired",)
            )
            cell = {
                "sdk": sdk,
                "status": "passed",
                "client_member": endpoints[sdk].name,
                "expected_side": (
                    "isolated"
                    if args.partition_shape == "total"
                    else ("minority" if expects_minority_failure else "majority")
                ),
                "partition_result": result,
                "remote_outcome_unknown": response.get("remote_outcome_unknown"),
            }
            if result not in expected_results:
                cell.update(
                    status="failed",
                    error=f"partition result did not match its quorum side: {response}",
                )
            if result == "acquired":
                cell["partition_lease_id"] = response.get("lease_id")
            cells.append(cell)
        phase = "verify_rejected_peer_reconnects"
        if args.partition_shape == "total":
            rejected_states = await wait_reconnects_rejected(
                partitioned_proxies, args.partition_barrier_timeout_seconds
            )
        else:
            assert cert_dir is not None
            rejected_matrix = await verify_directed_peer_matrix(
                proxies,
                names,
                cert_dir,
                certificate_fingerprints,
                partition_expected_edges,
                args.partition_barrier_timeout_seconds,
            )
            rejected_states = rejected_matrix["edges"]

        phase = "heal_peer_links"
        if args.partition_shape == "total":
            for proxy in proxies:
                partition_control_events.append(await set_peer_partition(proxy, False))
        else:
            all_allowed_edges = {
                (source, destination): True
                for source in names
                for destination in names
                if source != destination
            }
            for proxy in proxies:
                for source in proxy.identity_peer_ids:
                    partition_control_events.append(
                        await set_peer_acl(proxy, source, blocked=False)
                    )
                expected_sources = sorted(proxy.identity_peer_ids)
                await proxy.wait_state(
                    lambda state, expected=expected_sources: state.get("allowed_peers")
                    == expected,
                    timeout=args.partition_barrier_timeout_seconds,
                )
            assert cert_dir is not None
            recovered_directed_matrix = await verify_directed_peer_matrix(
                proxies,
                names,
                cert_dir,
                certificate_fingerprints,
                all_allowed_edges,
                args.partition_barrier_timeout_seconds,
            )
        phase = "verify_cluster_recovery"
        recovered_status = await wait_consistent_leader(
            etcdctl, members, args.recovery_timeout_seconds
        )
        recovered_cluster_ids = {row["cluster_id"] for row in recovered_status}
        recovered_member_ids = {row["member_id"] for row in recovered_status}
        if (
            recovered_cluster_ids != initial_cluster_ids
            or recovered_member_ids != initial_member_ids
        ):
            raise AssertionError(
                "cluster/member identity changed during peer partition recovery"
            )
        old_terms = {row["raft_term"] for row in initial_status}
        new_terms = {row["raft_term"] for row in recovered_status}
        old_leaders = {row["leader_id"] for row in initial_status}
        new_leaders = {row["leader_id"] for row in recovered_status}
        if max(new_terms) < max(old_terms):
            raise AssertionError(
                f"Raft term regressed after peer partition: {old_terms} -> {new_terms}"
            )

        # Inspect every possibly ambiguous attempt only after quorum is restored.
        # Never issue a successor request while its resource still has a lock key.
        phase = "verify_successor_and_cleanup"
        for cell in cells:
            sdk = cell["sdk"]
            if cell.get("partition_lease_id"):
                released = await workers[sdk].request(
                    "release", lease_id=cell.pop("partition_lease_id")
                )
                cell["partition_release_after_recovery"] = released.get("status")
                if released.get("status") != "released":
                    cell.update(
                        status="failed",
                        error=f"majority-acquired lease did not release after recovery: {released}",
                    )
                    continue
            count = await asyncio.to_thread(
                lock_key_count,
                endpoints[sdk].endpoint,
                lock_prefix(namespace, resources[sdk]),
            )
            cell["lock_keys_immediately_after_recovery"] = count
            if count:
                await asyncio.sleep(args.lease_ms / 1000 + 1)
                count = await asyncio.to_thread(
                    lock_key_count,
                    endpoints[sdk].endpoint,
                    lock_prefix(namespace, resources[sdk]),
                )
                cell["lock_keys_after_attempt_ttl"] = count
            if count != 0:
                cell.update(
                    status="failed",
                    error="partition attempt left a lock after its lease deadline",
                )
                continue
            response = await workers[sdk].request(
                "acquire",
                resource=resources[sdk],
                wait_ms=args.wait_ms,
                lease_ms=args.lease_ms,
                rpc_ms=args.rpc_ms,
                auto_renew=False,
                timeout=args.wait_ms / 1000 + args.worker_timeout_slack_seconds,
            )
            cell["successor_result_after_recovery"] = response.get("status")
            if response.get("status") != "acquired":
                cell.update(
                    status="failed",
                    error=f"successor after recovered quorum failed: {response}",
                )
                continue
            released = await workers[sdk].request(
                "release", lease_id=response["lease_id"]
            )
            cell["successor_release"] = released.get("status")
            if released.get("status") != "released":
                cell.update(
                    status="failed", error=f"successor release failed: {released}"
                )
                continue
            final_count = await asyncio.to_thread(
                lock_key_count,
                endpoints[sdk].endpoint,
                lock_prefix(namespace, resources[sdk]),
            )
            cell["lock_keys_after_release"] = final_count
            if final_count != 0:
                cell.update(
                    status="failed", error="successor lock key remained after release"
                )

        status = (
            "passed" if all(cell["status"] == "passed" for cell in cells) else "failed"
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        failure_proxy_snapshots = []
        for proxy in proxies:
            observed: dict | None = None
            try:
                response = await asyncio.wait_for(proxy.control("status"), timeout=1)
                observed = response.get("state")
            except Exception:
                observed = proxy.last_snapshot
            failure_proxy_snapshots.append(
                {
                    "run_directory": str(proxy.run_dir),
                    "state_file": str(proxy.state_path),
                    "snapshot": observed,
                }
            )
        if isinstance(exc, (FileNotFoundError, PermissionError)):
            status = "blocked"
    finally:
        # Heal the peer mesh before graceful service shutdown even if an assertion fails.
        for proxy in proxies:
            try:
                if args.partition_shape == "majority-minority":
                    for source in proxy.identity_peer_ids:
                        partition_control_events.append(
                            await set_peer_acl(proxy, source, blocked=False)
                        )
                partition_control_events.append(await set_peer_partition(proxy, False))
            except OSError as exc:
                cleanup_error = f"could not heal {proxy.run_dir.name}: {exc}"
                status = "failed"
            except Exception as exc:
                cleanup_error = f"could not heal {proxy.run_dir.name}: {exc}"
                status = "failed"
        await asyncio.gather(
            *(worker.close() for worker in workers.values()), return_exceptions=True
        )
        for proxy in proxies:
            try:
                result = await proxy.close()
                proxy_results.append(result)
            except Exception as exc:
                cleanup_error = f"proxy {type(exc).__name__}: {exc}"
                status = "failed"
        for member in reversed(members):
            try:
                await member.stop()
            except Exception as exc:
                cleanup_error = f"{member.name} {type(exc).__name__}: {exc}"
                status = "failed"
        certificate_cleanup = "not_created"
        if cert_dir is not None and cert_dir.exists():
            certificate_cleanup = "passed"
            generated_names = ["ca.crt", "fingerprints.json"]
            for member_name in names:
                generated_names.extend([f"{member_name}.crt", f"{member_name}.key"])
            for filename in generated_names:
                generated_file = cert_dir / filename
                try:
                    generated_file.unlink(missing_ok=True)
                except OSError as exc:
                    certificate_cleanup = f"failed: {type(exc).__name__}: {exc}"
                    cleanup_error = cleanup_error or f"could not remove ephemeral key material: {exc}"
                    status = "failed"
            try:
                cert_dir.rmdir()
            except OSError as exc:
                certificate_cleanup = f"failed: {type(exc).__name__}: {exc}"
                cleanup_error = cleanup_error or f"ephemeral certificate directory was not empty: {exc}"
                status = "failed"
        for log in logs:
            log.close()
        report = {
            "id": (
                "M6-ETCD-THREE-MEMBER-PEER-PARTITION-QUORUM-RECOVERY"
                if args.partition_shape == "total"
                else "M6-ETCD-TWO-OF-THREE-MAJORITY-MINORITY-PEER-PARTITION"
            ),
            "date": date.today().isoformat(),
            "status": status,
            "harness_exit_code": int(status != "passed"),
            "scenario": (
                (
                    "Three disposable etcd members use ephemeral mTLS peer identities and identity-pinned destination proxies. Native SDK workers validate namespace metadata and connect while the healthy three-member cluster has quorum, then remain idle while the harness applies ACLs to six non-self directed links. The harness requires an observed certificate-fingerprint-authenticated matrix with two majority links forwarded and four minority-crossing links blocked before SDK fault operations, exercises the partition behavior, then heals all six links and observes recovery connectivity."
                    if directed_matrix is not None
                    else "Three disposable etcd members use ephemeral peer mTLS identities and an identity-aware directional proxy. The harness failed closed before SDK fault operations because it could not observe the required exact six-edge directed connectivity matrix."
                )
                if args.partition_shape == "majority-minority"
                else (
                "Three disposable etcd members advertise peer URLs through local TCP fault proxies. "
                + "All peer ingress paths are partitioned; "
                + "client URLs remain reachable. Per-member write probes classify quorum, each selected SDK performs a lock attempt on its configured member, busy is rejected as an invalid minority outcome, and the healed cluster must converge before successors are acquired and released."
                )
            ),
            "partition_shape": args.partition_shape,
            "topology_verification": {
                "status": (
                    "passed" if directed_matrix and directed_matrix.get("status") == "passed"
                    else "not_verified"
                ) if args.partition_shape == "majority-minority" else "member-ingress-cut",
                "reason": error if args.partition_shape == "majority-minority" and not directed_matrix else None,
                "connectivity_matrix_observed": bool(directed_matrix and directed_matrix.get("status") == "passed") if args.partition_shape == "majority-minority" else None,
            },
            "certificate_fingerprints": certificate_fingerprints,
            "ephemeral_key_cleanup": certificate_cleanup,
            "directed_connectivity_matrix": directed_matrix,
            "recovered_directed_connectivity_matrix": recovered_directed_matrix,
            "majority_members": [] if args.partition_shape == "total" else [name for index, name in enumerate(names) if index != minority_index],
            "minority_member": None if args.partition_shape == "total" else names[minority_index],
            "backend": (
                "etcd 3.7.2 Windows/amd64 binary; three disposable local members and peer links proxied by directly owned Windows processes."
                if backend == "windows"
                else "etcd 3.7.2 Linux binary under WSL Ubuntu 26.04; three disposable local members; peer links proxied locally."
            ),
            "execution_platform": backend,
            "platform_preflight": toolchain,
            "namespace": namespace,
            "participants": sdk_names,
            "real_processes": bool(workers or any(member.starts for member in members)),
            "real_backend": bool(initial_status),
            "partition_seconds": args.partition_seconds,
            "initial_cluster_status": initial_status,
            "peer_partition_barrier": partition_states,
            "peer_reconnect_rejections": rejected_states,
            "partition_control_events": partition_control_events,
            "quorum_write_probe": quorum_probe,
            "majority_stabilization_before_sdk_fault_operations": majority_stabilization,
            "recovered_cluster_status": recovered_status,
            "leader_ids_before_after": {
                "before": sorted(old_leaders) if "old_leaders" in locals() else [],
                "after": sorted(new_leaders) if "new_leaders" in locals() else [],
            },
            "raft_terms_before_after": {
                "before": sorted(old_terms) if "old_terms" in locals() else [],
                "after": sorted(new_terms) if "new_terms" in locals() else [],
            },
            "peer_proxies": [
                {
                    "member": member.name,
                    "advertised_peer_endpoint": safe_endpoint(proxy.endpoint),
                    "upstream_peer_endpoint": safe_endpoint(member.peer_listen_url),
                    "control_endpoint": (
                        f"127.0.0.1:{proxy.control_port}"
                        if proxy.control_port is not None
                        else None
                    ),
                    "state_file": str(proxy.state_path),
                }
                for member, proxy in zip(members, proxies)
            ],
            "proxy_shutdown_results": proxy_results,
            "members": [
                {
                    "name": member.name,
                    "client_endpoint": safe_endpoint(member.endpoint),
                    "peer_listen_endpoint": safe_endpoint(member.peer_listen_url),
                    "start_count": member.starts,
                    "stop_count": member.stops,
                    "data_directory": str(member.data_dir),
                    "log": str(member.log_path),
                }
                for member in members
            ],
            "cells": cells,
            "error": error,
            "failure_phase": phase if status != "passed" else None,
            "last_proxy_snapshots_on_failure": failure_proxy_snapshots,
            "cleanup_error": cleanup_error,
            "run_directory": str(run_dir),
            "binary_hashes": {
                "etcd_sha256": hashlib.sha256(etcd_binary.read_bytes()).hexdigest(),
                "peer_proxy_sha256": hashlib.sha256(
                    proxy_binary.read_bytes()
                ).hexdigest(),
            },
            "not_run": [
                "kernel-level directional packet filtering and source-address-selected peer paths",
                "snapshot restore and epoch migration",
                "Redis failover",
                "external protected-resource fencing",
                "production network topology and repeated randomized fault schedules",
            ],
            "limitations": [
                "The three peer TCP proxies close existing peer streams and reject new connections; this is a local application-path partition, not kernel packet loss or a physical network fault.",
                "The total-partition mode cuts every advertised peer ingress. It is not arbitrary directional packet loss or a physical network fault.",
                "SDK clients retain one configured member endpoint each; no automatic multi-endpoint discovery or failover claim is made.",
                "Lease expiry, quorum recovery and lock release do not stop stale processes or replace fencing enforced by the protected resource.",
            ],
            "attempt_history": [
                "The first seven-SDK attempt reached a three-member healthy baseline but timed out before probes or SDK calls because the initial barrier incorrectly required rejected reconnects before any operation could trigger them. No partition-phase SDK cell ran; owned services were stopped and their data/logs retained in etcd-peer-partition-0900c3c9f6ad454d8daf01ee15ebbc00. The corrected barrier first verifies every proxy is partitioned with zero active streams, then checks rejected reconnect counters after probes and SDK calls.",
                f"Current invocation: status={status}, phase={phase}, error={error}; all owned-process and SDK-cell counts are recorded above.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-port",
        type=int,
        default=25279,
        help="client ports are base, base+10 and base+20; peer listeners use each client port+1",
    )
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument(
        "--backend",
        choices=("wsl", "windows"),
        default="wsl",
        help="run the disposable etcd members and peer proxies under WSL or as native Windows processes",
    )
    parser.add_argument(
        "--cpp-launcher",
        choices=("windows", "docker"),
        default="windows",
        help="run the C++ worker natively on Windows or in a pinned local Linux Docker image",
    )
    parser.add_argument(
        "--cpp-worker-path",
        help="path to the C++ etcd worker binary (required unless the default local build exists)",
    )
    parser.add_argument(
        "--proxy-binary",
        help="use a prebuilt Windows peer proxy (Windows backend only), for example a Docker cross-build",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="validate native Windows etcd/tool versions, peer flags and fixed ports without creating run files or starting services",
    )
    parser.add_argument("--workers", default=".tools/m6-etcd-outage-workers.json")
    parser.add_argument(
        "--wsl-distribution",
        default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"),
    )
    parser.add_argument("--lease-ms", type=int, default=5000)
    parser.add_argument("--wait-ms", type=int, default=5000)
    parser.add_argument("--rpc-ms", type=int, default=1500)
    parser.add_argument("--partition-seconds", type=float, default=6)
    parser.add_argument(
        "--partition-shape",
        choices=("total", "majority-minority"),
        default="total",
        help="use the total peer cut or the identity-aware majority/minority directed peer matrix",
    )
    parser.add_argument("--partition-barrier-timeout-seconds", type=float, default=15)
    parser.add_argument("--probe-timeout-seconds", type=float, default=3)
    parser.add_argument("--worker-timeout-slack-seconds", type=float, default=12)
    parser.add_argument("--cluster-timeout-seconds", type=float, default=30)
    parser.add_argument("--recovery-timeout-seconds", type=float, default=45)
    parser.add_argument(
        "--output",
        default="validation/implementation/m6-etcd-peer-partition-seven-sdk.json",
    )
    sys.exit(asyncio.run(main(parser.parse_args())))
