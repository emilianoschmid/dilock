from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
import uuid

import pytest

from conformance import etcd_peer_partition as harness
from conformance.run import Worker, spawn_process


@pytest.fixture
def local_test_dir():
    path = harness.ROOT / ".tools" / ("peerpartition-unit-" + uuid.uuid4().hex)
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class FakeProcess:
    def __init__(self) -> None:
        self.returncode = None
        self.pid = 9876
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        self.returncode = 0
        return 0


def test_windows_fixed_port_range_rejects_overflow() -> None:
    assert harness.windows_fixed_ports(25279) == [25279, 25289, 25299, 25280, 25290, 25300]
    with pytest.raises(ValueError, match="TCP range"):
        harness.windows_fixed_ports(65515)


def test_windows_port_preflight_rejects_occupied_port(monkeypatch: pytest.MonkeyPatch) -> None:
    class ProbeSocket:
        next_port = 0

        def __init__(self, *_args) -> None:
            self.port = None

        def setsockopt(self, *_args) -> None:
            pass

        def bind(self, address) -> None:
            self.port = address[1]
            if self.port == 25289:
                raise OSError("already in use")

        def close(self) -> None:
            pass

    monkeypatch.setattr(harness.socket, "socket", ProbeSocket)
    with pytest.raises(OSError, match="25289"):
        harness.preflight_windows_ports(25279)


def test_windows_toolchain_preflight_checks_version_tls_flags_and_ports(
    local_test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    etcd, ctl, go = (local_test_dir / name for name in ("etcd.exe", "etcdctl.exe", "go.exe"))
    for binary in (etcd, ctl, go):
        binary.touch()
    help_output = "\n".join(
        (
            "--listen-peer-urls",
            "--initial-advertise-peer-urls",
            "--initial-cluster-state",
            "--peer-cert-file",
            "--peer-key-file",
            "--peer-trusted-ca-file",
            "--peer-client-cert-auth",
        )
    )

    def fake_run(command, **_kwargs):
        if command[-1] == "--version":
            output = "etcd Version: 3.7.2\nGo OS/Arch: windows/amd64"
        elif command[-1] == "version":
            output = "etcdctl version: 3.7.2"
        elif command[-1] == "--help":
            output = help_output
        else:
            output = "go version go1.27.1 windows/amd64"
        return SimpleNamespace(stdout=output, stderr="")

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    monkeypatch.setattr(harness, "preflight_windows_ports", lambda port: harness.windows_fixed_ports(port))
    result = harness.preflight_windows_toolchain(25279, etcd, ctl, go)
    assert result["status"] == "passed"
    assert result["services_started"] is False
    assert result["fixed_loopback_ports"] == harness.windows_fixed_ports(25279)


def test_windows_toolchain_preflight_fails_closed_without_peer_tls_flag(
    local_test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    etcd, ctl, go = (local_test_dir / name for name in ("etcd.exe", "etcdctl.exe", "go.exe"))
    for binary in (etcd, ctl, go):
        binary.touch()

    def fake_run(command, **_kwargs):
        output = (
            "etcd Version: 3.7.2\nGo OS/Arch: windows/amd64"
            if command[-1] == "--version"
            else "etcdctl version: 3.7.2"
            if command[-1] == "version"
            else "go version go1.27.1 windows/amd64"
            if command[-1] == "go.exe"
            else "--listen-peer-urls"
        )
        return SimpleNamespace(stdout=output, stderr="")

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="peer flags"):
        harness.preflight_windows_toolchain(25279, etcd, ctl, go)


def test_docker_cpp_preflight_requires_worker_and_exact_local_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = []

    def fake_run(command, **kwargs):
        captured.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f\n", stderr="")

    monkeypatch.setattr(harness.subprocess, "run", fake_run)
    result = harness.preflight_docker_cpp_worker(Path(__file__))

    assert result["status"] == "passed"
    assert result["image"] == harness.DOCKER_CPP_WORKER_IMAGE
    assert len(result["runtime_library_directories"]) == 4
    assert captured[0][0] == [
        "docker", "image", "inspect", harness.DOCKER_CPP_WORKER_IMAGE, "--format", "{{.Id}}"
    ]


def test_docker_cpp_preflight_rejects_missing_worker() -> None:
    with pytest.raises(FileNotFoundError, match="worker is missing"):
        harness.preflight_docker_cpp_worker(Path(".tools/missing-dilock-cpp-worker"))


def test_identity_proxy_wait_state_joins_peer_events_from_snapshot(
    local_test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        proxy = harness.WslPeerPartitionProxy(
            local_test_dir / "proxy", "", "127.0.0.1:44103",
            identity_member="m6-p2", identity_peer_ids=["m6-p1", "m6-p3"],
        )
        proxy.run_dir.mkdir()
        event = {
            "sequence": 4,
            "destination": "m6-p2",
            "identity": "m6-p1",
            "certificate_sha256": "a" * 64,
            "identity_verified": True,
            "decision": "forwarded",
        }
        proxy.state_path.write_text(json.dumps({"peer_events": [event]}), encoding="utf-8")

        async def bounded_control(_command: str, _peer_id: str | None = None) -> dict:
            return {
                "request_id": "test",
                "status": "ok",
                "state": {
                    "identity_member": "m6-p2",
                    "allowed_peers": ["m6-p1"],
                    "allowed_by_peer": {"m6-p1": 2},
                },
            }

        monkeypatch.setattr(proxy, "control", bounded_control)
        state = await proxy.wait_state(
            lambda value: value.get("peer_events") == [event], timeout=0.1
        )
        assert state["allowed_peers"] == ["m6-p1"]
        assert state["peer_events"] == [event]

    asyncio.run(scenario())


def test_windows_proxy_starts_direct_binary_and_uses_native_paths(
    local_test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        monkeypatch.setattr(harness, "IS_WINDOWS", True)
        binary = local_test_dir / "peer-proxy.exe"
        binary.touch()
        cert_dir = local_test_dir / "certs"
        cert_dir.mkdir()
        process = FakeProcess()
        captured = []

        async def fake_create(*command, **_kwargs):
            captured.extend(command)
            flags = dict(zip(command[1::2], command[2::2]))
            Path(flags["--ready-file"]).write_text(json.dumps({"port": 44101, "control_port": 44102}))
            Path(flags["--state-file"]).write_text(json.dumps({"partitioned": False}))
            return process

        async def fake_spawn(*command, **kwargs):
            assert kwargs.pop("popen_on_windows") is True
            return await fake_create(*command, **kwargs)

        monkeypatch.setattr(harness, "spawn_process", fake_spawn)
        proxy = harness.WindowsPeerPartitionProxy(
            local_test_dir / "proxy", "", "127.0.0.1:44103", binary, "m6-p1", ["m6-p2", "m6-p3"], cert_dir
        )
        proxy.run_dir.mkdir()
        await proxy.start()
        assert captured[0] == str(binary)
        assert "wsl.exe" not in captured
        assert proxy.endpoint == "https://127.0.0.1:44101"
        assert str(cert_dir / "ca.crt") in captured
        result = await proxy.close()
        assert result is None
        assert process.killed is False

    asyncio.run(scenario())


def test_windows_member_owns_and_terminates_only_direct_child(
    local_test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        monkeypatch.setattr(harness, "IS_WINDOWS", True)
        etcd = local_test_dir / "etcd.exe"
        etcd.touch()
        cert_dir = local_test_dir / "certs"
        cert_dir.mkdir()
        process = FakeProcess()
        captured = []

        async def fake_create(*command, **_kwargs):
            captured.extend(command)
            return process

        async def fake_spawn(*command, **kwargs):
            assert kwargs.pop("popen_on_windows") is True
            return await fake_create(*command, **kwargs)

        monkeypatch.setattr(harness, "spawn_process", fake_spawn)
        member = harness.PeerPartitionMember(
            "m6-p1", 25279, 25280, local_test_dir, "", "m6-p1=https://127.0.0.1:44101",
            "https://127.0.0.1:44101", peer_tls=True, peer_cert_dir=cert_dir,
            backend="windows", etcd_binary=etcd,
        )
        await member.start()
        assert captured[0] == str(etcd)
        assert "wsl.exe" not in captured
        assert "--peer-client-cert-auth=true" in captured
        assert member.pid == process.pid
        await member.stop()
        assert process.terminated is True
        assert process.killed is False
        assert member.stops == 1
        assert member.log_handle is None

    asyncio.run(scenario())


@pytest.mark.skipif(os.name != "nt", reason="covers the native Windows Popen adapter")
def test_windows_popen_adapter_supports_worker_pipes_and_owned_termination() -> None:
    async def scenario() -> None:
        worker_code = (
            "import json,sys; "
            "[(lambda r: (print(json.dumps({'id':r['id'],'status':'ok'}),flush=True), "
            "sys.exit(0) if r['op']=='shutdown' else None))(json.loads(line)) "
            "for line in sys.stdin]"
        )
        process = await spawn_process(
            sys.executable, "-c", worker_code,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            popen_on_windows=True,
        )
        worker = Worker(process)
        assert await worker.request("hello", timeout=5) == {"id": "1", "status": "ok"}
        await worker.close()
        assert process.returncode == 0

        sleeper = await spawn_process(
            sys.executable, "-c", "import time; time.sleep(30)",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            popen_on_windows=True,
        )
        sleeper.terminate()
        await asyncio.wait_for(sleeper.wait(), 5)
        assert sleeper.returncode is not None

    asyncio.run(scenario())
