from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


LAUNCHER_PATH = Path(__file__).resolve().parents[1] / "sdk/cpp/worker_launcher.py"
SPEC = importlib.util.spec_from_file_location("dilock_cpp_worker_launcher", LAUNCHER_PATH)
assert SPEC is not None and SPEC.loader is not None
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


def test_docker_config_preserves_contract_and_routes_host_loopback() -> None:
    source = {
        "backend": "etcd",
        "endpoint": "http://127.0.0.1:25279",
        "namespace": "n",
        "epoch": "e",
        "rpc_timeout_ms": 900,
    }

    assert launcher.docker_config(source) == {
        **source,
        "endpoint": "http://host.docker.internal:25279",
    }
    assert source["endpoint"] == "http://127.0.0.1:25279"


def test_docker_redis_config_routes_only_host_loopback_and_preserves_credentials() -> None:
    source = {
        "backend": "redis",
        "redis": {
            "host": "127.0.0.1",
            "port": 16393,
            "username": "lock-user",
            "password": "test-secret",
            "database": 4,
            "namespace": "matrix-ns",
            "coordination_epoch": "matrix-epoch",
            "rpc_timeout_ms": 900,
        },
    }

    converted = launcher.docker_config(source)

    assert converted["redis"] == {**source["redis"], "host": "host.docker.internal"}
    assert source["redis"]["host"] == "127.0.0.1"


def test_docker_redis_config_routes_shared_endpoint_format() -> None:
    source = {
        "backend": "redis",
        "endpoint": "redis://127.0.0.1:16391",
        "namespace": "matrix-ns",
        "epoch": "matrix-epoch",
    }

    converted = launcher.docker_config(source)

    assert converted["redis"] == {
        "host": "host.docker.internal",
        "port": 16391,
        "namespace": "matrix-ns",
        "coordination_epoch": "matrix-epoch",
        "rpc_timeout_ms": 200,
    }
    assert source == {
        "backend": "redis",
        "endpoint": "redis://127.0.0.1:16391",
        "namespace": "matrix-ns",
        "epoch": "matrix-epoch",
    }


@pytest.mark.parametrize(
    "endpoint",
    (
        "http://127.0.0.1:16391",
        "redis://redis.internal:16391",
        "redis://user:secret@127.0.0.1:16391",
        "redis://127.0.0.1",
        "redis://127.0.0.1:16391/1",
    ),
)
def test_docker_redis_endpoint_config_rejects_non_local_or_unsupported_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError, match="loopback Redis endpoint"):
        launcher.docker_config({
            "backend": "redis",
            "endpoint": endpoint,
            "namespace": "matrix-ns",
            "epoch": "matrix-epoch",
        })


@pytest.mark.parametrize("host", ("redis.internal", "0.0.0.0", "192.0.2.1", "host.docker.internal"))
def test_docker_redis_config_rejects_non_loopback_hosts(host: str) -> None:
    config = {"backend": "redis", "redis": {"host": host, "port": 6379}}
    with pytest.raises(ValueError, match="loopback Redis host"):
        launcher.docker_config(config)


@pytest.mark.parametrize("port", (0, 65536, "6379", True))
def test_docker_redis_config_rejects_invalid_ports(port: object) -> None:
    config = {"backend": "redis", "redis": {"host": "localhost", "port": port}}
    with pytest.raises(ValueError, match="valid loopback Redis port"):
        launcher.docker_config(config)


@pytest.mark.parametrize(
    "endpoint",
    (
        "https://127.0.0.1:25279",
        "http://127.0.0.1",
        "http://127.0.0.1:25279/path",
        "http://user:secret@127.0.0.1:25279",
        "http://example.com:25279",
    ),
)
def test_docker_config_rejects_non_local_or_unsupported_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError, match="loopback etcd endpoint"):
        launcher.docker_config({"backend": "etcd", "endpoint": endpoint})


def test_docker_command_is_local_pinned_and_read_only(monkeypatch) -> None:
    worker = LAUNCHER_PATH
    libraries = [(LAUNCHER_PATH.parent, target) for _, target in launcher.DOCKER_LIBRARY_DIRS]
    monkeypatch.setattr(launcher, "DOCKER_LIBRARY_DIRS", tuple(libraries))
    config = LAUNCHER_PATH

    command = launcher.docker_worker_command(config, worker)

    assert command[:5] == ["docker", "run", "--rm", "--pull=never", "-i"]
    assert launcher.DOCKER_WORKER_IMAGE in command
    assert all("readonly" in argument for argument in command if argument.startswith("type=bind"))
    assert "--network=host" not in command
    assert "-p" not in command and "--publish" not in command
    assert "host.docker.internal" not in command[-1]
