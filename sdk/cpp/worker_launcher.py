"""Launch the locally built C++ conformance worker on Windows or Linux."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


ROOT = Path(__file__).resolve().parents[2]
DOCKER_WORKER_IMAGE = "debian@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f"
DOCKER_WORKER_BINARY = "/opt/dilock/worker"
DOCKER_CONFIG_PATH = "/run/dilock/config.json"
DOCKER_LIBRARY_DIRS = (
    (ROOT / ".tools/grpc-root/usr/lib/x86_64-linux-gnu", "/opt/dilock/grpc-lib"),
    (ROOT / ".tools/cppinstall/lib", "/opt/dilock/cpp-lib"),
    (ROOT / ".tools/cppdeps/usr/lib/x86_64-linux-gnu", "/opt/dilock/cppdeps-lib"),
    (ROOT / ".tools/openssl-runtime/usr/lib/x86_64-linux-gnu", "/opt/dilock/openssl-lib"),
)


def docker_config(config: dict) -> dict:
    """Route only a loopback backend endpoint from Docker back to the host."""
    backend = config.get("backend")
    if backend == "etcd":
        endpoint = config.get("endpoint")
        if not isinstance(endpoint, str):
            raise ValueError("the C++ etcd worker config requires an endpoint")
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in ("127.0.0.1", "localhost")
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Docker worker requires an uncredentialed loopback etcd endpoint")
        return {
            **config,
            "endpoint": urlunsplit(("http", f"host.docker.internal:{parsed.port}", "", "", "")),
        }
    if backend == "redis":
        redis = config.get("redis")
        if not isinstance(redis, dict):
            endpoint = config.get("endpoint")
            if not isinstance(endpoint, str):
                raise ValueError("the C++ Redis worker config requires Redis options or an endpoint")
            parsed = urlsplit(endpoint)
            docker_network = os.environ.get("DILOCK_CPP_DOCKER_NETWORK")
            if (docker_network and parsed.scheme == "redis"
                    and parsed.hostname in ("primary", "replica") and parsed.port == 6379
                    and parsed.path in ("", "/") and not parsed.query and not parsed.fragment):
                namespace = config.get("namespace")
                epoch = config.get("epoch")
                if not isinstance(namespace, str) or not namespace or not isinstance(epoch, str) or not epoch:
                    raise ValueError("the C++ Redis worker config requires namespace and epoch")
                return {
                    **config,
                    "redis": {
                        "host": parsed.hostname,
                        "port": parsed.port,
                        "namespace": namespace,
                        "coordination_epoch": epoch,
                        "rpc_timeout_ms": 200,
                    },
                }
            if (
                parsed.scheme != "redis"
                or parsed.hostname not in ("127.0.0.1", "localhost")
                or parsed.port is None
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in ("", "/")
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("Docker worker requires an uncredentialed loopback Redis endpoint")
            namespace = config.get("namespace")
            epoch = config.get("epoch")
            if not isinstance(namespace, str) or not namespace or not isinstance(epoch, str) or not epoch:
                raise ValueError("the C++ Redis worker config requires namespace and epoch")
            redis = {
                "host": parsed.hostname,
                "port": parsed.port,
                "namespace": namespace,
                "coordination_epoch": epoch,
                "rpc_timeout_ms": 200,
            }
        host = redis.get("host")
        port = redis.get("port")
        if host not in ("127.0.0.1", "localhost"):
            raise ValueError("Docker worker requires a loopback Redis host")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("Docker worker requires a valid loopback Redis port")
        return {**config, "redis": {**redis, "host": "host.docker.internal"}}
    raise ValueError("Docker C++ worker mode requires backend redis or etcd")


def docker_worker_command(config_path: Path, worker_path: Path) -> list[str]:
    """Build a fail-closed, local-only Docker command for the C++ worker."""
    if not worker_path.is_file():
        raise FileNotFoundError(f"Docker C++ worker is missing: {worker_path}")
    missing = [str(path) for path, _ in DOCKER_LIBRARY_DIRS if not path.is_dir()]
    if missing:
        raise FileNotFoundError(f"Docker C++ runtime libraries are missing: {missing}")
    command = ["docker", "run", "--rm", "--pull=never", "-i"]
    docker_network = os.environ.get("DILOCK_CPP_DOCKER_NETWORK")
    if docker_network:
        command.extend(("--network", docker_network))
    command.extend((
        "--mount",
        f"type=bind,source={worker_path},target={DOCKER_WORKER_BINARY},readonly",
        "--mount",
        f"type=bind,source={config_path},target={DOCKER_CONFIG_PATH},readonly",
    ))
    library_path = []
    for host_path, container_path in DOCKER_LIBRARY_DIRS:
        command.extend(("--mount", f"type=bind,source={host_path},target={container_path},readonly"))
        library_path.append(container_path)
    command.extend((
        DOCKER_WORKER_IMAGE,
        "/bin/sh",
        "-lc",
        f"LD_LIBRARY_PATH={':'.join(library_path)} exec {DOCKER_WORKER_BINARY} {DOCKER_CONFIG_PATH}",
    ))
    return command


def run_docker_worker(source_config: Path) -> int:
    """Run the current Linux backend worker with the conformance stdio protocol."""
    worker_path = Path(os.environ.get(
        "DILOCK_CPP_DOCKER_WORKER_PATH",
        ROOT / ".tools/dilock-cpp-etcd-docker-build/dilock_cpp_worker",
    )).resolve()
    source = json.loads(source_config.read_text(encoding="utf-8"))
    converted = docker_config(source)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", prefix="cpp-docker-",
        suffix=".json", dir=source_config.parent, delete=False,
    ) as stream:
        temporary_config = Path(stream.name)
        json.dump(converted, stream, separators=(",", ":"))
        stream.write("\n")
    try:
        command = docker_worker_command(temporary_config, worker_path)
        return subprocess.run(
            command,
            stdin=sys.stdin.buffer,
            stdout=sys.stdout.buffer,
            stderr=sys.stderr.buffer,
            check=False,
        ).returncode
    except (OSError, ValueError) as error:
        print(f"could not start local Docker C++ backend worker: {error}", file=sys.stderr)
        return 2
    finally:
        temporary_config.unlink(missing_ok=True)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).resolve()
    try:
        backend = json.loads(config.read_text(encoding="utf-8")).get("backend", "redis")
    except (OSError, json.JSONDecodeError):
        backend = "redis"
    if backend in ("redis", "etcd") and os.environ.get("DILOCK_CPP_DOCKER_WORKER") == "1":
        return run_docker_worker(config)
    executable_candidates = [] if backend == "etcd" else [
        ROOT / "build/cpp/Release/dilock_cpp_worker.exe",
        ROOT / "build/cpp/dilock_cpp_worker.exe",
    ]
    if os.name != "nt":
        executable_candidates.extend([
            ROOT / "build/cpp/dilock_cpp_worker",
            ROOT / "build/cpp/Release/dilock_cpp_worker",
        ])
    for executable in executable_candidates:
        if executable.is_file():
            return subprocess.run([str(executable), str(config)], check=False).returncode

    linux_worker = Path(os.environ.get("DILOCK_CPP_WSL_WORKER", ROOT / "build/cpp/dilock_cpp_worker"))
    if os.name == "nt" and linux_worker.is_file():
        distribution = os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04")

        def wsl_path(path: Path) -> str:
            result = subprocess.run(
                ["wsl.exe", "-d", distribution, "--exec", "wslpath", "-a", str(path)],
                check=True,
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
            )
            return result.stdout.strip()

        wsl_command = ["wsl.exe", "-d", distribution, "--exec"]
        linux_library_path = os.environ.get("DILOCK_CPP_WSL_LD_LIBRARY_PATH")
        if linux_library_path:
            wsl_command.extend(["env", f"LD_LIBRARY_PATH={linux_library_path}"])
        wsl_command.extend([wsl_path(linux_worker), wsl_path(config)])
        process = subprocess.Popen(
            wsl_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr.buffer,
        )

        def send_requests() -> None:
            assert process.stdin is not None
            try:
                while line := sys.stdin.buffer.readline():
                    process.stdin.write(line)
                    process.stdin.flush()
            finally:
                process.stdin.close()

        def forward_responses() -> None:
            assert process.stdout is not None
            while chunk := process.stdout.read1(4096):
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()

        input_thread = threading.Thread(target=send_requests, daemon=True)
        output_thread = threading.Thread(target=forward_responses, daemon=True)
        input_thread.start()
        output_thread.start()
        result = process.wait()
        input_thread.join()
        output_thread.join()
        return result

    print("C++ worker is not built; run cmake -S sdk/cpp -B build/cpp and build dilock_cpp_worker.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
