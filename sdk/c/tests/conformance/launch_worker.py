"""Run the test-only C ABI worker in an explicitly supplied Docker container."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[4]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: launch_worker.py <config.json>", file=sys.stderr)
        return 2
    container = os.environ.get("DILOCK_C_WORKER_CONTAINER", "")
    if not container:
        print("DILOCK_C_WORKER_CONTAINER must name a task-owned disposable container", file=sys.stderr)
        return 2
    config_path = Path(sys.argv[1]).resolve()
    try:
        relative = config_path.relative_to(ROOT.resolve())
    except ValueError:
        print("worker config must be inside the mounted repository", file=sys.stderr)
        return 2
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        print("worker config is unreadable or invalid", file=sys.stderr)
        return 2

    backend = config.get("backend", "redis")
    if backend == "etcd":
        endpoint = config.get("endpoint")
        if isinstance(endpoint, str) and endpoint.startswith("http://127.0.0.1:"):
            config["endpoint"] = "http://host.docker.internal:" + endpoint.rsplit(":", 1)[1]
    elif backend == "redis":
        endpoint = config.get("endpoint")
        if isinstance(endpoint, str) and endpoint.startswith("redis://127.0.0.1:"):
            config["endpoint"] = "redis://host.docker.internal:" + endpoint.rsplit(":", 1)[1]
        redis = config.get("redis")
        if isinstance(redis, dict) and redis.get("host") in ("127.0.0.1", "localhost"):
            config["redis"] = {**redis, "host": "host.docker.internal"}

    fd, temporary_name = tempfile.mkstemp(prefix="dilock-c-worker-", suffix=".json",
                                          dir=ROOT / "validation/implementation")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(config, stream, separators=(",", ":"))
            stream.write("\n")
        container_config = "/workspace/" + relative.as_posix()
        if config != json.loads(config_path.read_text(encoding="utf-8")):
            container_config = "/workspace/validation/implementation/" + temporary.name
        worker = os.environ.get("DILOCK_C_WORKER_BINARY", "/tmp/dilock-c-conformance-worker")
        runtime_library_path = os.environ.get(
            "DILOCK_C_WORKER_LD_LIBRARY_PATH",
            "/tmp/c-m8-prefix/lib:/workspace/.tools/cppinstall/lib:"
            "/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu:"
            "/workspace/.tools/cppdeps/usr/lib/x86_64-linux-gnu:"
            "/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu:"
            "/usr/lib/x86_64-linux-gnu",
        )
        return subprocess.run(
            ["docker", "exec", "-i", "-e", f"LD_LIBRARY_PATH={runtime_library_path}",
             container, worker, container_config],
            stdin=sys.stdin.buffer,
            stdout=sys.stdout.buffer,
            stderr=sys.stderr.buffer,
            check=False,
        ).returncode
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
