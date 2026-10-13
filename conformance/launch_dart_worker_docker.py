"""Run the current Dart JSONL worker in a pinned, task-owned Docker network."""
from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "dart@sha256:5ac89dbcae4327278b257920e2786df0f22c87adc630017266b67cfcceef8348"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: launch_dart_worker_docker.py <config.json>", file=sys.stderr)
        return 2
    network = os.environ.get("DILOCK_M8_DOCKER_NETWORK")
    if not network:
        print("DILOCK_M8_DOCKER_NETWORK must name a task-owned internal Docker network", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).resolve()
    try:
        relative = config.relative_to(ROOT)
    except ValueError:
        print("worker config must be inside the repository so Docker can mount it read-only", file=sys.stderr)
        return 2
    executable = os.environ.get("DILOCK_M8_DART_WORKER")
    if not executable or not Path(executable).is_file():
        print("DILOCK_M8_DART_WORKER must point to the AOT executable built from this checkout", file=sys.stderr)
        return 2
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", dir=config.parent, delete=False) as mapped:
        mapped_path = Path(mapped.name)
        value = json.loads(config.read_text(encoding="utf-8"))
        value["endpoint"] = os.environ.get("DILOCK_M8_WORKER_ENDPOINT", value["endpoint"])
        json.dump(value, mapped)
    try:
        command = [
            "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
            "-v", f"{Path(executable).resolve()}:/tmp/dilock-dart-worker:ro",
            "-v", f"{mapped_path.resolve()}:/worker-config.json:ro",
            IMAGE, "/tmp/dilock-dart-worker", "/worker-config.json",
        ]
        return subprocess.run(command, stdin=sys.stdin.buffer, stdout=sys.stdout.buffer, stderr=sys.stderr.buffer, check=False).returncode
    finally:
        mapped_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
