"""Run the prebuilt current Swift JSONL worker with persistent Docker stdio."""
from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "swift@sha256:eccc7a97f9b9881d9659e2e788081fd7675f86d39793b51200bfb178398b3784"
WORKER = ROOT / "sdk" / "swift" / ".build" / "x86_64-unknown-linux-gnu" / "debug" / "dilock-swift-worker"
REDIS_RESOURCES = ROOT / "sdk" / "swift" / ".build" / "x86_64-unknown-linux-gnu" / "debug" / "Dilock_DilockRedis.resources"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: launch_swift_worker_docker.py <config.json>", file=sys.stderr)
        return 2
    network = os.environ.get("DILOCK_M8_DOCKER_NETWORK")
    if not network:
        print("DILOCK_M8_DOCKER_NETWORK must name a task-owned internal Docker network", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).resolve()
    if not config.is_file():
        print("worker config must be inside the repository so Docker can mount it read-only", file=sys.stderr)
        return 2
    if not WORKER.is_file():
        print(f"prebuilt Swift worker is missing: {WORKER}", file=sys.stderr)
        return 2
    if not REDIS_RESOURCES.is_dir():
        print(f"Swift Redis resource bundle is missing: {REDIS_RESOURCES}", file=sys.stderr)
        return 2
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", dir=config.parent, delete=False) as mapped:
        mapped_path = Path(mapped.name)
        value = json.loads(config.read_text(encoding="utf-8"))
        value["endpoint"] = os.environ.get("DILOCK_M8_WORKER_ENDPOINT", value["endpoint"])
        json.dump(value, mapped)
    try:
        command = [
            "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
            "-v", f"{WORKER}:/dilock-swift-worker:ro",
            "-v", f"{REDIS_RESOURCES}:/Dilock_DilockRedis.resources:ro",
            "-v", f"{mapped_path.resolve()}:/worker-config.json:ro",
        ]
        if os.environ.get("DILOCK_WORKER_DIAGNOSTICS") == "1":
            command.extend(["-e", "DILOCK_WORKER_DIAGNOSTICS=1"])
        command.extend([IMAGE, "/dilock-swift-worker", "/worker-config.json"])
        return subprocess.run(command, stdin=sys.stdin.buffer, stdout=sys.stdout.buffer, stderr=sys.stderr.buffer, check=False).returncode
    finally:
        mapped_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
