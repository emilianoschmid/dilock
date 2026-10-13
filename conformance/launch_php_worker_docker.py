"""Run the current PHP JSONL worker from the pinned local PHP runtime image."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "dilock-php-core-interop:20261008"
IMAGE_ID = "sha256:a933423111912bc83f5a9e24cd221db41eb87ddf322b4a2f0e933ceaa5694441"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: launch_php_worker_docker.py <config.json>", file=sys.stderr)
        return 2
    network = os.environ.get("DILOCK_M8_DOCKER_NETWORK")
    if not network:
        print("DILOCK_M8_DOCKER_NETWORK must name a task-owned internal Docker network", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).resolve()
    try:
        config.relative_to(ROOT)
    except ValueError:
        print("worker config must be inside the repository so Docker can mount it read-only", file=sys.stderr)
        return 2
    if not config.is_file():
        print("worker config does not exist", file=sys.stderr)
        return 2

    inspect = subprocess.run(
        ["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, check=False,
    )
    if inspect.returncode != 0 or inspect.stdout.strip() != IMAGE_ID:
        print("the pinned local PHP runtime image is unavailable or has a different image ID", file=sys.stderr)
        return 2

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", dir=config.parent, delete=False) as mapped:
        mapped_path = Path(mapped.name)
        value = json.loads(config.read_text(encoding="utf-8"))
        value["endpoint"] = os.environ.get("DILOCK_M8_WORKER_ENDPOINT", value["endpoint"])
        json.dump(value, mapped)
    try:
        command = [
            "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
            "-v", f"{(ROOT / 'sdk/php/src').resolve()}:/workspace/sdk/php/src:ro",
            "-v", f"{(ROOT / 'sdk/php/bin').resolve()}:/workspace/sdk/php/bin:ro",
            "-v", f"{mapped_path.resolve()}:/tmp/dilock-worker-config.json:ro",
            IMAGE_ID, "php", "/workspace/sdk/php/bin/dilock-worker", "/tmp/dilock-worker-config.json",
        ]
        return subprocess.run(command, stdin=sys.stdin.buffer, stdout=sys.stdout.buffer,
                              stderr=sys.stderr.buffer, check=False).returncode
    finally:
        mapped_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
