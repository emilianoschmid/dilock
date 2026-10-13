"""Launch the Ruby JSONL worker in the pinned local Docker runtime image."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SDK = ROOT / "sdk" / "ruby"
IMAGE = "dilock-ruby-core-interop:20261007"
IMAGE_ID = "sha256:c3a848a223c5ecd999881bd923ab21932cc8c70567a4303ca3d7be7817dc7e37"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    network = os.environ.get("DILOCK_M8_DOCKER_NETWORK")
    if not network:
        print("DILOCK_M8_DOCKER_NETWORK must name a task-owned Docker network", file=sys.stderr)
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
        print("the pinned local Ruby runtime image is unavailable or has a different image ID", file=sys.stderr)
        return 2

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json",
                                     dir=config.parent, delete=False) as mapped:
        mapped_path = Path(mapped.name)
        value = json.loads(config.read_text(encoding="utf-8"))
        value["endpoint"] = os.environ.get("DILOCK_M8_WORKER_ENDPOINT", value["endpoint"])
        json.dump(value, mapped)
    try:
        command = [
            "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
            "-v", f"{SDK.resolve()}:/workspace/sdk/ruby:ro",
            "-v", f"{mapped_path.resolve()}:/worker-config.json:ro",
            "--workdir", "/workspace/sdk/ruby", IMAGE_ID,
            "ruby", "-Ilib", "bin/dilock-worker", "/worker-config.json",
        ]
        return subprocess.run(command, stdin=sys.stdin.buffer, stdout=sys.stdout.buffer,
                              stderr=sys.stderr.buffer, check=False).returncode
    finally:
        mapped_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
