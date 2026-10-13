"""Launch the locally built Rust JSON Lines conformance worker."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    name = "dilock-rust-worker.exe" if os.name == "nt" else "dilock-rust-worker"
    worker = ROOT / "sdk" / "rust" / "target" / "release" / name
    if not worker.is_file():
        print(f"Rust conformance worker is not built: {worker}", file=sys.stderr)
        return 2
    return subprocess.run([str(worker), str(Path(sys.argv[1]).resolve())], cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
