"""Launch the Python conformance worker with this platform's local venv."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: launch_python_worker.py <config.json>", file=sys.stderr)
        return 2
    venv_python = ROOT / ".tools" / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    python = str(venv_python) if venv_python.is_file() else sys.executable
    worker = ROOT / "sdk" / "python" / "worker.py"
    return subprocess.run([python, str(worker), str(Path(sys.argv[1]).resolve())], cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
