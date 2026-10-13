"""Test-only bridge from the shared conformance runner to the Kotlin worker.

The runner currently adds its local RPC deadline (`rpc_ms`) to acquire
requests, although that field is not part of the shared worker protocol. Keep
the Kotlin worker strict and strip only that harness field at this boundary.
This launcher is not included in the published Kotlin artifacts.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER_MAIN = "org.dilock.kotlin.worker.ConformanceWorkerKt"
WORKER_CLASSPATH = ROOT / "sdk" / "kotlin" / "worker" / "build" / "install" / "worker" / "lib" / "*"


def strip_harness_rpc_deadline(line: bytes) -> bytes:
    """Remove only the out-of-protocol rpc_ms field from acquire JSONL lines."""
    try:
        message = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return line
    if not isinstance(message, dict) or message.get("op") not in {"acquire", "try_acquire"}:
        return line
    if "rpc_ms" not in message:
        return line
    del message["rpc_ms"]
    return (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _forward_requests(child: subprocess.Popen[bytes]) -> None:
    assert child.stdin is not None
    try:
        for line in sys.stdin.buffer:
            child.stdin.write(strip_harness_rpc_deadline(line))
            child.stdin.flush()
    except BrokenPipeError:
        pass
    finally:
        try:
            child.stdin.close()
        except OSError:
            pass


def _forward_responses(child: subprocess.Popen[bytes]) -> None:
    assert child.stdout is not None
    for line in child.stdout:
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: conformance_launcher.py <worker-config.json>", file=sys.stderr)
        return 2
    java = shutil.which("java")
    if java is None:
        print("Kotlin conformance worker requires java on PATH", file=sys.stderr)
        return 127
    if not WORKER_CLASSPATH.parent.is_dir():
        print("Kotlin worker is not built; run :worker:installDist first", file=sys.stderr)
        return 127

    try:
        child = subprocess.Popen(
            [java, "-cp", str(WORKER_CLASSPATH), WORKER_MAIN, argv[1]],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
        )
    except OSError as error:
        print(f"could not start Kotlin conformance worker: {error}", file=sys.stderr)
        return 127
    assert child.stdin is not None and child.stdout is not None
    writer = threading.Thread(target=_forward_requests, args=(child,), daemon=True)
    reader = threading.Thread(target=_forward_responses, args=(child,), daemon=True)
    writer.start()
    reader.start()
    result = child.wait()
    writer.join(timeout=2)
    reader.join(timeout=2)
    return result


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
