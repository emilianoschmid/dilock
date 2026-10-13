"""Exercise hello and one backend capability through the local worker launcher."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = ROOT / "sdk" / "elixir" / "worker_launcher.py"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_capability_probe.py <config.json>", file=sys.stderr)
        return 2

    config = Path(sys.argv[1]).resolve()
    process = subprocess.Popen(
        [sys.executable, str(LAUNCHER), str(config)],
        cwd=ROOT,
        env=os.environ.copy(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def request(value: dict) -> dict:
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write((json.dumps(value) + "\n").encode("utf-8"))
        process.stdin.flush()
        line = process.stdout.readline()
        if not line:
            raise RuntimeError("worker exited without a JSONL response")
        response = json.loads(line)
        if response.get("id") != value["id"]:
            raise RuntimeError(f"response id mismatch: {response}")
        return response

    try:
        hello = request({"id": "probe-hello", "op": "hello"})
        if hello.get("status") != "ok" or hello.get("capabilities", {}).get("try_acquire") is not False:
            raise RuntimeError(f"unexpected etcd hello capability: {hello}")

        attempt = request({
            "id": "probe-try",
            "op": "try_acquire",
            "resource": "worker-capability-probe",
            "wait_ms": 0,
            "lease_ms": 2_500,
            "auto_renew": False,
        })
        if attempt.get("status") != "unsupported_capability":
            raise RuntimeError(f"etcd try_acquire did not fail explicitly: {attempt}")

        shutdown = request({"id": "probe-shutdown", "op": "shutdown"})
        if shutdown.get("status") != "ok":
            raise RuntimeError(f"worker did not acknowledge shutdown: {shutdown}")
        process.stdin.close()
        exit_code = process.wait(timeout=10)
        if exit_code != 0:
            raise RuntimeError(f"worker exited with status {exit_code}")

        print(json.dumps({
            "hello": "PASS",
            "advertised_try_acquire": False,
            "try_acquire_response": attempt["status"],
            "shutdown": "PASS",
            "exit_code": exit_code,
        }))
        return 0
    except Exception as exc:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
        stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
        print(f"capability probe failed: {exc}; stderr={stderr}", file=sys.stderr)
        return 1
    finally:
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        if process.stderr is not None:
            process.stderr.close()
        if process.stdout is not None:
            process.stdout.close()


if __name__ == "__main__":
    raise SystemExit(main())
