"""Smoke-test the TypeScript JSONL worker against an explicitly provisioned local etcd namespace."""
from __future__ import annotations

import argparse
import json
import queue
import subprocess
import threading
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True, help="must already be provisioned; this tool never writes metadata")
    parser.add_argument("--epoch", required=True)
    args = parser.parse_args()
    config = ROOT / ".tmp" / f"typescript-etcd-{uuid.uuid4().hex}.json"
    config.parent.mkdir(exist_ok=True)
    config.write_text(json.dumps({"backend": "etcd", "endpoint": args.endpoint, "namespace": args.namespace, "epoch": args.epoch}), encoding="utf-8")
    process = subprocess.Popen(["node", "sdk/typescript/worker-launcher.mjs", str(config)], cwd=ROOT,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    responses: queue.Queue[dict[str, object] | None] = queue.Queue()
    def read() -> None:
        assert process.stdout
        for line in process.stdout:
            try: responses.put(json.loads(line))
            except json.JSONDecodeError: responses.put({"status": "non-json-output", "line": line[:300]})
        responses.put(None)
    threading.Thread(target=read, daemon=True).start()
    counter = 0
    def call(op: str, **fields: object) -> dict[str, object]:
        nonlocal counter
        counter += 1
        request_id = f"smoke-{counter}"
        assert process.stdin
        process.stdin.write(json.dumps({"id": request_id, "op": op, **fields}) + "\n")
        process.stdin.flush()
        try: response = responses.get(timeout=10)
        except queue.Empty as error: raise TimeoutError(f"worker did not answer {request_id}") from error
        if response is None: raise RuntimeError(f"worker exited before reply {request_id}")
        if response.get("id") != request_id: raise RuntimeError(f"expected reply {request_id}, received {response}")
        return response
    try:
        if call("hello").get("status") != "ok": raise AssertionError("hello failed")
        suffix = uuid.uuid4().hex
        unsupported = call("try_acquire", resource=f"typescript-worker-smoke/{suffix}", lease_ms=3000)
        if unsupported.get("status") != "unsupported_capability": raise AssertionError(f"native try-acquire must be explicit: {unsupported}")
        acquired = call("acquire", resource=f"typescript-worker-smoke/{suffix}", wait_ms=5000, lease_ms=3000)
        if acquired.get("status") != "acquired" or not isinstance(acquired.get("fence"), dict): raise AssertionError(f"invalid etcd acquisition reply: {acquired}")
        lease_id = acquired["lease_id"]
        if call("inspect", lease_id=lease_id).get("state") != "held": raise AssertionError("local inspection failed")
        if call("release", lease_id=lease_id).get("status") != "released": raise AssertionError("release failed")
        if call("shutdown").get("status") != "ok": raise AssertionError("shutdown failed")
        process.stdin.close()
        if process.wait(timeout=10) != 0: raise RuntimeError("worker exited with an error")
        print("PASS TypeScript etcd worker JSONL hello/acquire/fence/inspect/release/shutdown")
        return 0
    finally:
        if process.poll() is None:
            if process.stdin and not process.stdin.closed: process.stdin.close()
            process.kill(); process.wait(timeout=5)
        config.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
