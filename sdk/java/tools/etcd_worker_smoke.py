"""Exercise the Java worker against an explicitly provisioned local etcd namespace."""
from __future__ import annotations

import argparse
import json
import queue
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


class Worker:
    def __init__(self, endpoint: str, namespace: str, epoch: str) -> None:
        self.config = ROOT / ".tmp" / f"java-etcd-{uuid.uuid4().hex}.json"
        self.config.parent.mkdir(exist_ok=True)
        self.config.write_text(json.dumps({"backend": "etcd", "endpoint": endpoint, "namespace": namespace, "epoch": epoch}), encoding="utf-8")
        self.stderr_path = self.config.with_suffix(".stderr.log")
        self.stderr_file = self.stderr_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen([sys.executable, "sdk/java/worker_launcher.py", str(self.config)], cwd=ROOT,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr_file, text=True, bufsize=1)
        self.responses: queue.Queue[dict[str, object] | None] = queue.Queue()
        self.deferred: dict[object, dict[str, object]] = {}
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.counter = 0

    def _read(self) -> None:
        assert self.process.stdout
        for line in self.process.stdout:
            try:
                self.responses.put(json.loads(line))
            except json.JSONDecodeError:
                self.responses.put({"status": "non-json-output", "line": line[:300]})
        self.responses.put(None)

    def send(self, op: str, **fields: object) -> str:
        assert self.process.stdin
        self.counter += 1
        request_id = f"smoke-{self.counter}"
        self.process.stdin.write(json.dumps({"id": request_id, "op": op, **fields}) + "\n")
        self.process.stdin.flush()
        return request_id

    def wait(self, request_id: str, timeout: float = 12) -> dict[str, object]:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            remaining = end - time.monotonic()
            try:
                reply = self.responses.get(timeout=remaining)
            except queue.Empty as error:
                raise TimeoutError(f"worker did not answer {request_id}") from error
            if reply is None:
                raise RuntimeError(f"worker exited before reply {request_id}")
            if reply.get("id") == request_id:
                return reply
            self.deferred[reply.get("id")] = reply
        raise TimeoutError(f"worker did not answer {request_id}")

    def reply(self, request_id: str, timeout: float = 12) -> dict[str, object]:
        if request_id in self.deferred:
            return self.deferred.pop(request_id)
        return self.wait(request_id, timeout)

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self.send("shutdown")
                if self.process.stdin:
                    self.process.stdin.close()
                self.process.wait(timeout=12)
            except (OSError, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait(timeout=5)
        self.config.unlink(missing_ok=True)
        self.stderr_file.close()
        self.stderr_path.unlink(missing_ok=True)


def require(reply: dict[str, object], status: str) -> dict[str, object]:
    if reply.get("status") != status:
        raise AssertionError(f"expected {status}, got {reply}")
    return reply


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True, help="must already be provisioned; this tool never writes metadata")
    parser.add_argument("--epoch", required=True)
    args = parser.parse_args()
    worker = Worker(args.endpoint, args.namespace, args.epoch)
    suffix = uuid.uuid4().hex
    try:
        require(worker.reply(worker.send("hello")), "ok")
        print("PASS hello", flush=True)
        require(worker.reply(worker.send("try_acquire", resource=f"java-smoke/queue/{suffix}", lease_ms=3000)), "unsupported_capability")
        first = require(worker.reply(worker.send("acquire", resource=f"java-smoke/queue/{suffix}", wait_ms=5000, lease_ms=8000)), "acquired")
        print("PASS first acquisition", flush=True)
        waiting = worker.send("acquire", resource=f"java-smoke/queue/{suffix}", lease_ms=3000, wait_ms=9000)
        time.sleep(3.8)
        owner_state = require(worker.reply(worker.send("inspect", lease_id=first["lease_id"])), "ok")
        if owner_state.get("state") != "held":
            raise AssertionError(f"owner lost while the second native waiter exceeded one TTL: {owner_state}")
        require(worker.reply(worker.send("release", lease_id=first["lease_id"])), "released")
        require(worker.reply(worker.send("release", lease_id=first["lease_id"])), "released")
        successor = require(worker.reply(waiting, timeout=8), "acquired")
        first_revision = int(first["fence"]["value"])
        next_revision = int(successor["fence"]["value"])
        if next_revision <= first_revision:
            raise AssertionError(f"create_revision did not advance: {first_revision} then {next_revision}")
        require(worker.reply(worker.send("release", lease_id=successor["lease_id"])), "released")

        renewal_resource = f"java-smoke/auto-renew/{suffix}"
        owner_hello = require(worker.reply(worker.send("hello")), "ok")
        if not owner_hello["capabilities"].get("auto_renew"):
            raise AssertionError(f"Java etcd worker did not advertise auto_renew: {owner_hello}")
        renewed = require(worker.reply(worker.send("acquire", resource=renewal_resource, wait_ms=5000,
                                                   lease_ms=3000, auto_renew=True)), "acquired")
        time.sleep(4.0)
        blocked = require(worker.reply(worker.send("acquire", resource=renewal_resource, wait_ms=1200,
                                                   lease_ms=3000, auto_renew=False), timeout=6), "timeout")
        if blocked["status"] != "timeout":
            raise AssertionError(f"auto-renewed etcd lease did not block contender beyond TTL: {blocked}")
        require(worker.reply(worker.send("release", lease_id=renewed["lease_id"])), "released")
        handoff = require(worker.reply(worker.send("acquire", resource=renewal_resource, wait_ms=5000,
                                                   lease_ms=3000, auto_renew=False)), "acquired")
        require(worker.reply(worker.send("release", lease_id=handoff["lease_id"])), "released")
        print("PASS auto_renew=true holds beyond original server TTL and stops on acknowledged release", flush=True)
        print("PASS queue keepalive, repeat release, and monotonic create_revision", flush=True)

        expiring = require(worker.reply(worker.send("acquire", resource=f"java-smoke/expiry/{suffix}", wait_ms=5000, lease_ms=3000)), "acquired")
        time.sleep(3.5)
        expired_state = require(worker.reply(worker.send("inspect", lease_id=expiring["lease_id"])), "ok")
        if expired_state.get("state") != "lost":
            raise AssertionError(f"auto_renew=false lease did not expire: {expired_state}")
        replacement = require(worker.reply(worker.send("acquire", resource=f"java-smoke/expiry/{suffix}", wait_ms=5000, lease_ms=3000)), "acquired")
        if int(replacement["fence"]["value"]) <= int(expiring["fence"]["value"]):
            raise AssertionError("create_revision did not advance after TTL expiry")
        require(worker.reply(worker.send("release", lease_id=replacement["lease_id"])), "released")
        print("PASS auto_renew=false expiry and reacquisition", flush=True)

        held = require(worker.reply(worker.send("acquire", resource=f"java-smoke/cancel/{suffix}", wait_ms=5000, lease_ms=3000)), "acquired")
        cancelled = worker.send("acquire", resource=f"java-smoke/cancel/{suffix}", lease_ms=3000, wait_ms=9000)
        time.sleep(0.250)
        require(worker.reply(worker.send("cancel_request", target_id=cancelled)), "ok")
        cancellation = require(worker.reply(cancelled), "cancelled")
        if cancellation.get("remote_outcome_unknown") is not False:
            raise AssertionError(f"expected cancellation cleanup confirmation: {cancellation}")
        require(worker.reply(worker.send("release", lease_id=held["lease_id"])), "released")
        reacquired = require(worker.reply(worker.send("acquire", resource=f"java-smoke/cancel/{suffix}", wait_ms=5000, lease_ms=3000)), "acquired")
        require(worker.reply(worker.send("release", lease_id=reacquired["lease_id"])), "released")
        print("PASS Java etcd worker: queue-time keepalive, opt-in held auto-renewal, held lease expiry, repeat release, monotonic create_revision, confirmed cancel/revoke and reacquire")
        return 0
    finally:
        worker.close()


if __name__ == "__main__":
    raise SystemExit(main())
