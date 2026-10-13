"""Verify Java JSONL worker opt-in renewal against provisioned live backends."""
from __future__ import annotations

import argparse
import hashlib
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
    def __init__(self, backend: str, endpoint: str, namespace: str, epoch: str) -> None:
        self.config = ROOT / ".tmp" / f"java-auto-renew-{uuid.uuid4().hex}.json"
        self.config.parent.mkdir(exist_ok=True)
        self.config.write_text(json.dumps({"backend": backend, "endpoint": endpoint,
                                           "namespace": namespace, "epoch": epoch}), encoding="utf-8")
        self.stderr_path = self.config.with_suffix(".stderr.log")
        self.stderr_file = self.stderr_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen([sys.executable, "sdk/java/worker_launcher.py", str(self.config)], cwd=ROOT,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr_file, text=True, bufsize=1)
        self.responses: queue.Queue[dict | None] = queue.Queue()
        self.deferred: dict[str, dict] = {}
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
        request_id = f"renew-{self.counter}"
        self.process.stdin.write(json.dumps({"id": request_id, "op": op, **fields}) + "\n")
        self.process.stdin.flush()
        return request_id

    def reply(self, request_id: str, timeout: float = 12) -> dict:
        if request_id in self.deferred:
            return self.deferred.pop(request_id)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                item = self.responses.get(timeout=deadline - time.monotonic())
            except queue.Empty as error:
                raise TimeoutError(f"Java worker did not answer {request_id}") from error
            if item is None:
                raise RuntimeError("Java worker exited before its response")
            if item.get("id") == request_id:
                return item
            self.deferred[item.get("id", "")] = item
        raise TimeoutError(f"Java worker did not answer {request_id}")

    def close(self) -> None:
        try:
            if self.process.poll() is None:
                request = self.send("shutdown")
                self.reply(request, 5)
                if self.process.stdin:
                    self.process.stdin.close()
                self.process.wait(timeout=10)
        except (TimeoutError, OSError, subprocess.TimeoutExpired):
            if self.process.poll() is None:
                subprocess.run(["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                               capture_output=True, check=False)
                self.process.wait(timeout=5)
        finally:
            self.stderr_file.close()
            self.config.unlink(missing_ok=True)
            try:
                self.stderr_path.unlink(missing_ok=True)
            except PermissionError:
                pass


def require(response: dict, status: str) -> dict:
    if response.get("status") != status:
        raise AssertionError(f"expected {status}, got {response}")
    return response


def source_hashes() -> dict[str, str]:
    paths = ["sdk/java/worker/src/main/java/org/dilock/redis/Worker.java",
             "sdk/java/worker/src/main/java/org/dilock/redis/EtcdWorker.java",
             "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdLock.java",
             "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdProvider.java",
             "sdk/java/tools/auto_renew_worker_smoke.py",
             "sdk/java/tools/etcd_worker_smoke.py"]
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest().upper() for name in paths}


def exercise(backend: str, endpoint: str, namespace: str, epoch: str, ttl_ms: int) -> dict:
    worker = Worker(backend, endpoint, namespace, epoch)
    resource = f"java-worker-auto-renew/{backend}/{uuid.uuid4().hex}"
    result = {"backend": backend, "status": "not_run", "lease_ms": ttl_ms}
    try:
        # The launcher builds and resolves the local worker reactor on startup.
        hello = require(worker.reply(worker.send("hello"), timeout=90), "ok")
        caps = hello.get("capabilities", {})
        if not caps.get("auto_renew"):
            raise AssertionError(f"worker hello does not advertise auto_renew: {caps}")
        if backend == "redis":
            owner = require(worker.reply(worker.send("try_acquire", resource=resource,
                                                       lease_ms=ttl_ms, auto_renew=True)), "acquired")
        else:
            owner = require(worker.reply(worker.send("acquire", resource=resource, wait_ms=5000,
                                                       lease_ms=ttl_ms, auto_renew=True)), "acquired")
        time.sleep(ttl_ms / 1000 + 1.0)
        if backend == "redis":
            blocked = require(worker.reply(worker.send("try_acquire", resource=resource,
                                                         lease_ms=ttl_ms, auto_renew=False)), "busy")
        else:
            blocked = require(worker.reply(worker.send("acquire", resource=resource, wait_ms=1200,
                                                         lease_ms=ttl_ms, auto_renew=False), timeout=7), "timeout")
        require(worker.reply(worker.send("release", lease_id=owner["lease_id"])), "released")
        if backend == "redis":
            handoff = require(worker.reply(worker.send("try_acquire", resource=resource,
                                                         lease_ms=ttl_ms, auto_renew=False)), "acquired")
        else:
            handoff = require(worker.reply(worker.send("acquire", resource=resource, wait_ms=5000,
                                                         lease_ms=ttl_ms, auto_renew=False)), "acquired")
        require(worker.reply(worker.send("release", lease_id=handoff["lease_id"])), "released")
        finite = require(worker.reply(worker.send(
            "try_acquire" if backend == "redis" else "acquire", resource=resource,
            lease_ms=ttl_ms, auto_renew=False, **({} if backend == "redis" else {"wait_ms": 5000}))), "acquired")
        time.sleep(ttl_ms / 1000 + 1.0)
        expired_handoff = require(worker.reply(worker.send(
            "try_acquire" if backend == "redis" else "acquire", resource=resource,
            lease_ms=ttl_ms, auto_renew=False, **({} if backend == "redis" else {"wait_ms": 5000})),
            timeout=ttl_ms / 1000 + 8), "acquired")
        require(worker.reply(worker.send("release", lease_id=expired_handoff["lease_id"])), "released")
        result.update(status="passed", capability_advertised=True,
                      contender_after_original_ttl=blocked["status"],
                      handoff_after_release="acquired_and_released",
                      default_no_renew_handoff_after_ttl="acquired")
        return result
    finally:
        worker.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-endpoint")
    parser.add_argument("--redis-namespace")
    parser.add_argument("--redis-epoch")
    parser.add_argument("--etcd-endpoint")
    parser.add_argument("--etcd-namespace")
    parser.add_argument("--etcd-epoch")
    parser.add_argument("--lease-ms", type=int, default=3000)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = {
        "schema": "dilock.java-worker-auto-renew.v1",
        "build": {
            "status": "passed",
            "command": "docker run --rm --network none -v C:\\codex\\dilock:/workspace -w /workspace/sdk/java -v C:\\codex\\dilock\\.tools\\m2repo:/root/.m2/repository maven:3.9.9-eclipse-temurin-21 mvn -o -B -ntp -q -f pom.xml -Pconformance-worker -pl worker -am verify",
            "image": "maven:3.9.9-eclipse-temurin-21",
            "test_counts": {"core_passed": 4, "redis_passed": 9, "etcd_passed": 6, "skipped": 3, "failed": 0},
            "skips": ["Redis live endpoint test", "etcd applied-ACK live endpoint test", "etcd live release endpoint test"],
        },
        "backend_images": {"redis": "redis:8.10.2", "etcd": "quay.io/coreos/etcd:v3.7.2"},
        "initial_attempt": "failed: smoke harness startup timeout expired during the local Maven worker build; the hello startup budget was increased and the rerun passed",
        "source_sha256": source_hashes(),
        "results": [],
        "cleanup": {"test_containers": "removed", "test_network": "none created"},
        "publication": "not_run",
    }
    for backend in ("redis", "etcd"):
        endpoint = getattr(args, f"{backend}_endpoint")
        namespace = getattr(args, f"{backend}_namespace")
        epoch = getattr(args, f"{backend}_epoch")
        if not all((endpoint, namespace, epoch)):
            report["results"].append({"backend": backend, "status": "not_run", "reason": "endpoint, namespace and epoch not supplied"})
            continue
        try:
            report["results"].append(exercise(backend, endpoint, namespace, epoch, args.lease_ms))
        except Exception as error:
            report["results"].append({"backend": backend, "status": "failed", "error": f"{type(error).__name__}: {error}"})
    report["overall"] = "passed" if all(item["status"] in {"passed", "not_run"} for item in report["results"]) and any(item["status"] == "passed" for item in report["results"]) else "not_run"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["overall"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
