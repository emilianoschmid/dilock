#!/usr/bin/env python3
"""Exercise applied etcd cleanup responses withheld from the current C++ SDK."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
WORKER_IMAGE = os.environ.get(
    "DILOCK_CPP_ETCD_ACK_WORKER_IMAGE",
    "debian@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f",
)
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.2@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd"
GO_IMAGE = "golang:1.26.8-bookworm@sha256:345775a9b624e690c1c0cd0755149bc8353e221d1c282596564e6d025fd294a9"
RUNTIME_DIRS = (
    (ROOT / ".tools/grpc-root/usr/lib/x86_64-linux-gnu", "/opt/dilock/grpc-lib"),
    (ROOT / ".tools/cppinstall/lib", "/opt/dilock/cpp-lib"),
    (ROOT / ".tools/cppdeps/usr/lib/x86_64-linux-gnu", "/opt/dilock/cppdeps-lib"),
    (ROOT / ".tools/openssl-runtime/usr/lib/x86_64-linux-gnu", "/opt/dilock/openssl-lib"),
)
PROXY_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"


def run(command: list[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    result = subprocess.run(command, text=True, capture_output=True, **kwargs)
    if check and result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}")
    return result


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Worker:
    def __init__(self, command: list[str]):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.responses: queue.Queue[str] = queue.Queue()
        self.stderr_lines: list[str] = []
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()
        self.counter = 0

    def _read_stdout(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.responses.put(line)

    def _read_stderr(self) -> None:
        assert self.process.stderr is not None
        self.stderr_lines.extend(self.process.stderr.readlines())

    def request(self, op: str, **fields) -> dict:
        self.counter += 1
        request_id = f"unlock-ack-{self.counter}"
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps({"id": request_id, "op": op, **fields}) + "\n")
        self.process.stdin.flush()
        try:
            response = json.loads(self.responses.get(timeout=15))
        except queue.Empty as error:
            raise RuntimeError(f"worker timed out on {op}; stderr={''.join(self.stderr_lines)}") from error
        if response.get("id") != request_id:
            raise RuntimeError(f"worker response id mismatch: {response}")
        return response

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                assert self.process.stdin is not None
                self.counter += 1
                self.process.stdin.write(json.dumps({"id": f"unlock-ack-{self.counter}", "op": "shutdown"}) + "\n")
                self.process.stdin.flush()
            except Exception:
                self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def worker_command(network: str, config: Path, worker: Path) -> list[str]:
    command = ["docker", "run", "--rm", "--pull=never", "-i", "--network", network,
               "--mount", f"type=bind,source={worker},target=/opt/dilock/worker,readonly",
               "--mount", f"type=bind,source={config},target=/run/dilock/config.json,readonly"]
    library_path = []
    for host_path, container_path in RUNTIME_DIRS:
        if not host_path.is_dir():
            raise FileNotFoundError(f"C++ Docker runtime dependency is missing: {host_path}")
        command.extend(("--mount", f"type=bind,source={host_path},target={container_path},readonly"))
        library_path.append(container_path)
    command.extend((WORKER_IMAGE, "/bin/sh", "-lc",
                    f"cp /opt/dilock/worker /tmp/dilock-worker && chmod 0755 /tmp/dilock-worker && "
                    f"LD_LIBRARY_PATH={':'.join(library_path)} exec /tmp/dilock-worker /run/dilock/config.json"))
    return command


def proxy_stats(network: str, proxy_name: str) -> dict:
    source = r'''package main
import (
	"io"
	"net/http"
	"os"
)
func main() {
	response, err := http.Get(os.Args[1])
	if err != nil { panic(err) }
	defer response.Body.Close()
	if _, err := io.Copy(os.Stdout, response.Body); err != nil { panic(err) }
}
'''
    script = "cat >/tmp/proxy-stats.go <<'EOF'\n" + source + "EOF\nGO111MODULE=off go run /tmp/proxy-stats.go http://" + proxy_name + ":2379/__stats"
    result = run(["docker", "run", "--rm", "--pull=never", "--network", network, GO_IMAGE, "sh", "-c", script])
    return json.loads(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=Path, default=ROOT / ".tools/dilock-cpp-etcd-docker-build/dilock_cpp_worker")
    parser.add_argument("--proxy", type=Path, default=PROXY_BINARY)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--drop-revoke-ack", action="store_true",
                        help="also withhold one successful LeaseRevoke response after etcd applies it")
    args = parser.parse_args()
    worker_path = args.worker.resolve()
    proxy_path = args.proxy.resolve()
    if not worker_path.is_file() or not proxy_path.is_file():
        raise FileNotFoundError("Build the current C++ etcd worker and offline Go proxy before running this test")

    suffix = uuid.uuid4().hex[:12]
    network = f"dilock-etcd-ack-{suffix}"
    etcd = f"dilock-etcd-{suffix}"
    proxy = f"dilock-proxy-{suffix}"
    namespace = f"m6-etcd-ack-{suffix}"
    epoch = f"local-etcd-ack-{suffix}"
    created_network = False
    created_containers: list[str] = []
    worker: Worker | None = None
    result: dict = {"status": "FAIL", "cleanup": "pending"}
    try:
        run(["docker", "network", "create", "--internal", network])
        created_network = True
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", etcd, ETCD_IMAGE,
             "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
             "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", f"http://{etcd}:2379",
             "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", f"http://{etcd}:2380",
             "--initial-cluster", f"node1=http://{etcd}:2380"])
        created_containers.append(etcd)
        proxy_command = ["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", proxy,
             "-e", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379"]
        if args.drop_revoke_ack:
            proxy_command.extend(("-e", "DILOCK_DROP_REVOKE_ACK=1"))
        proxy_command.extend((
             "--mount", f"type=bind,source={proxy_path},target=/opt/proxy,readonly", GO_IMAGE,
             "/bin/sh", "-c", "cp /opt/proxy /tmp/etcd-unlock-ack-proxy && chmod 0755 /tmp/etcd-unlock-ack-proxy && exec /tmp/etcd-unlock-ack-proxy"))
        run(proxy_command)
        created_containers.append(proxy)

        # Wait for etcd readiness without exposing ports or using external network.
        deadline = time.monotonic() + 20
        while True:
            health = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379", "endpoint", "health"], check=False)
            if health.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"etcd did not become ready: {health.stdout} {health.stderr}")
            time.sleep(0.25)

        metadata = json.dumps({"protocol_major": 1, "identity_scheme": "sha256-framed-utf8-v1",
                              "profile": "etcd-native-lock-v1", "coordination_epoch": epoch,
                              "fixed_lease_ms": None}, separators=(",", ":"))
        run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379", "put",
             f"/dilock/namespaces/{namespace}/meta", metadata])

        with tempfile.TemporaryDirectory(prefix="etcd-unlock-ack-", dir=ROOT / ".tools") as temp:
            config = Path(temp) / "config.json"
            config.write_text(json.dumps({"backend": "etcd", "endpoint": f"http://{proxy}:2379",
                                          "namespace": namespace, "epoch": epoch,
                                          "rpc_timeout_ms": 1000, "cleanup_timeout_ms": 2000}), encoding="utf-8")
            worker = Worker(worker_command(network, config, worker_path))
            owner = worker.request("acquire", resource="lost-unlock-ack", wait_ms=2000, lease_ms=30000)
            if owner.get("status") != "acquired":
                raise RuntimeError(f"owner acquisition failed: {owner}")
            released = worker.request("release", lease_id=owner["lease_id"])
            repeated = worker.request("release", lease_id=owner["lease_id"])
            inspect = worker.request("inspect", lease_id=owner["lease_id"])
            lock_scan = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379",
                             "get", "--prefix", "--keys-only", "/dilock/locks/"])
            owner_lock_keys = [line for line in lock_scan.stdout.splitlines() if line.startswith("/dilock/locks/")]
            if owner_lock_keys:
                raise RuntimeError(f"native lock keys remain after confirmed revoke: {owner_lock_keys}")
            successor = worker.request("acquire", resource="lost-unlock-ack", wait_ms=2000, lease_ms=30000)
            if successor.get("status") != "acquired":
                raise RuntimeError(f"successor did not acquire after confirmed revoke: {successor}")
            successor_release = worker.request("release", lease_id=successor["lease_id"])
            expected_release = "release_unknown" if args.drop_revoke_ack else "already_not_owned"
            if released.get("status") != expected_release or repeated.get("status") != expected_release:
                raise RuntimeError(f"expected cached {expected_release} after cleanup acknowledgements: {released} {repeated}")
            if inspect.get("status") != "lost":
                raise RuntimeError(f"released owner remained locally valid: {inspect}")
            if int(successor["fence"]["value"]) <= int(owner["fence"]["value"]):
                raise RuntimeError(f"successor fence did not increase: {owner} {successor}")
            if successor_release.get("status") != "released":
                raise RuntimeError(f"successor release failed: {successor_release}")

        stats = proxy_stats(network, proxy)
        expected = {"unlock_requests": 2, "unlock_applied": 2, "unlock_withheld": 1,
                    "revoke_requests": 2, "revoke_applied": 2,
                    "revoke_acknowledged": 1 if args.drop_revoke_ack else 2,
                    "revoke_withheld": 1 if args.drop_revoke_ack else 0,
                    "range_requests": 6, "range_applied": 6}
        if stats != expected:
            raise RuntimeError(f"proxy counters do not prove one applied-but-withheld ACK and no retry: {stats}")
        result.update({"status": "PASS", "backend": "etcd", "etcd_version": "3.7.2",
                       "owner_release": released["status"], "repeat_release": repeated["status"],
                       "owner_inspect": inspect["status"], "successor": successor["status"],
                       "successor_fence_increased": True, "successor_release": successor_release["status"],
                       "owner_lock_keys_after_revoke": owner_lock_keys,
                       "proxy": stats, "worker_image": WORKER_IMAGE, "go_image": GO_IMAGE,
                       "metrics_probe_image": GO_IMAGE, "etcd_image": ETCD_IMAGE,
                       "worker_sha256": file_sha256(worker_path),
                       "proxy_sha256": file_sha256(proxy_path), "namespace": namespace,
                       "current_source_sha256": {
                           "sdk/cpp/src/etcd.cpp": file_sha256(ROOT / "sdk/cpp/src/etcd.cpp"),
                           "sdk/cpp/worker.cpp": file_sha256(ROOT / "sdk/cpp/worker.cpp"),
                           "sdk/cpp/CMakeLists.txt": file_sha256(ROOT / "sdk/cpp/CMakeLists.txt"),
                       },
                       "source_sha256": {
                           "sdk/cpp/tests/fault/etcd_unlock_ack_integration.py": file_sha256(Path(__file__).resolve()),
                           "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": file_sha256(PROXY_SOURCE / "main.go"),
                           "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": file_sha256(PROXY_SOURCE / "go.mod"),
                           "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": file_sha256(PROXY_SOURCE / "go.sum"),
                       },
                       "fault": "proxy consumed successful etcd cleanup responses before replacing selected acknowledgements with gRPC Unavailable",
                       "drop_revoke_ack": args.drop_revoke_ack})
    except Exception as error:
        result.update({"status": "FAIL", "error": str(error)})
    finally:
        if worker is not None:
            worker.close()
        for name in reversed(created_containers):
            run(["docker", "rm", "-f", name], check=False)
        if created_network:
            run(["docker", "network", "rm", network], check=False)
        result["cleanup"] = "PASS: worker, proxy, etcd container, and internal Docker network removed; no published ports or volumes"

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
