#!/usr/bin/env python3
"""Run the Java etcd double-cleanup-ACK fault against isolated local Docker services."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.2"
GO_IMAGE = "golang:1.26.8-bookworm"
JAVA_IMAGE = "maven:3.9.9-eclipse-temurin-21"
PYTHON_IMAGE = "python:3.12-slim"
PROXY_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"
MAVEN_CACHE = ROOT / ".tools/m2repo"
PROXY_GO_MOD_CACHE = ROOT / ".tools/gomodcache"
PROXY_MAIN = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go"
JAVA_TEST = ROOT / "sdk/java/etcd/src/test/java/org/dilock/etcd/EtcdAppliedDoubleLostAckLiveTest.java"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}")
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def docker_image_id(image: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=ROOT / "validation/implementation/java-etcd-double-lost-cleanup-ack-docker-20261007.json")
    parser.add_argument("--proxy-binary", type=Path, default=PROXY_BINARY)
    parser.add_argument("--reuse-proxy-binary", action="store_true",
                        help="Use the existing local proxy binary without rebuilding or modifying it.")
    parser.add_argument("--revoke-ack-mode", choices=("withheld", "acknowledged"), default="withheld",
                        help="Whether to withhold the applied LeaseRevoke acknowledgement after the lost Unlock ACK.")
    args = parser.parse_args()
    proxy_binary = args.proxy_binary.resolve()
    proxy_binary.parent.mkdir(parents=True, exist_ok=True)
    if not MAVEN_CACHE.is_dir() or not PROXY_GO_MOD_CACHE.is_dir():
        raise FileNotFoundError("Offline Maven or Go dependency cache is missing")

    suffix = uuid.uuid4().hex[:12]
    network = f"dilock-java-etcd-ack-{suffix}"
    etcd = f"dilock-java-etcd-{suffix}"
    proxy = f"dilock-java-proxy-{suffix}"
    namespace = f"m6-java-etcd-ack-{suffix}"
    epoch = f"local-java-etcd-ack-{suffix}"
    containers: list[str] = []
    network_created = False
    result: dict = {"status": "FAIL", "cleanup": "pending"}
    try:
        # Build the test proxy with cached Go modules unless explicitly reusing
        # an existing local binary. Neither path contacts a registry.
        if args.reuse_proxy_binary:
            if not proxy_binary.is_file():
                raise FileNotFoundError(f"requested reusable proxy binary is absent: {proxy_binary}")
        else:
            run(["docker", "run", "--rm", "--pull=never", "--network", "none",
                 "--mount", f"type=bind,source={PROXY_SOURCE},target=/src,readonly",
                 "--mount", f"type=bind,source={PROXY_GO_MOD_CACHE},target=/go/pkg/mod,readonly",
                 "--mount", f"type=bind,source={proxy_binary.parent},target=/out",
                 "--tmpfs", "/root/.cache/go-build", "--workdir", "/src",
                 "-e", "GOPROXY=off", "-e", "GOSUMDB=off", GO_IMAGE,
                 "/bin/sh", "-c", "test -z \"$(gofmt -d main.go)\" && go vet -mod=readonly ./... && go test -mod=readonly ./... && go build -mod=readonly -o /out/" + proxy_binary.name + " ."])

        run(["docker", "network", "create", "--internal", network])
        network_created = True
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", etcd, ETCD_IMAGE,
             "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
             "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", f"http://{etcd}:2379",
             "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", f"http://{etcd}:2380",
             "--initial-cluster", f"node1=http://{etcd}:2380"])
        containers.append(etcd)
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", proxy,
             "-e", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379",
             "-e", "DILOCK_DROP_REVOKE_ACK=" + ("1" if args.revoke_ack_mode == "withheld" else "0"),
             "--mount", f"type=bind,source={proxy_binary},target=/opt/proxy,readonly", GO_IMAGE,
             "/bin/sh", "-c", "cp /opt/proxy /tmp/etcd-unlock-ack-proxy && chmod 0755 /tmp/etcd-unlock-ack-proxy && exec /tmp/etcd-unlock-ack-proxy"])
        containers.append(proxy)

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

        maven_command = [
            "docker", "run", "--rm", "--pull=never", "--network", network,
            "--mount", f"type=bind,source={ROOT},target=/repo,readonly",
            "--mount", f"type=bind,source={MAVEN_CACHE},target=/m2,readonly",
            "-e", "DILOCK_ETCD_LOST_ACK_PROXY_ENDPOINT=" + f"http://{proxy}:2379",
            "-e", f"DILOCK_ETCD_NAMESPACE={namespace}", "-e", f"DILOCK_ETCD_EPOCH={epoch}",
            JAVA_IMAGE, "/bin/sh", "-c",
            "cp -a /repo/sdk/java /tmp/java && cp -a /m2 /tmp/m2 && "
            "mvn -o -B -ntp -Dmaven.repo.local=/tmp/m2 -f /tmp/java/pom.xml -pl etcd -am "
            "-Dtest=EtcdAppliedDoubleLostAckLiveTest -Dsurefire.failIfNoSpecifiedTests=false test",
        ]
        maven = run(maven_command, check=False)
        if maven.returncode:
            raise RuntimeError(f"Java Maven test failed ({maven.returncode}):\n{maven.stdout}\n{maven.stderr}")
        if "EtcdAppliedDoubleLostAckLiveTest" not in maven.stdout or "BUILD SUCCESS" not in maven.stdout:
            raise RuntimeError(f"Maven output did not prove the focused live test ran:\n{maven.stdout}\n{maven.stderr}")

        stats_code = "import json,urllib.request; print(urllib.request.urlopen('http://" + proxy + ":2379/__stats',timeout=3).read().decode())"
        stats_result = run(["docker", "run", "--rm", "--pull=never", "--network", network, PYTHON_IMAGE, "python", "-c", stats_code])
        stats = json.loads(stats_result.stdout)
        expected = {"unlock_requests": 2, "unlock_applied": 2, "unlock_withheld": 1,
                    "revoke_requests": 2, "revoke_applied": 2,
                    "revoke_acknowledged": 2 if args.revoke_ack_mode == "acknowledged" else 1,
                    "revoke_withheld": 0 if args.revoke_ack_mode == "acknowledged" else 1}
        mismatches = {key: (expected_value, stats.get(key))
                      for key, expected_value in expected.items()
                      if stats.get(key) != expected_value}
        if stats.get("range_requests") != stats.get("range_applied") or mismatches:
            raise RuntimeError(f"RPC counts do not prove one cleanup per lease and complete Range replies: expected={expected}, mismatches={mismatches}, actual={stats}")
        if args.revoke_ack_mode == "withheld" and stats.get("range_requests", 0) < 1:
            raise RuntimeError(f"lost Unlock and Revoke acknowledgements require exact-key Range confirmation: {stats}")

        scan = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379",
                    "get", "--prefix", "--keys-only", "/dilock/locks/"])
        remaining = [line for line in scan.stdout.splitlines() if line.startswith("/dilock/locks/")]
        if remaining:
            raise RuntimeError(f"native lock keys remain after both SDK leases were released: {remaining}")

        maven_summary = [line.strip() for line in maven.stdout.splitlines() if "Tests run:" in line and "EtcdAppliedDoubleLostAckLiveTest" in line]
        result.update({"status": "PASS", "sdk": "Java", "backend": "etcd", "etcd_version": "3.7.2",
                       "maven_summary": maven_summary,
                       "outcome": "ALREADY_NOT_OWNED", "repeat_outcome": "ALREADY_NOT_OWNED",
                       "owner_state": "RELEASED", "revoke_ack_mode": args.revoke_ack_mode,
                       "reason": ("Revoke acknowledgement confirms cleanup after applied Unlock ACK loss"
                                  if args.revoke_ack_mode == "acknowledged"
                                  else "both cleanup ACKs were withheld; exact opaque-key Range confirmed absence"),
                       "proxy": stats, "owner_lock_keys": remaining,
                       "successor_acquired": True, "successor_revision_greater": True,
                       "successor_release": "RELEASED", "namespace": namespace,
                       "images": {image: docker_image_id(image) for image in (ETCD_IMAGE, GO_IMAGE, JAVA_IMAGE, PYTHON_IMAGE)},
                       "proxy_sha256": sha256(proxy_binary)})
    except Exception as error:
        result.update({"status": "FAIL", "error": str(error)})
    finally:
        for container in reversed(containers):
            run(["docker", "rm", "-f", container], check=False)
        if network_created:
            run(["docker", "network", "rm", network], check=False)
        remaining_containers = run(["docker", "ps", "-a", "--format", "{{.Names}}"], check=False)
        remaining_networks = run(["docker", "network", "ls", "--format", "{{.Name}}"], check=False)
        leftover_containers = [name for name in (etcd, proxy) if name in remaining_containers.stdout.splitlines()]
        leftover_networks = [network] if network in remaining_networks.stdout.splitlines() else []
        result["cleanup"] = {
            "status": "PASS" if not leftover_containers and not leftover_networks else "FAIL",
            "details": "ephemeral etcd, proxy and internal network removed; Maven/metrics containers use --rm; no published ports or volumes",
            "remaining_task_containers": leftover_containers,
            "remaining_task_networks": leftover_networks,
        }

    result["sha256"] = {
        "sdk/java/etcd/src/test/java/org/dilock/etcd/EtcdAppliedDoubleLostAckLiveTest.java": sha256(JAVA_TEST),
        "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdProvider.java": sha256(ROOT / "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdProvider.java"),
        "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdLease.java": sha256(ROOT / "sdk/java/etcd/src/main/java/org/dilock/etcd/EtcdLease.java"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": sha256(PROXY_MAIN),
        "sdk/java/tests/fault/etcd_double_lost_ack_integration.py": sha256(Path(__file__).resolve()),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": sha256(PROXY_SOURCE / "go.mod"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": sha256(PROXY_SOURCE / "go.sum"),
        proxy_binary.relative_to(ROOT).as_posix(): sha256(proxy_binary),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" and result["cleanup"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
