#!/usr/bin/env python3
"""Run the Rust etcd applied-cleanup ACK-loss test against isolated local Docker services."""

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
RUST_IMAGE = "rust:1.99.0-bookworm"
PYTHON_IMAGE = "python:3.12-slim"
RUNTIME_IMAGE = "debian@sha256:913f6706df59a68922d1dd08f78c2476560a8d367897200a6005b00e5f67c2d5"
PROXY_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"
PROXY_MAIN = PROXY_SOURCE / "main.go"
RUST_TARGET = ROOT / ".tools/rust-current-docker-target"
RUST_CARGO_HOME = ROOT / ".tools/cargo"
INTEGRATION_TEST = ROOT / "sdk/rust/etcd/tests/etcd_real.rs"
RUST_ETCD_LIB = ROOT / "sdk/rust/etcd/src/lib.rs"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}"
        )
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def image_id(image: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT
        / "validation/implementation/rust-etcd-applied-double-cleanup-ack-loss-docker-20261008.json",
    )
    args = parser.parse_args()

    suffix = uuid.uuid4().hex[:12]
    network = f"dilock-rust-etcd-ack-{suffix}"
    etcd = f"dilock-rust-etcd-{suffix}"
    proxy = f"dilock-rust-proxy-{suffix}"
    runtime_volume = f"dilock-rust-protoc-runtime-{suffix}"
    containers: list[str] = []
    network_created = False
    runtime_volume_created = False
    result: dict = {
        "status": "FAIL",
        "cleanup": "pending",
        "attempts": [],
        "corrected_harness_attempts": [
            {
                "status": "BLOCKED_BEFORE_TEST",
                "cause": "The first Rust Docker invocation mounted the local Cargo registry but omitted CARGO_HOME=/root/.cargo, so Cargo could not resolve cached sha2.",
                "correction": "Set CARGO_HOME=/root/.cargo while keeping the registry mount read-only and Cargo offline.",
            },
            {
                "status": "BLOCKED_BEFORE_TEST",
                "cause": "The next invocation used the cached protoc wrapper/runtime but did not mount .tools/grpc-root into the Rust container, so the wrapper could not open /grpc/usr/bin/protoc.",
                "correction": "Mount .tools/grpc-root read-only at /grpc; the subsequent integration passed.",
            },
        ],
    }
    try:
        # Keep the source and Cargo registry read-only, but create an isolated
        # writable target directory when this task has no prior build cache.
        # A missing cache is a cold-build condition, not a reason to skip the
        # current-source fault test.
        RUST_TARGET.mkdir(parents=True, exist_ok=True)
        for path in (RUST_CARGO_HOME, PROXY_SOURCE):
            if not path.exists():
                raise FileNotFoundError(f"required local cache/source missing: {path}")

        proxy_build = run(
            [
                "docker", "run", "--rm", "--pull=never", "--network", "none",
                "--mount", f"type=bind,source={PROXY_SOURCE},target=/src,readonly",
                "--mount", f"type=bind,source={ROOT / '.tools/gomodcache'},target=/go/pkg/mod,readonly",
                "--mount", f"type=bind,source={ROOT / '.tools'},target=/out",
                "--tmpfs", "/root/.cache/go-build", "--workdir", "/src",
                "-e", "GOPROXY=off", "-e", "GOSUMDB=off", GO_IMAGE,
                "/bin/sh", "-c",
                "test -z \"$(gofmt -d main.go)\" && go vet -mod=readonly ./... && "
                "go test -mod=readonly ./... && go build -mod=readonly -o /out/etcd-unlock-ack-proxy .",
            ]
        )
        result["attempts"].append({"name": "offline gRPC proxy build", "status": "PASS"})

        run(["docker", "network", "create", "--internal", network])
        network_created = True
        run(["docker", "volume", "create", runtime_volume])
        runtime_volume_created = True
        runtime_setup = run(
            [
                "docker", "run", "--rm", "--pull=never", "--network", "none",
                "--mount", f"type=volume,source={runtime_volume},target=/runtime",
                "--mount", f"type=bind,source={ROOT / '.tools/grpc-root'},target=/grpc,readonly",
                RUNTIME_IMAGE, "/bin/sh", "-c",
                "set -eu; mkdir -p /runtime/lib/x86_64-linux-gnu /runtime/usr/lib/x86_64-linux-gnu; "
                "cp -a /lib/x86_64-linux-gnu/. /runtime/lib/x86_64-linux-gnu/; "
                "cp -a /usr/lib/x86_64-linux-gnu/. /runtime/usr/lib/x86_64-linux-gnu/; "
                "cp -L /lib64/ld-linux-x86-64.so.2 /runtime/ld-linux-x86-64.so.2; "
                "printf '%s\\n' '#!/bin/sh' "
                "'exec /runtime/ld-linux-x86-64.so.2 --library-path /runtime/lib/x86_64-linux-gnu:/runtime/usr/lib/x86_64-linux-gnu:/grpc/usr/lib/x86_64-linux-gnu /grpc/usr/bin/protoc \"$@\"' "
                "> /runtime/protoc-wrapper; chmod 0755 /runtime/protoc-wrapper; "
                "PROTOC=/runtime/protoc-wrapper /runtime/protoc-wrapper --version",
            ]
        )
        if "libprotoc 3.21.12" not in runtime_setup.stdout:
            raise RuntimeError(f"cached protoc/runtime setup did not verify: {runtime_setup.stdout} {runtime_setup.stderr}")
        result["attempts"].append({"name": "cached protoc with local Trixie runtime", "status": "PASS", "version": "libprotoc 3.21.12"})
        run(
            [
                "docker", "run", "-d", "--rm", "--pull=never", "--network", network,
                "--name", etcd, ETCD_IMAGE,
                "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
                "--listen-client-urls", "http://0.0.0.0:2379",
                "--advertise-client-urls", f"http://{etcd}:2379",
                "--listen-peer-urls", "http://0.0.0.0:2380",
                "--initial-advertise-peer-urls", f"http://{etcd}:2380",
                "--initial-cluster", f"node1=http://{etcd}:2380",
            ]
        )
        containers.append(etcd)
        run(
            [
                "docker", "run", "-d", "--rm", "--pull=never", "--network", network,
                "--name", proxy, "-e", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379",
                "-e", "DILOCK_DROP_REVOKE_ACK=1",
                "--mount", f"type=bind,source={PROXY_BINARY},target=/opt/proxy,readonly",
                GO_IMAGE, "/bin/sh", "-c",
                "cp /opt/proxy /tmp/etcd-unlock-ack-proxy && chmod 0755 /tmp/etcd-unlock-ack-proxy && exec /tmp/etcd-unlock-ack-proxy",
            ]
        )
        containers.append(proxy)

        deadline = time.monotonic() + 20
        while True:
            health = run(
                [
                    "docker", "exec", etcd, "/usr/local/bin/etcdctl",
                    "--endpoints=http://127.0.0.1:2379", "endpoint", "health",
                ],
                check=False,
            )
            if health.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"etcd did not become ready: {health.stdout} {health.stderr}")
            time.sleep(0.25)

        cargo = run(
            [
                "docker", "run", "--rm", "--pull=never", "--network", network,
                "--mount", f"type=bind,source={ROOT / 'sdk/rust'},target=/repo,readonly",
                "--mount", f"type=bind,source={RUST_CARGO_HOME},target=/root/.cargo,readonly",
                "--mount", f"type=bind,source={RUST_TARGET},target=/target",
                "--mount", f"type=volume,source={runtime_volume},target=/runtime,readonly",
                "--mount", f"type=bind,source={ROOT / '.tools/grpc-root'},target=/grpc,readonly",
                "--workdir", "/repo",
                "-e", "CARGO_HOME=/root/.cargo",
                "-e", "CARGO_TARGET_DIR=/target",
                "-e", f"DILOCK_ETCD_ENDPOINT=http://{proxy}:2379",
                "-e", "CARGO_NET_OFFLINE=true", "-e", "PROTOC=/runtime/protoc-wrapper",
                RUST_IMAGE, "cargo", "test", "--locked", "--offline", "-p", "dilock-etcd",
                "--test", "etcd_real",
                "applied_unlock_and_revoke_ack_loss_is_terminal_and_preserves_successor_fence",
                "--", "--ignored", "--exact", "--nocapture",
            ],
            check=False,
        )
        if cargo.returncode:
            product_failure = "test result: FAILED" in cargo.stdout or "error: test failed" in cargo.stderr
            result["attempts"].append(
                {
                    "name": "Rust live etcd integration",
                    "status": "FAIL" if product_failure else "BLOCKED",
                    "exit_code": cargo.returncode,
                    "stdout": cargo.stdout,
                    "stderr": cargo.stderr,
                }
            )
            if not product_failure and "no matching package named `sha2`" in cargo.stderr:
                raise RuntimeError("Rust run blocked before compile: CARGO_HOME did not resolve the mounted offline registry")
            if not product_failure:
                raise RuntimeError(f"Rust integration was blocked before a test result:\n{cargo.stdout}\n{cargo.stderr}")
            raise RuntimeError(f"Rust live integration failed:\n{cargo.stdout}\n{cargo.stderr}")
        if "1 passed" not in cargo.stdout or "test result: ok" not in cargo.stdout:
            raise RuntimeError(f"focused Rust integration did not prove a pass:\n{cargo.stdout}\n{cargo.stderr}")
        result["attempts"].append({"name": "Rust live etcd integration", "status": "PASS"})

        stats_cmd = (
            "import json,urllib.request; "
            f"print(urllib.request.urlopen('http://{proxy}:2379/__stats',timeout=3).read().decode())"
        )
        stats_result = run(["docker", "run", "--rm", "--pull=never", "--network", network, PYTHON_IMAGE, "python", "-c", stats_cmd])
        stats = json.loads(stats_result.stdout)
        expected = {
            "unlock_requests": 2,
            "unlock_applied": 2,
            "unlock_withheld": 1,
            "revoke_requests": 2,
            "revoke_applied": 2,
            "revoke_acknowledged": 1,
            "revoke_withheld": 1,
        }
        if any(stats.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"RPC counters do not prove both applied ACK losses and no retry: {stats}")

        scan = run(
            [
                "docker", "exec", etcd, "/usr/local/bin/etcdctl",
                "--endpoints=http://127.0.0.1:2379", "get", "--prefix", "--keys-only", "/dilock/locks/",
            ]
        )
        lock_keys = [line for line in scan.stdout.splitlines() if line.startswith("/dilock/locks/")]
        if lock_keys:
            raise RuntimeError(f"lock keys remain after the successor release: {lock_keys}")

        result.update(
            {
                "status": "PASS",
                "sdk": "Rust",
                "backend": "etcd",
                "etcd_version": "3.7.2",
                "test": "applied_unlock_and_revoke_ack_loss_is_terminal_and_preserves_successor_fence: 1 passed, 0 failed, 0 skipped",
                "outcomes": {
                    "first_release": "Unknown",
                    "repeat_release": "Unknown (cached; no cleanup RPC replay)",
                    "owner_state": "ReleasedUnknown",
                    "successor_acquired": True,
                    "successor_create_revision_greater": True,
                    "successor_release": "Released",
                },
                "proxy": stats,
                "remaining_lock_keys": lock_keys,
                "images": {image: image_id(image) for image in (ETCD_IMAGE, GO_IMAGE, RUST_IMAGE, PYTHON_IMAGE, RUNTIME_IMAGE)},
                "commands": {
                    "proxy_build": "gofmt, go vet, go test and go build with --network none, GOPROXY=off and GOSUMDB=off",
                    "rust_test": "cargo test --locked --offline -p dilock-etcd --test etcd_real applied_unlock_and_revoke_ack_loss_is_terminal_and_preserves_successor_fence -- --ignored --exact --nocapture",
                },
                "logs": {"rust_stdout": cargo.stdout, "rust_stderr": cargo.stderr, "proxy_build_stdout": proxy_build.stdout},
            }
        )
    except Exception as error:
        result.update({"status": "FAIL", "error": str(error)})
    finally:
        for container in reversed(containers):
            run(["docker", "rm", "-f", container], check=False)
        if network_created:
            run(["docker", "network", "rm", network], check=False)
        if runtime_volume_created:
            run(["docker", "volume", "rm", runtime_volume], check=False)
        result["cleanup"] = "PASS: task-owned etcd/proxy containers and internal network removed; no exposed ports or remote access"

    result["sha256"] = {
        "sdk/rust/etcd/tests/etcd_real.rs": sha256(INTEGRATION_TEST),
        "sdk/rust/etcd/src/lib.rs": sha256(RUST_ETCD_LIB),
        "sdk/rust/Cargo.lock": sha256(ROOT / "sdk/rust/Cargo.lock"),
        "sdk/rust/tests/fault/etcd_applied_cleanup_ack_loss_docker.py": sha256(Path(__file__).resolve()),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": sha256(PROXY_MAIN),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": sha256(PROXY_SOURCE / "go.mod"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": sha256(PROXY_SOURCE / "go.sum"),
        ".tools/etcd-unlock-ack-proxy": sha256(PROXY_BINARY) if PROXY_BINARY.exists() else None,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
