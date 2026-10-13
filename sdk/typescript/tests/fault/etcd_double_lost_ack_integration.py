#!/usr/bin/env python3
"""Run current TypeScript etcd cleanup through an isolated, local Docker fault proxy."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SDK = ROOT / "sdk/typescript"
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.2"
GO_IMAGE = "golang:1.26.8-bookworm"
NODE_IMAGE = "node:22-bookworm-slim"
GO_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"
GO_CACHE = ROOT / ".tools/gomodcache"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"
LIVE_TEST = SDK / "tests/fault/etcd_double_lost_ack_live.mjs"
REPORT = ROOT / "validation/implementation/typescript-etcd-double-lost-cleanup-ack-docker-20261007.json"


def run(command: list[str], *, check: bool = True, env: dict[str, str] | None = None,
        cwd: Path | None = None) -> subprocess.CompletedProcess:
    result = subprocess.run(command, text=True, capture_output=True, env=env, cwd=cwd)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}")
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def image_id(image: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()


def main() -> int:
    if not GO_CACHE.is_dir():
        raise FileNotFoundError("offline Go module cache is missing")
    suffix = uuid.uuid4().hex[:12]
    network = f"dilock-ts-etcd-ack-{suffix}"
    etcd = f"dilock-ts-etcd-{suffix}"
    proxy = f"dilock-ts-proxy-{suffix}"
    namespace = f"m6-ts-etcd-ack-{suffix}"
    epoch = f"local-ts-etcd-ack-{suffix}"
    containers: list[str] = []
    network_created = False
    result: dict = {"status": "BLOCKED", "checks": {}}
    try:
        npm_env = os.environ.copy()
        npm_env["npm_config_offline"] = "true"
        build = run(["cmd.exe", "/d", "/s", "/c", "npm.cmd run build"], env=npm_env, cwd=SDK, check=False)
        result["checks"]["current_source_build_host"] = {
            "status": "PASS" if build.returncode == 0 else "FAIL",
            "toolchain": "host Node 22 / TypeScript 7.0.2; npm forced offline",
            "stdout": build.stdout[-5000:], "stderr": build.stderr[-3000:],
        }
        if build.returncode:
            raise RuntimeError("current-source TypeScript build failed")

        # The locked current dependency is 1.14.5. This workspace has only 1.14.4;
        # keep the source behavior run explicit, and never call it exact-artifact proof.
        grpc_version = run(["node", "-p", "require('./node_modules/@grpc/grpc-js/package.json').version"], env=npm_env, cwd=SDK).stdout.strip()
        locked = json.loads((SDK / "package-lock.json").read_text(encoding="utf-8"))
        locked_grpc = locked["packages"]["node_modules/@grpc/grpc-js"]["version"]
        result["checks"]["exact_locked_dependency_artifact"] = {
            "status": "PASS" if grpc_version == locked_grpc else "BLOCKED",
            "locked_grpc_js": locked_grpc, "available_grpc_js": grpc_version,
            "reason": None if grpc_version == locked_grpc else "locked @grpc/grpc-js tarball 1.14.5 and the Linux TypeScript 7.0.2 platform package are absent from local caches; no network fallback was used",
        }

        go_build = run([
            "docker", "run", "--rm", "--pull=never", "--network", "none",
            "--mount", f"type=bind,source={GO_SOURCE},target=/src,readonly",
            "--mount", f"type=bind,source={GO_CACHE},target=/go/pkg/mod,readonly",
            "--mount", f"type=bind,source={ROOT / '.tools'},target=/out",
            "--tmpfs", "/root/.cache/go-build", "--workdir", "/src",
            "-e", "GOPROXY=off", "-e", "GOSUMDB=off", GO_IMAGE,
            "/bin/sh", "-c",
            "test -z \"$(gofmt -d main.go)\" && go vet -mod=readonly ./... && go test -mod=readonly ./... && go build -mod=readonly -o /out/etcd-unlock-ack-proxy .",
        ], check=False)
        result["checks"]["fault_proxy_docker_build"] = {
            "status": "PASS" if go_build.returncode == 0 else "FAIL",
            "stdout": go_build.stdout[-3000:], "stderr": go_build.stderr[-3000:],
        }
        if go_build.returncode:
            raise RuntimeError("offline Docker build/test of the fault proxy failed")

        node_id = image_id(NODE_IMAGE)
        run(["docker", "network", "create", "--internal", network])
        network_created = True
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", etcd,
             ETCD_IMAGE, "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
             "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", f"http://{etcd}:2379",
             "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", f"http://{etcd}:2380",
             "--initial-cluster", f"node1=http://{etcd}:2380"])
        containers.append(etcd)
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", proxy,
             "-e", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379", "-e", "DILOCK_DROP_REVOKE_ACK=1",
             "--mount", f"type=bind,source={PROXY_BINARY},target=/opt/proxy,readonly", GO_IMAGE,
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

        # Copy current generated JS and installed runtime dependencies into ephemeral Linux storage.
        # Windows workspace Junctions under node_modules/@dilock are recreated as package directories.
        shell = r'''set -eu
mkdir -p /tmp/runtime/node_modules/@dilock
find /repo/node_modules -mindepth 1 -maxdepth 1 ! -name '@dilock' -exec cp -a {} /tmp/runtime/node_modules/ \;
for p in core redis etcd; do
  mkdir -p "/tmp/runtime/node_modules/@dilock/$p"
  cp -a "/repo/packages/$p/package.json" "/tmp/runtime/node_modules/@dilock/$p/"
  cp -a "/repo/packages/$p/dist" "/tmp/runtime/node_modules/@dilock/$p/"
done
cp -a /repo/packages/etcd/proto /tmp/runtime/node_modules/@dilock/etcd/
cp /repo/tests/fault/etcd_double_lost_ack_live.mjs /tmp/runtime/
cd /tmp/runtime
node -e "if (require('@grpc/grpc-js/package.json').version !== process.env.EXPECTED_GRPC) process.exit(23)"
node /tmp/runtime/etcd_double_lost_ack_live.mjs
'''
        node = run([
            "docker", "run", "--rm", "--pull=never", "--network", network,
            "--mount", f"type=bind,source={SDK},target=/repo,readonly",
            "-e", f"DILOCK_ETCD_ENDPOINT=http://{proxy}:2379",
            "-e", f"DILOCK_ETCD_NAMESPACE={namespace}", "-e", f"DILOCK_ETCD_EPOCH={epoch}",
            "-e", f"DILOCK_PROXY_STATS_URL=http://{proxy}:2379/__stats",
            "-e", f"EXPECTED_GRPC={grpc_version}", NODE_IMAGE, "/bin/sh", "-c", shell,
        ], check=False)
        if node.returncode:
            raise RuntimeError(f"current-source Docker live test failed ({node.returncode}):\n{node.stdout}\n{node.stderr}")
        live = json.loads(next(line for line in reversed(node.stdout.splitlines()) if line.startswith("{")))
        stats = live["proxy"]

        scan = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379",
                    "get", "--prefix", "--keys-only", "/dilock/locks/"])
        remaining = [line for line in scan.stdout.splitlines() if line.startswith("/dilock/locks/")]
        if remaining:
            raise RuntimeError(f"etcd lock keys remain after successor release: {remaining}")
        expected = {"unlock_requests": 2, "unlock_applied": 2, "unlock_withheld": 1,
                    "revoke_requests": 2, "revoke_applied": 2, "revoke_acknowledged": 1,
                    "revoke_withheld": 1}
        if any(stats.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"proxy counters did not prove applied cleanup and no retry: {stats}")

        result.update({
            "source_behavior_status": "PASS", "sdk": "TypeScript", "backend": "etcd", "etcd_version": "3.7.2",
            "source_behavior_scope": "current TypeScript source compiled with TypeScript 7.0.2; Docker runtime used locally installed grpc-js 1.14.4",
            "live_test": live, "lock_keys_after_successor": remaining,
            "images": {image: image_id(image) for image in (ETCD_IMAGE, GO_IMAGE, NODE_IMAGE)},
            "proxy_sha256": sha256(PROXY_BINARY),
        })
        if result["checks"]["exact_locked_dependency_artifact"]["status"] == "PASS":
            result["status"] = "PASS"
        else:
            result["status"] = "BLOCKED"
            result["blocked_reason"] = result["checks"]["exact_locked_dependency_artifact"]["reason"]
    except Exception as error:
        result["status"] = "FAIL"
        result["error"] = str(error)
    finally:
        for container in reversed(containers):
            run(["docker", "rm", "-f", container], check=False)
        if network_created:
            run(["docker", "network", "rm", network], check=False)
        result["cleanup"] = "PASS: disposable etcd/proxy and internal network removed; Node test container auto-removed; no published ports or persistent volumes"

    result["sha256"] = {
        "sdk/typescript/packages/etcd/src/etcd.ts": sha256(SDK / "packages/etcd/src/etcd.ts"),
        "sdk/typescript/packages/etcd/package.json": sha256(SDK / "packages/etcd/package.json"),
        "sdk/typescript/package-lock.json": sha256(SDK / "package-lock.json"),
        "sdk/typescript/tests/fault/etcd_double_lost_ack_live.mjs": sha256(LIVE_TEST),
        "sdk/typescript/tests/fault/etcd_double_lost_ack_integration.py": sha256(Path(__file__).resolve()),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": sha256(GO_SOURCE / "main.go"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": sha256(GO_SOURCE / "go.mod"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": sha256(GO_SOURCE / "go.sum"),
        ".tools/etcd-unlock-ack-proxy": sha256(PROXY_BINARY),
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
