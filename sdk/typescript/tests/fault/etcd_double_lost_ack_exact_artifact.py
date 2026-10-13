#!/usr/bin/env python3
"""Run exact current TypeScript artifacts through both etcd cleanup ACK-loss paths."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SDK = ROOT / "sdk/typescript"
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.2@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd"
GO_IMAGE = "golang:1.26.8-bookworm@sha256:345775a9b624e690c1c0cd0755149bc8353e221d1c282596564e6d025fd294a9"
NODE_IMAGE = "node:22-bookworm-slim@sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c"
GO_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"
GO_CACHE = ROOT / ".tools/gomodcache"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"
LIVE_TEST = SDK / "tests/fault/etcd_double_lost_ack_live.mjs"
REPORT = SDK / "validation/etcd-unlock-ack-current-source-docker-20261010.json"


def run(command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}")
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def image_id(image: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()


def finish(result: dict, containers: list[str], network_created: bool, network: str) -> int:
    for container in reversed(containers):
        run(["docker", "rm", "-f", container], check=False)
    if network_created:
        run(["docker", "network", "rm", network], check=False)
    result["cleanup"] = "PASS: disposable npm/Node, etcd, proxy containers and internal network removed; no published ports or persistent volumes"
    result["sha256"] = {
        "sdk/typescript/packages/etcd/src/etcd.ts": sha256(SDK / "packages/etcd/src/etcd.ts"),
        "sdk/typescript/packages/etcd/package.json": sha256(SDK / "packages/etcd/package.json"),
        "sdk/typescript/package-lock.json": sha256(SDK / "package-lock.json"),
        "sdk/typescript/tests/fault/etcd_double_lost_ack_live.mjs": sha256(LIVE_TEST),
        "sdk/typescript/tests/fault/etcd_double_lost_ack_exact_artifact.py": sha256(Path(__file__).resolve()),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": sha256(GO_SOURCE / "main.go"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": sha256(GO_SOURCE / "go.mod"),
        "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": sha256(GO_SOURCE / "go.sum"),
        ".tools/etcd-unlock-ack-proxy": sha256(PROXY_BINARY),
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


def main() -> int:
    lock = json.loads((SDK / "package-lock.json").read_text(encoding="utf-8"))
    hosts = sorted({
        __import__("urllib.parse", fromlist=["urlparse"]).urlparse(pkg.get("resolved", "")).netloc
        for pkg in lock["packages"].values() if pkg.get("resolved", "").startswith("https://")
    })
    if hosts != ["registry.npmjs.org"]:
        raise RuntimeError(f"unexpected package-lock dependency host(s): {hosts}")
    if not GO_CACHE.is_dir():
        raise FileNotFoundError("offline Go module cache is missing")

    suffix = uuid.uuid4().hex[:12]
    network, etcd = f"dilock-ts-exact-etcd-{suffix}", f"dilock-ts-etcd-{suffix}"
    proxies = [f"dilock-ts-proxy-{index}-{suffix}" for index in range(2)]
    node = f"dilock-ts-npm-{suffix}"
    namespaces = [f"m6-ts-unlock-ack-{index}-{suffix}" for index in range(2)]
    epoch = f"local-ts-unlock-ack-{suffix}"
    cases = [
        {"name": "revoke-acknowledged", "drop_revoke_ack": False, "expected": "already-not-owned"},
        {"name": "revoke-ack-lost", "drop_revoke_ack": True, "expected": "release_unknown"},
    ]
    containers: list[str] = []
    network_created = False
    result: dict = {"status": "BLOCKED", "checks": {}, "lockfile_dependency_hosts": hosts}
    try:
        built_proxy = run([
            "docker", "run", "--rm", "--pull=never", "--network", "none",
            "--mount", f"type=bind,source={GO_SOURCE},target=/src,readonly",
            "--mount", f"type=bind,source={GO_CACHE},target=/go/pkg/mod,readonly",
            "--mount", f"type=bind,source={ROOT / '.tools'},target=/out",
            "--tmpfs", "/root/.cache/go-build", "--workdir", "/src",
            "-e", "GOPROXY=off", "-e", "GOSUMDB=off", GO_IMAGE,
            "/bin/sh", "-c", "test -z \"$(gofmt -d main.go)\" && go vet -mod=readonly ./... && go test -mod=readonly ./... && go build -mod=readonly -o /out/etcd-unlock-ack-proxy .",
        ], check=False)
        result["checks"]["proxy_docker_build"] = {"status": "PASS" if built_proxy.returncode == 0 else "FAIL",
                                                   "stdout": built_proxy.stdout[-3000:], "stderr": built_proxy.stderr[-3000:]}
        if built_proxy.returncode:
            result["status"] = "FAIL"
            raise RuntimeError("offline Go proxy Docker checks failed")

        run(["docker", "network", "create", "--internal", network])
        network_created = True
        # This container is the only egress-enabled step. npm cache and all build output live in its disposable FS.
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", "bridge", "--name", node,
             "--mount", f"type=bind,source={SDK},target=/repo,readonly",
             "--mount", f"type=bind,source={ROOT / 'spec'},target=/repo-spec,readonly", NODE_IMAGE,
             "/bin/sh", "-c", "exec sleep infinity"])
        containers.append(node)
        copied = run(["docker", "exec", node, "/bin/sh", "-c",
                      "mkdir -p /tmp/sdk /spec && tar -C /repo --exclude='./node_modules' --exclude='./target' --exclude='*/dist' -cf - . | tar -C /tmp/sdk -xf - && cp -a /repo-spec/. /spec/"], check=False)
        result["checks"]["source_copy"] = {"status": "PASS" if copied.returncode == 0 else "FAIL", "stderr": copied.stderr[-2000:]}
        if copied.returncode:
            result["status"] = "FAIL"
            raise RuntimeError("could not make disposable source copy")

        npm = run(["docker", "exec", "-w", "/tmp/sdk", "-e", "npm_config_cache=/tmp/npm-cache",
                   "-e", "npm_config_registry=https://registry.npmjs.org/", "-e", "npm_config_audit=false",
                   "-e", "npm_config_fund=false", node, "npm", "ci", "--strict-peer-deps", "--ignore-scripts", "--no-audit", "--no-fund"], check=False)
        result["checks"]["npm_ci_exact_lock"] = {
            "status": "PASS" if npm.returncode == 0 else "BLOCKED", "exit_code": npm.returncode,
            "command": "npm ci --strict-peer-deps --ignore-scripts --no-audit --no-fund",
            "registry": "https://registry.npmjs.org/", "integrity_enforced_by_package_lock": True,
            "stdout": npm.stdout[-6000:], "stderr": npm.stderr[-6000:],
        }
        if npm.returncode:
            result["status"] = "BLOCKED"
            result["blocked_reason"] = (npm.stdout + "\n" + npm.stderr)[-5000:]
            return finish(result, containers, network_created, network)

        # Drop the public bridge before any backend/runtime operation.
        run(["docker", "network", "connect", network, node])
        run(["docker", "network", "disconnect", "bridge", node])
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", etcd,
             ETCD_IMAGE, "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
             "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", f"http://{etcd}:2379",
             "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", f"http://{etcd}:2380",
             "--initial-cluster", f"node1=http://{etcd}:2380"])
        containers.append(etcd)
        for index, proxy in enumerate(proxies):
            command = ["docker", "run", "-d", "--rm", "--pull=never", "--network", network, "--name", proxy,
                       "-e", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379"]
            if cases[index]["drop_revoke_ack"]:
                command.extend(("-e", "DILOCK_DROP_REVOKE_ACK=1"))
            command.extend(("--mount", f"type=bind,source={PROXY_BINARY},target=/opt/proxy,readonly", GO_IMAGE,
                            "/bin/sh", "-c", "cp /opt/proxy /tmp/proxy && chmod 0755 /tmp/proxy && exec /tmp/proxy"))
            run(command)
            containers.append(proxy)
        deadline = time.monotonic() + 20
        while True:
            health = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379", "endpoint", "health"], check=False)
            if health.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"etcd startup timed out: {health.stdout} {health.stderr}")
            time.sleep(0.25)
        metadata = json.dumps({"protocol_major": 1, "identity_scheme": "sha256-framed-utf8-v1",
                              "profile": "etcd-native-lock-v1", "coordination_epoch": epoch,
                              "fixed_lease_ms": None}, separators=(",", ":"))
        for namespace in namespaces:
            run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379", "put",
                 f"/dilock/namespaces/{namespace}/meta", metadata])

        build = run(["docker", "exec", "-w", "/tmp/sdk", "-e", "npm_config_offline=true", node, "npm", "run", "build"], check=False)
        result["checks"]["linux_typescript_build"] = {"status": "PASS" if build.returncode == 0 else "FAIL",
                                                       "stdout": build.stdout[-6000:], "stderr": build.stderr[-3000:]}
        if build.returncode:
            result["status"] = "FAIL"
            raise RuntimeError("Linux TypeScript build failed after npm ci")
        tests = run(["docker", "exec", "-w", "/tmp/sdk", "-e", "npm_config_offline=true", node, "npm", "test"], check=False)
        result["checks"]["workspace_tests"] = {"status": "PASS" if tests.returncode == 0 else "FAIL",
                                                "stdout": tests.stdout[-7000:], "stderr": tests.stderr[-3000:]}
        if tests.returncode:
            result["status"] = "FAIL"
            raise RuntimeError("exact-lockfile workspace tests failed")

        pack_script = (
            "mkdir -p /tmp/artifacts /tmp/consumer/node_modules && "
            "cp -a /tmp/sdk/node_modules/. /tmp/consumer/node_modules/ && "
            "rm -rf /tmp/consumer/node_modules/@dilock && "
            "mkdir -p /tmp/consumer/node_modules/@dilock/core /tmp/consumer/node_modules/@dilock/etcd && "
            "npm pack --ignore-scripts --workspace=@dilock/core --pack-destination /tmp/artifacts >/dev/null && "
            "npm pack --ignore-scripts --workspace=@dilock/etcd --pack-destination /tmp/artifacts >/dev/null && "
            "tar -xzf /tmp/artifacts/dilock-core-0.1.0.tgz -C /tmp/consumer/node_modules/@dilock/core --strip-components=1 && "
            "tar -xzf /tmp/artifacts/dilock-etcd-0.1.0.tgz -C /tmp/consumer/node_modules/@dilock/etcd --strip-components=1"
        )
        pack = run(["docker", "exec", "-w", "/tmp/sdk", node, "/bin/sh", "-c", pack_script], check=False)
        result["checks"]["exact_artifact_consumer_install"] = {"status": "PASS" if pack.returncode == 0 else "FAIL",
                                                               "method": "extract exact local core/etcd npm archives; copy third-party runtime closure from exact npm ci install",
                                                               "stdout": pack.stdout[-5000:], "stderr": pack.stderr[-3000:]}
        if pack.returncode:
            result["status"] = "FAIL"
            raise RuntimeError("local exact npm artifact consumer install failed offline")
        run(["docker", "exec", node, "cp", "/tmp/sdk/tests/fault/etcd_double_lost_ack_live.mjs", "/tmp/consumer/"])
        live_cases = []
        leftovers_by_case = {}
        for index, case in enumerate(cases):
            proxy = proxies[index]
            command = ["docker", "exec", "-w", "/tmp/consumer",
                       "-e", f"DILOCK_ETCD_ENDPOINT=http://{proxy}:2379",
                       "-e", f"DILOCK_ETCD_NAMESPACE={namespaces[index]}",
                       "-e", f"DILOCK_ETCD_EPOCH={epoch}",
                       "-e", f"DILOCK_PROXY_STATS_URL=http://{proxy}:2379/__stats"]
            if case["drop_revoke_ack"]:
                command.extend(("-e", "DILOCK_DROP_REVOKE_ACK=1"))
            command.extend((node, "node", "etcd_double_lost_ack_live.mjs"))
            live = run(command, check=False)
            if live.returncode:
                result["status"] = "FAIL"
                raise RuntimeError(f"{case['name']} exact artifact live test failed:\n{live.stdout}\n{live.stderr}")
            outcome = json.loads(next(line for line in reversed(live.stdout.splitlines()) if line.startswith("{")))
            if outcome.get("first_release") != case["expected"] or outcome.get("status") != "PASS":
                result["status"] = "FAIL"
                raise RuntimeError(f"{case['name']} returned unexpected outcome: {outcome}")
            scan = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl", "--endpoints=http://127.0.0.1:2379",
                        "get", "--prefix", "--keys-only", "/dilock/locks/"])
            leftovers = [line for line in scan.stdout.splitlines() if line.startswith("/dilock/locks/")]
            if leftovers:
                result["status"] = "FAIL"
                raise RuntimeError(f"{case['name']} left lock keys after successor release: {leftovers}")
            live_cases.append({"name": case["name"], "expected_release": case["expected"], "result": outcome})
            leftovers_by_case[case["name"]] = leftovers

        hashes = run(["docker", "exec", node, "/bin/sh", "-c",
                      "sha256sum /tmp/artifacts/dilock-core-0.1.0.tgz /tmp/artifacts/dilock-etcd-0.1.0.tgz /tmp/consumer/node_modules/@grpc/grpc-js/package.json /tmp/consumer/node_modules/typescript/package.json"])
        result.update({"status": "PASS", "sdk": "TypeScript", "backend": "etcd", "etcd_version": "3.7.2",
                       "scope": "Current source rebuilt from this checkout with lockfile-exact dependencies, exact local Core/etcd npm archives, two ACK-loss cases",
                       "npm_ci": "PASS exact package-lock; npm cache lived only in disposable Node container",
                       "runtime_network_after_install": "public bridge disconnected; internal test network only",
                       "live_cases": live_cases, "lock_keys_after_successor": leftovers_by_case,
                       "artifact_and_dependency_sha256": hashes.stdout.splitlines(),
                       "images": {image: image_id(image) for image in (ETCD_IMAGE, GO_IMAGE, NODE_IMAGE)},
                       "proxy_sha256": sha256(PROXY_BINARY)})
    except Exception as error:
        if result.get("status") != "FAIL":
            result["status"] = "FAIL"
        result["error"] = str(error)
    return finish(result, containers, network_created, network)


if __name__ == "__main__":
    raise SystemExit(main())
