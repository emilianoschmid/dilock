"""Run the current Python SDK against etcd 3.7.2 through the shared gRPC fault proxy."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SDK = ROOT / "sdk/python"
PROXY_BINARY = ROOT / ".tools/etcd-unlock-ack-proxy"
PYTHON_IMAGE = "python:3.13-slim@sha256:3dd7cc108ec1493442514f5c2a871af6af0ec31d768ff6e378a93340c3b3db5f"
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.2@sha256:e9afa62b1e914f02db62e5c46a7a140d8cdf026f2ed888cab4e5a142ec4d06fd"
GO_IMAGE = "golang:1.26.8-bookworm@sha256:345775a9b624e690c1c0cd0755149bc8353e221d1c282596564e6d025fd294a9"
PROXY_SOURCE = ROOT / "sdk/cpp/tests/fault/etcd_unlock_ack_proxy"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}"
        )
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def docker_mount(path: Path, target: str, readonly: bool = False) -> str:
    value = f"type=bind,source={path.resolve()},target={target}"
    return value + (",readonly" if readonly else "")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not PROXY_BINARY.is_file():
        raise FileNotFoundError(f"shared etcd fault proxy binary is missing: {PROXY_BINARY}")

    suffix = uuid.uuid4().hex[:10]
    network = f"dilock-py-unlock-ack-{suffix}"
    etcd = f"dilock-py-etcd-{suffix}"
    client = f"dilock-py-client-{suffix}"
    cases = [
        {"name": "revoke-acknowledged", "drop_revoke_ack": False,
         "namespace": f"py-unlock-ack-ok-{suffix}", "expected": "already_not_owned"},
        {"name": "revoke-ack-lost", "drop_revoke_ack": True,
         "namespace": f"py-unlock-ack-unknown-{suffix}", "expected": "release_unknown"},
    ]
    proxies: list[str] = []
    created: list[str] = []
    network_created = False
    result: dict[str, object] = {"status": "FAIL", "cleanup": "pending"}
    try:
        run(["docker", "network", "create", "--internal", network])
        network_created = True
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", network,
             "--name", etcd, ETCD_IMAGE,
             "/usr/local/bin/etcd", "--name", "node1", "--data-dir", "/tmp/etcd.data",
             "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", f"http://{etcd}:2379",
             "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", f"http://{etcd}:2380",
             "--initial-cluster", f"node1=http://{etcd}:2380"])
        created.append(etcd)

        # Install the pinned runtime/test dependencies before isolating the
        # consumer from the bridge network. The source is mounted read-only.
        run(["docker", "run", "-d", "--rm", "--pull=never", "--network", "bridge",
             "--name", client, "--mount", docker_mount(SDK, "/work", True),
             "--env", "PYTHONPATH=/work/src", "--env", "PYTHONDONTWRITEBYTECODE=1",
             PYTHON_IMAGE, "sh", "-lc",
             "python -m pip install --disable-pip-version-check pytest==9.0.2 pytest-asyncio==1.3.0 "
             "grpcio==1.84.0 protobuf==7.36.2 redis==8.1.0 >/tmp/deps.log 2>&1 && sleep infinity"])
        created.append(client)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            probe = run(["docker", "exec", client, "python", "-c",
                         "import grpc,google.protobuf,pytest,redis; print('ready')"], check=False)
            if probe.returncode == 0:
                break
            state = run(["docker", "inspect", "--format", "{{.State.Running}}", client], check=False)
            if state.returncode or state.stdout.strip() != "true":
                logs = run(["docker", "logs", client], check=False)
                raise RuntimeError(f"Python dependency container exited: {logs.stdout} {logs.stderr}")
            time.sleep(0.5)
        else:
            raise TimeoutError("Python dependency container did not become ready")

        run(["docker", "network", "connect", network, client])
        run(["docker", "network", "disconnect", "bridge", client])
        docker_versions = run(["docker", "exec", client, "python", "-c",
                               "import json,sys,grpc,google.protobuf,pytest,redis; print(json.dumps({'python':sys.version.split()[0], 'grpcio':grpc.__version__, 'protobuf':google.protobuf.__version__, 'pytest':pytest.__version__, 'redis':redis.__version__}))"])

        deadline = time.monotonic() + 30
        while True:
            health = run(["docker", "exec", etcd, "/usr/local/bin/etcdctl",
                          "--endpoints=http://127.0.0.1:2379", "endpoint", "health"], check=False)
            if health.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"etcd failed readiness: {health.stdout} {health.stderr}")
            time.sleep(0.25)

        epoch = f"py-unlock-ack-epoch-{suffix}"
        case_results: list[dict[str, object]] = []
        for index, case in enumerate(cases):
            proxy = f"dilock-py-proxy-{index}-{suffix}"
            proxies.append(proxy)
            command = ["docker", "run", "-d", "--rm", "--pull=never", "--network", network,
                       "--name", proxy, "--env", f"DILOCK_ETCD_UPSTREAM=http://{etcd}:2379"]
            if case["drop_revoke_ack"]:
                command.extend(("--env", "DILOCK_DROP_REVOKE_ACK=1"))
            command.extend(("--mount", docker_mount(PROXY_BINARY, "/opt/proxy", True), GO_IMAGE,
                            "/bin/sh", "-c",
                            "cp /opt/proxy /tmp/etcd-unlock-ack-proxy && chmod 0755 /tmp/etcd-unlock-ack-proxy && exec /tmp/etcd-unlock-ack-proxy"))
            run(command)
            created.append(proxy)

            metadata = json.dumps({
                "protocol_major": 1,
                "identity_scheme": "sha256-framed-utf8-v1",
                "profile": "etcd-native-lock-v1",
                "coordination_epoch": epoch,
                "fixed_lease_ms": None,
            }, separators=(",", ":"))
            metadata_key = f"/dilock/namespaces/{case['namespace']}/meta"
            run(["docker", "exec", etcd, "/usr/local/bin/etcdctl",
                 "--endpoints=http://127.0.0.1:2379", "put", metadata_key, metadata])

            stats_url = f"http://{proxy}:2379/__stats"
            env = ["docker", "exec", "--env", f"DILOCK_ETCD_ENDPOINT=http://{proxy}:2379",
                   "--env", f"DILOCK_ETCD_NAMESPACE={case['namespace']}",
                   "--env", f"DILOCK_ETCD_EPOCH={epoch}",
                   "--env", f"DILOCK_ETCD_PROXY_STATS_URL={stats_url}"]
            if case["drop_revoke_ack"]:
                env.extend(("--env", "DILOCK_DROP_REVOKE_ACK=1"))
            consumer = run(env + [client, "python", "/work/tests/fault/etcd_unlock_ack_live_consumer.py"])
            case_results.append({"name": case["name"], "expected_release": case["expected"],
                                 "consumer": json.loads(consumer.stdout.strip())})
            run(["docker", "exec", etcd, "/usr/local/bin/etcdctl",
                 "--endpoints=http://127.0.0.1:2379", "del", metadata_key])

        result.update({
            "status": "PASS",
            "backend": "live etcd native Lock gRPC through shared HTTP/2 fault proxy",
            "etcd_image": ETCD_IMAGE,
            "proxy_image": GO_IMAGE,
            "python_image": PYTHON_IMAGE,
            "python_runtime": json.loads(docker_versions.stdout.strip()),
            "cases": case_results,
            "proxy_binary_sha256": sha256(PROXY_BINARY),
            "source_sha256": {
                "sdk/python/src/dilock/etcd.py": sha256(SDK / "src/dilock/etcd.py"),
                "sdk/python/tests/fault/etcd_unlock_ack_live_consumer.py": sha256(Path(__file__).with_name("etcd_unlock_ack_live_consumer.py")),
                "sdk/python/tests/fault/etcd_unlock_ack_docker.py": sha256(Path(__file__).resolve()),
                "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/main.go": sha256(PROXY_SOURCE / "main.go"),
                "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.mod": sha256(PROXY_SOURCE / "go.mod"),
                "sdk/cpp/tests/fault/etcd_unlock_ack_proxy/go.sum": sha256(PROXY_SOURCE / "go.sum"),
            },
        })
    except Exception as error:
        result.update({"status": "FAIL", "error": str(error)})
    finally:
        for key in [f"/dilock/namespaces/{case['namespace']}/meta" for case in cases]:
            if etcd in created:
                run(["docker", "exec", etcd, "/usr/local/bin/etcdctl",
                     "--endpoints=http://127.0.0.1:2379", "del", key], check=False)
        cleanup_errors = []
        for name in reversed(created):
            cleanup = run(["docker", "rm", "-f", name], check=False)
            if cleanup.returncode:
                cleanup_errors.append(f"{name}: {cleanup.stderr.strip()}")
        if network_created:
            cleanup = run(["docker", "network", "rm", network], check=False)
            if cleanup.returncode:
                cleanup_errors.append(f"{network}: {cleanup.stderr.strip()}")
        result["cleanup"] = "PASS" if not cleanup_errors else {"status": "FAIL", "errors": cleanup_errors}

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" and result["cleanup"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
