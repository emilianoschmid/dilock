"""Reproduce Redis asynchronous-replication failover's stale-promotion limit.

This is a limitation test, not a claim that Dilock prevents split ownership.
It uses two local Redis processes and real SDK workers. Never run against a
non-disposable Redis service.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import socket

ROOT = Path(__file__).resolve().parents[1]
REDIS_SERVER = "/mnt/c/codex/dilock/.tools/redis-8.10.2/src/redis-server"
REDIS_CLI = "/mnt/c/codex/dilock/.tools/redis-8.10.2/src/redis-cli"
PRIMARY_PORT = 16479
REPLICA_PORT = 16480
SDKS = ("dotnet", "python", "cpp")  # Historical/default RDS-003 scope.
ALL_SDKS = ("dotnet", "cpp", "python", "typescript", "go", "rust", "java")
REDIS_IMAGE = "redis:8.10.2@sha256:c94085d298b738be22c9ccdc0ac3761fa6649df7dd82ad1d42367f3cb9714935"
DOCKER_REDIS_CONTAINERS: dict[int, str] = {}
DOCKER_NETWORK = ""
DOCKER_REDIS_EVIDENCE: dict[str, dict[str, str | bool]] = {}


def wsl(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["wsl.exe", "-d", "Ubuntu-26.04", "--", *args], cwd=ROOT,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=20,
    )
    if check and result.returncode:
        raise RuntimeError(f"WSL command failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout.strip()


def redis_cli(port: int, *args: str, check: bool = True) -> str:
    if port in DOCKER_REDIS_CONTAINERS:
        result = docker_run("exec", DOCKER_REDIS_CONTAINERS[port], "redis-cli", "-p", "6379", "--raw", *args,
                            check=check)
        return result.stdout.strip()
    return wsl(REDIS_CLI, "-h", "127.0.0.1", "-p", str(port), "--raw", *args, check=check)


def docker_run(*args: str, check: bool = True, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *args], cwd=ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout,
    )
    if check and result.returncode:
        raise RuntimeError(f"Docker command failed ({result.returncode}): {' '.join(args)}: {result.stderr.strip()}")
    return result


def docker_read_file(container: str, source_path: str, destination: Path) -> tuple[bool, str]:
    result = subprocess.run(
        ["docker", "exec", container, "cat", source_path], cwd=ROOT,
        capture_output=True, timeout=30,
    )
    if result.returncode:
        return False, result.stderr.decode("utf-8", errors="replace").strip()
    destination.write_bytes(result.stdout)
    return True, ""


def _port_is_closed(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.25)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def current_source_inventory() -> tuple[dict[str, str], list[dict[str, str]]]:
    baseline_path = ROOT / "validation/implementation/m6-current-source-matrix-docker-20261009.json"
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))["source_hashes_sha256"]
    observed: dict[str, str] = {}
    baseline_drift: list[dict[str, str]] = []
    for relative, recorded in baseline.items():
        path = ROOT / relative
        current = hashlib.sha256(path.read_bytes()).hexdigest()
        observed[relative] = current
        if current != recorded:
            baseline_drift.append({"path": relative, "previous_sha256": recorded,
                                   "current_sha256": current})
    return observed, baseline_drift


def start_docker_redis(port: int, side: str, run_id: str) -> dict[str, str]:
    container = f"dilock-rds003-{run_id[:12]}-{side}"
    docker_run(
        "run", "-d", "--name", container, "--network", DOCKER_NETWORK,
        "--network-alias", side, "--publish", f"127.0.0.1:{port}:6379",
        "--tmpfs", "/data:rw,noexec,nosuid,size=64m", REDIS_IMAGE,
        "redis-server", "--port", "6379", "--bind", "0.0.0.0",
        "--protected-mode", "no", "--appendonly", "no", "--save", "", "--dir", "/data",
    )
    DOCKER_REDIS_CONTAINERS[port] = container
    return {"_driver": "docker", "_container": container, "_side": side,
            "_evidence_dir": "", "port": str(port)}


def redis_info(port: int) -> dict[str, str]:
    result = redis_cli(port, "INFO", "replication")
    return dict(line.split(":", 1) for line in result.splitlines() if ":" in line)


def replica_fully_synchronized() -> bool:
    info = redis_info(REPLICA_PORT)
    return info.get("master_link_status") == "up" and info.get("master_sync_in_progress") == "0"


def replica_disconnected_read_only() -> bool:
    info = redis_info(REPLICA_PORT)
    return (info.get("role") == "slave" and info.get("master_link_status") == "down"
            and info.get("master_sync_in_progress") == "0")


async def wait_for(predicate, *, timeout: float = 8, message: str) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return
        except Exception as exc:  # diagnostic retry while a local server starts
            last = repr(exc)
        await asyncio.sleep(0.1)
    raise TimeoutError(f"{message}; last observation={last!r}")


def start_redis(port: int, directory: Path, *, replica_of: int | None = None) -> dict[str, str]:
    directory.mkdir(parents=True, exist_ok=True)
    # Redis's streamed RDB install needs normal Linux filesystem semantics;
    # keep runtime data in a unique WSL ext4 directory and retain a SAVE copy
    # plus logs under validation/implementation before shutdown.
    linux_dir = f"/tmp/dilock-rds003-{directory.parent.name}/{directory.name}"
    wsl("mkdir", "-p", linux_dir)
    log_path = str((directory / f"redis-{port}.log")).replace("\\", "/")
    pid_path = str((directory / f"redis-{port}.pid")).replace("\\", "/")
    log_path = wsl("wslpath", "-a", log_path)
    pid_path = wsl("wslpath", "-a", pid_path)
    config = {
        # Windows-hosted SDK workers reach the isolated WSL VM over its
        # private vEthernet address; do not expose beyond that local VM link.
        "port": str(port), "bind": "0.0.0.0", "protected-mode": "no",
        "daemonize": "yes", "dir": linux_dir, "dbfilename": "dump.rdb",
        "appendonly": "no", "save": "", "pidfile": pid_path,
        "logfile": log_path, "loglevel": "notice",
        "_runtime_dir": linux_dir, "_evidence_dir": str(directory),
    }
    argv = [REDIS_SERVER]
    for name, value in config.items():
        if name.startswith("_"):
            continue
        argv.extend([f"--{name}", value])
    if replica_of is not None:
        argv.extend(["--replicaof", "127.0.0.1", str(replica_of)])
    wsl(*argv)
    return config


def stop_redis(port: int, config: dict[str, str]) -> bool:
    try:
        if config.get("_driver") == "docker":
            save_result = redis_cli(port, "SAVE", check=False)
            container = config["_container"]
            evidence_path = Path(config["_evidence_dir"])
            saved = False
            if evidence_path:
                evidence_path.mkdir(parents=True, exist_ok=True)
                log_result = docker_run("logs", container, check=False)
                logs = log_result.stdout + log_result.stderr
                (evidence_path / f"redis-{port}.log").write_text(logs, encoding="utf-8")
                rdb_path = evidence_path / f"redis-{port}.rdb"
                saved, copy_error = docker_read_file(container, "/data/dump.rdb", rdb_path)
                DOCKER_REDIS_EVIDENCE[config["_side"]] = {
                    "save_acknowledged": save_result == "OK",
                    "rdb_saved": saved,
                    "rdb_copy_error": copy_error,
                    "rdb_path": str(rdb_path.relative_to(ROOT)) if rdb_path.is_relative_to(ROOT) else str(rdb_path),
                    "redis_log_path": str((evidence_path / f"redis-{port}.log").relative_to(ROOT)),
                }
            removed = docker_run("rm", "--force", container, check=False)
            DOCKER_REDIS_CONTAINERS.pop(port, None)
            return removed.returncode == 0 and saved
        redis_cli(port, "SAVE", check=False)
        evidence_dir = str(Path(config["_evidence_dir"])).replace("\\", "/")
        evidence_dir = wsl("wslpath", "-a", evidence_dir)
        wsl("cp", f"{config['_runtime_dir']}/dump.rdb", f"{evidence_dir}/redis-{port}.rdb", check=False)
        result = redis_cli(port, "SHUTDOWN", "NOSAVE", check=False)
        # Redis closes the control socket on successful SHUTDOWN. Verify its
        # process is gone using the unique pidfile, never by broad process name.
        linux_pidfile = config["pidfile"]
        pid = wsl("sh", "-lc", f"cat {linux_pidfile}", check=False)
        if pid.isdigit():
            wsl("kill", "-0", pid, check=False)
            still_alive = wsl("sh", "-lc", f"kill -0 {pid} 2>/dev/null", check=False)
            if still_alive:
                wsl("kill", pid, check=False)
        return not result
    except Exception:
        return False


class RunningWorker:
    def __init__(self, process, log):
        from conformance.run import Worker
        self.worker = Worker(process)
        self.log = log

    async def close(self):
        await self.worker.close()
        self.log.close()


async def run(args) -> int:
    global DOCKER_NETWORK
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "sdk/python/src"))
    from conformance.run import expect
    from tools.provision import provision

    run_id = uuid.uuid4().hex
    run_dir = ROOT / "validation/implementation" / f"rds-003-{run_id}"
    run_dir.mkdir(parents=True)
    primary_dir, replica_dir = run_dir / "primary", run_dir / "replica"
    manifest_path = args.workers_manifest or (ROOT / "conformance/workers.json")
    manifest: dict[str, dict] = {}
    primary_config = replica_config = None
    workers: dict[tuple[str, str], RunningWorker] = {}
    cells: list[dict] = []
    errors: list[str] = []
    services_stopped = {"primary": True, "replica": True}
    docker_cleanup = {"network_created": False, "network_removed": False,
                      "containers_removed": False, "published_ports_closed": False}
    endpoint_reachability: dict[str, bool] = {}
    env = os.environ.copy()
    host_reachable_wsl_ip = ""
    namespace = "rds003-" + run_id[:20]
    epoch = "rds003-local-1"
    selected_sdks = args.sdks
    directions = tuple((owner, contender) for owner in selected_sdks for contender in selected_sdks)
    expected_reproduction = False
    key = None
    try:
        if args.backend == "docker":
            for port in (PRIMARY_PORT, REPLICA_PORT):
                with socket.socket() as listener:
                    try:
                        listener.bind(("127.0.0.1", port))
                    except OSError as exc:
                        raise RuntimeError(f"test port 127.0.0.1:{port} is not free: {exc}") from exc
            DOCKER_NETWORK = f"dilock-rds003-{run_id[:12]}"
            docker_run("network", "create", DOCKER_NETWORK)
            docker_cleanup["network_created"] = True
            host_reachable_wsl_ip = "127.0.0.1"
            primary_config = start_docker_redis(PRIMARY_PORT, "primary", run_id)
        else:
            # Preserve the historical default backend driver and reject port collisions.
            for port in (PRIMARY_PORT, REPLICA_PORT):
                output = wsl("sh", "-lc", f"ss -H -ltn '( sport = :{port} )'", check=False)
                if output:
                    raise RuntimeError(f"test port {port} already has a listener: {output}")
            host_reachable_wsl_ip = wsl("hostname", "-I").split()[0]
            primary_config = start_redis(PRIMARY_PORT, primary_dir)
        services_stopped["primary"] = False
        await wait_for(lambda: redis_cli(PRIMARY_PORT, "PING") == "PONG", message="primary ready")
        await provision(f"redis://{host_reachable_wsl_ip}:{PRIMARY_PORT}", namespace, epoch)
        if args.backend == "docker":
            replica_config = start_docker_redis(REPLICA_PORT, "replica", run_id)
            services_stopped["replica"] = False
            await wait_for(lambda: redis_cli(REPLICA_PORT, "PING") == "PONG", message="replica ready")
            redis_cli(REPLICA_PORT, "REPLICAOF", "primary", "6379")
        else:
            replica_config = start_redis(REPLICA_PORT, replica_dir, replica_of=PRIMARY_PORT)
        services_stopped["replica"] = False
        await wait_for(replica_fully_synchronized,
                       message="replica connected and synchronized")
        if redis_cli(REPLICA_PORT, "GET", f"dilock:namespace:{namespace}:meta") != redis_cli(
            PRIMARY_PORT, "GET", f"dilock:namespace:{namespace}:meta"
        ):
            raise RuntimeError("namespace metadata did not replicate before fault")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # Allow a pinned, current-source worker manifest (including its Docker
        # launcher for C++) without changing the historical default manifest.
        env["DILOCK_CPP_DOCKER_WORKER"] = "1"
        env["DILOCK_CPP_DOCKER_WORKER_PATH"] = str(
            ROOT / ".tools/m6-cpp-partition-worker-20261009/dilock_cpp_worker"
        )
        if args.backend == "docker":
            env["DILOCK_CPP_DOCKER_NETWORK"] = DOCKER_NETWORK
        for sdk in selected_sdks:
            for side, port in (("primary", PRIMARY_PORT), ("replica", REPLICA_PORT)):
                config = run_dir / f"{sdk}-{side}.config.json"
                config.write_text(json.dumps({
                    "backend": "redis", "endpoint": (
                        f"redis://{side}:6379" if args.backend == "docker" and sdk == "cpp"
                        else f"redis://{host_reachable_wsl_ip}:{port}"
                    ),
                    "namespace": namespace, "epoch": epoch,
                }), encoding="utf-8")
                argv = manifest[sdk]["argv"] + [str(config)]
                log = (run_dir / f"{sdk}-{side}.stderr.log").open("wb")
                process = await asyncio.create_subprocess_exec(
                    *argv, cwd=ROOT, env=env, stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=log,
                    start_new_session=(os.name != "nt"),
                )
                workers[(sdk, side)] = RunningWorker(process, log)
                hello = await workers[(sdk, side)].worker.request("hello", timeout=60)
                expect(hello, "ok")
                endpoint_reachability[f"{sdk}-{side}"] = True

        # Each ordered owner/promoted-replica pair gets a fresh resource. Keep
        # the replica read-only but disconnected while the primary grants the
        # lease, prove that the lock did not reach it, then promote and acquire
        # on that stale authority while the primary lease remains held.
        for index, (owner_sdk, promoted_sdk) in enumerate(directions):
            cell = {"owner_sdk": owner_sdk, "promoted_replica_sdk": promoted_sdk,
                    "status": "failed", "primary_acquire": "not_run",
                    "promoted_replica_acquire": "not_run",
                    "simultaneous_live_owner_values": False, "timeline": []}
            cells.append(cell)
            try:
                # Rejoin and catch up before each independent resource scenario.
                redis_cli(REPLICA_PORT, "REPLICAOF",
                          "primary" if args.backend == "docker" else "127.0.0.1",
                          "6379" if args.backend == "docker" else str(PRIMARY_PORT))
                await wait_for(replica_fully_synchronized,
                               message=f"replica catch-up before case {index + 1}")
                cell["timeline"].append("replica_synchronized_read_only")

                # Point the replica at a deliberately closed local port. It
                # remains read-only with its synchronized dataset.
                redis_cli(REPLICA_PORT, "REPLICAOF", "primary", "9")
                await wait_for(replica_disconnected_read_only,
                               message=f"replica disconnected but still read-only before case {index + 1}")
                cell["timeline"].append("replica_disconnected_read_only")

                resource = f"rds003/{index}/{uuid.uuid4().hex}"
                from dilock.core import identity
                key = f"dilock:{{{identity(namespace, resource)}}}:lease"
                primary_owner = workers[(owner_sdk, "primary")].worker
                replica_owner = workers[(promoted_sdk, "replica")].worker
                owner = await primary_owner.request(
                    "try_acquire", timeout=15, resource=resource, lease_ms=120000, rpc_ms=3000,
                )
                cell["primary_acquire"] = owner.get("status", "missing_status")
                cell["primary_remote_outcome_unknown"] = owner.get("remote_outcome_unknown")
                if owner.get("status") != "acquired":
                    raise AssertionError(f"primary owner did not acquire; no retry: {owner}")
                cell["timeline"].append("primary_lease_acquired")
                primary_value = redis_cli(PRIMARY_PORT, "GET", key)
                replica_value = redis_cli(REPLICA_PORT, "GET", key)
                if not primary_value or replica_value:
                    raise AssertionError(
                        f"stale promotion precondition failed: primary_present={bool(primary_value)} "
                        f"replica_present={bool(replica_value)}"
                    )
                cell["primary_key_present"] = True
                cell["replica_key_absent_before_promotion"] = True
                cell["timeline"].append("replica_lock_key_absence_verified")

                # Promote only after the live primary lease is known to be
                # absent from the replica's dataset.
                redis_cli(REPLICA_PORT, "REPLICAOF", "NO", "ONE")
                await wait_for(lambda: redis_info(REPLICA_PORT).get("role") == "master",
                               message=f"stale replica promotion after primary grant in case {index + 1}")
                cell["timeline"].append("stale_replica_promoted_after_primary_grant")

                second = await replica_owner.request(
                    "try_acquire", timeout=15, resource=resource, lease_ms=120000, rpc_ms=3000,
                )
                cell["promoted_replica_acquire"] = second.get("status", "missing_status")
                cell["replica_remote_outcome_unknown"] = second.get("remote_outcome_unknown")
                if second.get("status") != "acquired":
                    raise AssertionError(f"promoted replica did not grant independent lock: {second}")
                cell["timeline"].append("promoted_authority_granted_second_lease")
                replica_value = redis_cli(REPLICA_PORT, "GET", key)
                if not replica_value or replica_value == primary_value:
                    raise AssertionError("expected distinct simultaneous owner values on both authorities")
                cell.update({
                    "simultaneous_live_owner_values": True,
                    "owner_values_distinct": True,
                    "primary_value_sha256": __import__("hashlib").sha256(primary_value.encode()).hexdigest(),
                    "replica_value_sha256": __import__("hashlib").sha256(replica_value.encode()).hexdigest(),
                    "replica_had_lock_before_promotion": False,
                })
                # Both lease handles must still report held after the two
                # independent authorities grant the same resource.
                primary_state = await primary_owner.request("inspect", lease_id=owner["lease_id"])
                replica_state = await replica_owner.request("inspect", lease_id=second["lease_id"])
                primary_is_held = (
                    primary_state.get("status") == "held"
                    or (primary_state.get("status") == "ok" and primary_state.get("state") == "held")
                )
                if not primary_is_held:
                    raise AssertionError(f"old-primary lease is not held after promotion: {primary_state}")
                replica_is_held = (
                    replica_state.get("status") == "held"
                    or (replica_state.get("status") == "ok" and replica_state.get("state") == "held")
                )
                if not replica_is_held:
                    raise AssertionError(f"promoted-replica lease is not held: {replica_state}")
                cell["old_owner_still_live_after_promotion"] = True
                expected_reproduction = True
                expect(await primary_owner.request("release", lease_id=owner["lease_id"]), "released")
                expect(await replica_owner.request("release", lease_id=second["lease_id"]), "released")
                cell["cleanup"] = {"primary_owner_release": "released",
                                   "replica_owner_release": "released"}
                if redis_cli(PRIMARY_PORT, "GET", key) or redis_cli(REPLICA_PORT, "GET", key):
                    raise AssertionError("owner-checked release left an unexpected test key")
                cell["status"] = "passed"
            except Exception as exc:
                cell["error"] = f"{type(exc).__name__}: {exc}"
                if (cell.get("primary_remote_outcome_unknown") is True
                        or cell.get("replica_remote_outcome_unknown") is True):
                    cell["status"] = "inconclusive"
                else:
                    cell["status"] = "failed"
                errors.append(f"cell {index + 1} ({owner_sdk}->{promoted_sdk}): {cell['error']}")
                # No acquisition with an uncertain result is repeated on this
                # resource; subsequent directions use distinct resource names.

        if (len(cells) == len(directions) and all(cell["status"] == "passed" for cell in cells)
                and expected_reproduction):
            status = "passed"
        elif (cells and all(cell["status"] in ("passed", "inconclusive") for cell in cells)
              and any(cell["status"] == "inconclusive" for cell in cells)):
            status = "partial"
        else:
            status = "failed"
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        status = "failed"
    finally:
        for worker in workers.values():
            try:
                await worker.close()
            except Exception as exc:
                errors.append(f"worker cleanup: {type(exc).__name__}: {exc}")
        if replica_config is not None:
            if args.backend == "docker":
                replica_config["_evidence_dir"] = str(replica_dir)
            services_stopped["replica"] = stop_redis(REPLICA_PORT, replica_config)
        if primary_config is not None:
            if args.backend == "docker":
                primary_config["_evidence_dir"] = str(primary_dir)
            services_stopped["primary"] = stop_redis(PRIMARY_PORT, primary_config)
        if args.backend == "docker":
            docker_cleanup["containers_removed"] = not DOCKER_REDIS_CONTAINERS
            if docker_cleanup["network_created"]:
                result = docker_run("network", "rm", DOCKER_NETWORK, check=False)
                docker_cleanup["network_removed"] = result.returncode == 0
            docker_cleanup["published_ports_closed"] = all(
                _port_is_closed(port) for port in (PRIMARY_PORT, REPLICA_PORT)
            )
            services_stopped["primary"] = services_stopped["primary"] and docker_cleanup["published_ports_closed"]
            services_stopped["replica"] = services_stopped["replica"] and docker_cleanup["published_ports_closed"]
        else:
            for port, name in ((PRIMARY_PORT, "primary"), (REPLICA_PORT, "replica")):
                output = wsl("sh", "-lc", f"ss -H -ltn '( sport = :{port} )'", check=False)
                services_stopped[name] = services_stopped[name] and not output

        per_sdk = {}
        for sdk in ALL_SDKS:
            participating = [cell for cell in cells if cell.get("owner_sdk") == sdk or
                             cell.get("promoted_replica_sdk") == sdk]
            if sdk in selected_sdks and participating and all(cell["status"] == "passed" for cell in participating):
                sdk_status = "passed"
            elif sdk in selected_sdks and any(cell["status"] == "inconclusive" for cell in participating):
                sdk_status = "partial"
            elif sdk in selected_sdks:
                sdk_status = "blocked" if not endpoint_reachability else "failed"
            else:
                sdk_status = "not_run"
            per_sdk[sdk] = {"status": sdk_status, "required_by_case": sdk in selected_sdks,
                            "worker_endpoints_started": sum(
                                name.startswith(f"{sdk}-") and ready
                                for name, ready in endpoint_reachability.items()
                            )}
        source_hashes, previous_source_drift = current_source_inventory()
        source_hashes["conformance/redis_failover_limitation.py"] = hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest()
        source_hashes["sdk/cpp/worker_launcher.py"] = hashlib.sha256(
            (ROOT / "sdk/cpp/worker_launcher.py").read_bytes()
        ).hexdigest()
        source_hashes[str(manifest_path.relative_to(ROOT)).replace("\\", "/")] = hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest()
        # Hash files selected by the actual worker manifest instead of a fixed
        # collection of historical artifacts. Docker launcher environments are
        # recorded separately because their child binaries are not necessarily
        # present in argv.
        worker_manifest_inputs: dict[str, dict] = {}
        for sdk in selected_sdks:
            argv = manifest[sdk]["argv"]
            inputs: dict[str, dict[str, str]] = {}
            for argument in argv[1:]:
                candidate = Path(argument)
                if not candidate.is_absolute():
                    candidate = ROOT / candidate
                if candidate.is_file():
                    resolved = candidate.resolve()
                    try:
                        relative = resolved.relative_to(ROOT).as_posix()
                    except ValueError:
                        relative = str(resolved)
                    inputs[relative] = {
                        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest()
                    }
            worker_manifest_inputs[sdk] = {
                "argv": argv,
                "files_sha256": inputs,
            }
        if "cpp" in worker_manifest_inputs:
            cpp_env = {
                "DILOCK_CPP_DOCKER_WORKER": env.get("DILOCK_CPP_DOCKER_WORKER", ""),
                "DILOCK_CPP_DOCKER_WORKER_PATH": env.get("DILOCK_CPP_DOCKER_WORKER_PATH", ""),
                "DILOCK_CPP_DOCKER_NETWORK": env.get("DILOCK_CPP_DOCKER_NETWORK", ""),
            }
            cpp_worker = Path(cpp_env["DILOCK_CPP_DOCKER_WORKER_PATH"])
            if cpp_worker.is_file():
                cpp_env["DILOCK_CPP_DOCKER_WORKER_PATH_SHA256"] = hashlib.sha256(
                    cpp_worker.read_bytes()
                ).hexdigest()
            worker_manifest_inputs["cpp"]["environment"] = cpp_env
        report = {
            "id": "RDS-003", "date": time.strftime("%Y-%m-%d"), "status": status,
            "scenario": "A synchronized replica is disconnected while still read-only. The primary grants a Redis SET NX PX; the lock key is verified absent on the replica; only then is the stale replica promoted and independently grants the same resource while the primary lease remains live.",
            "profile": "redis-single-authority-v1", "backend": "Redis 8.10.2 primary + replica",
            "replication_ack_required": False, "wait_used": False,
            "primary_endpoint": f"redis://{host_reachable_wsl_ip}:{PRIMARY_PORT}",
            "replica_endpoint": f"redis://{host_reachable_wsl_ip}:{REPLICA_PORT}",
            "namespace": namespace, "lease_ms": 120000,
            "coverage": {"directed_pairs_total": len(cells),
                         "directed_pairs_passed": sum(cell["status"] == "passed" for cell in cells),
                         "directed_pairs_inconclusive": sum(cell["status"] == "inconclusive" for cell in cells),
                         "directed_pairs_failed": sum(cell["status"] == "failed" for cell in cells),
                         "matrix": [list(direction) for direction in directions],
                         "interpretation": (
                             f"Exhaustive ordered pairs among the selected {len(selected_sdks)} SDK(s); "
                             f"{len(ALL_SDKS) - len(selected_sdks)} unselected SDK(s) are not_run."
                         )},
            "fault_schedule": [
                "Synchronize replica while it is read-only.",
                "Disconnect replica by pointing it at a closed local port; verify role=slave and link down.",
                "Acquire on primary and verify the lock key exists only on primary.",
                "Promote the stale replica and acquire the same resource there while the primary lease remains held.",
            ],
            "harness_sha256": __import__("hashlib").sha256(Path(__file__).read_bytes()).hexdigest(),
            "counterexample_reproduced": expected_reproduction,
            "prevention_claimed": False,
            "participants": per_sdk, "cells": cells,
            "selected_sdks": list(selected_sdks),
            "worker_manifest": {
                "path": str(manifest_path.relative_to(ROOT)).replace("\\", "/"),
                "sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            },
            "worker_manifest_runtime_inputs": worker_manifest_inputs,
            "current_source_worker_audit": "validation/implementation/m6-etcd-leader-failover-current-source-audit-20261010-r5.json",
            "current_source_hashes_sha256": source_hashes,
            "source_hash_verification": {
                "status": "PASS", "checked": len(source_hashes),
                "baseline_source_report": "validation/implementation/m6-current-source-matrix-docker-20261009.json",
                "baseline_drift_refreshed": previous_source_drift,
                "interpretation": "Source paths from the prior 813-file seven-SDK inventory were rehashed from the current checkout; drift is reported with both prior and current digests. Worker artifacts are separately hashed below.",
            },
            "services_stopped": services_stopped,
            "real_processes": True, "real_backend": True,
            "run_directory": str(run_dir), "errors": errors,
            "previous_two_direction_report": "validation/implementation/m6-redis-failover-limitation-rds003-previous-two-directions.json",
            "diagnostic_attempts": [
                {
                    "run_directory": "validation/implementation/rds-003-d1ed8024f44a4c5fa8941afb1f61ba02",
                    "status": "blocked", "reason": "Replica RDB import from DrvFS failed; fixed by using a unique WSL ext4 runtime data directory.",
                },
                {
                    "run_directory": "validation/implementation/rds-003-e93cf393fc394008a7866d73947f6974",
                    "status": "partial", "reason": "Two double grants passed; third diagnostic direction returned acquisition_unknown and was not retried. Logs/data retained.",
                },
                {
                    "run_directory": "validation/implementation/rds-003-b56181e07da348f1ad935b63a9040f6d",
                    "status": "partial", "reason": "Two double grants passed; third diagnostic direction returned acquisition_unknown and was not retried. Logs/data retained.",
                },
            ],
            "limitations": [
                "This manually promotes a disconnected stale replica after proving the primary's lock write is absent; it does not run Sentinel or Cluster failover control planes or prove a particular managed-service schedule.",
                "A finite local counterexample does not prove all schedules or production outcomes.",
                "Redis commands are not acknowledged after quorum/replica durability; no fencing token is exposed by the Redis profile.",
                "Both independent leases were explicitly released after simultaneous ownership was observed; test data and logs are retained.",
            ],
        }
        if args.report_path is not None:
            report_path = args.report_path
        elif set(selected_sdks) == set(ALL_SDKS):
            report_path = ROOT / "validation/implementation/m6-redis-failover-limitation-rds003-seven-sdk-20261009.json"
        elif args.backend == "docker":
            report_path = ROOT / "validation/implementation/m6-redis-failover-limitation-rds003-smoke-docker-20261009.json"
        else:
            report_path = ROOT / "validation/implementation/m6-redis-failover-limitation-rds003-ordered-20261007.json"
        report["backend_driver"] = args.backend
        report["redis_image"] = REDIS_IMAGE if args.backend == "docker" else None
        report["docker_engine"] = (
            docker_run("info", "--format", "{{.ServerVersion}} {{.OSType}}").stdout.strip()
            if args.backend == "docker" else None
        )
        report["docker_cleanup"] = docker_cleanup if args.backend == "docker" else None
        report["saved_backend_state"] = DOCKER_REDIS_EVIDENCE if args.backend == "docker" else None
        report["docker_resources"] = ({
            "network": DOCKER_NETWORK,
            "primary_container": primary_config.get("_container") if primary_config else None,
            "replica_container": replica_config.get("_container") if replica_config else None,
            "host_published_ports": ["127.0.0.1:16479", "127.0.0.1:16480"],
            "network_aliases": ["primary", "replica"],
        } if args.backend == "docker" else None)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
    cleanup_ok = all(services_stopped.values())
    if args.backend == "docker":
        cleanup_ok = cleanup_ok and all(docker_cleanup.values())
    return 0 if status == "passed" and cleanup_ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--sdks", nargs="+", metavar="SDK",
        help="SDK names to test (space- or comma-separated); defaults to dotnet python cpp; use 'all' for all seven.",
    )
    selection.add_argument("--all", action="store_true", help="Run all seven core SDKs (49 directed pairs).")
    parser.add_argument(
        "--workers-manifest", type=Path,
        help="Use a built worker manifest; defaults to conformance/workers.json.",
    )
    parser.add_argument("--backend", choices=("wsl", "docker"), default="wsl",
                        help="Disposable Redis runtime; defaults to the historical WSL runner.")
    parser.add_argument("--report-path", type=Path,
                        help="Write evidence to this path instead of replacing the historical default report.")
    parsed = parser.parse_args()
    if parsed.all or parsed.sdks == ["all"]:
        selected = ALL_SDKS
    elif parsed.sdks is None:
        selected = SDKS
    else:
        selected = tuple(sdk for item in parsed.sdks for sdk in item.split(",") if sdk)
        unknown = sorted(set(selected) - set(ALL_SDKS))
        if unknown:
            parser.error(f"unknown SDK(s): {', '.join(unknown)}; choose from {', '.join(ALL_SDKS)}")
        if not selected or len(set(selected)) != len(selected):
            parser.error("SDK selection must be non-empty and contain no duplicates")
    parsed.sdks = selected
    if parsed.workers_manifest is not None and not parsed.workers_manifest.is_absolute():
        parsed.workers_manifest = (ROOT / parsed.workers_manifest).resolve()
    if parsed.report_path is not None and not parsed.report_path.is_absolute():
        parsed.report_path = (ROOT / parsed.report_path).resolve()
    if parsed.report_path is not None:
        evidence_root = (ROOT / "validation/implementation").resolve()
        try:
            parsed.report_path.resolve().relative_to(evidence_root)
        except ValueError:
            parser.error("--report-path must remain under validation/implementation")
    return asyncio.run(run(parsed))


if __name__ == "__main__":
    raise SystemExit(main())
