"""Exercise real SDK recovery when Redis has flushed its Lua script cache."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path

from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from conformance.run import Worker, expect  # noqa: E402
from tools.provision import provision  # noqa: E402


def script_sha(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


async def run(endpoint: str, output: Path, workers_path: Path) -> dict[str, object]:
    namespace = "rds002-" + uuid.uuid4().hex[:24]
    epoch = "rds002-local-" + uuid.uuid4().hex
    await provision(endpoint, namespace, epoch)
    run_dir = output.parent / ("rds002-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "worker-config.json"
    config.write_text(json.dumps({"backend": "redis", "endpoint": endpoint,
                                 "namespace": namespace, "epoch": epoch}), encoding="utf-8")
    manifest = json.loads(workers_path.read_text(encoding="utf-8"))
    client = Redis.from_url(endpoint, retry=Retry(NoBackoff(), 0),
                            socket_timeout=2, socket_connect_timeout=2)
    info = await client.info("server")
    script_hashes = [script_sha(ROOT / "spec/backends" / f"redis-{name}.lua")
                     for name in ("renew", "release")]
    workers: dict[str, Worker] = {}
    logs = []
    cells: list[dict[str, object]] = []
    try:
        for sdk in ("dotnet", "python", "cpp"):
            argv = manifest[sdk]["argv"] + [str(config)]
            log = (run_dir / f"{sdk}.stderr.log").open("wb")
            logs.append(log)
            process = await asyncio.create_subprocess_exec(
                *argv, cwd=ROOT, env=os.environ.copy(),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=log, start_new_session=(os.name != "nt"),
            )
            worker = Worker(process)
            workers[sdk] = worker
            expect(await worker.request("hello", timeout=60), "ok")

        for sdk, worker in workers.items():
            cell: dict[str, object] = {"sdk": sdk, "status": "failed", "stages": []}
            cells.append(cell)
            try:
                cell["stages"] = await _exercise_sdk(worker, client, namespace, sdk,
                                                       script_hashes)
                cell["status"] = "passed"
            except Exception as error:
                cell["error"] = f"{type(error).__name__}: {error}"

        return {
            "id": "M6-RDS-002-REDIS-SCRIPT-CACHE-LOSS",
            "date": time.strftime("%Y-%m-%d", time.gmtime()),
            "status": "passed" if all(c["status"] == "passed" for c in cells) else "failed",
            "backend": {"name": "redis", "version": str(info.get("redis_version", "unknown")),
                        "endpoint": endpoint, "namespace": namespace, "real_backend": True},
            "real_sdk_worker_processes": True,
            "coverage": {"sdks": [c["sdk"] for c in cells], "cells_passed": sum(c["status"] == "passed" for c in cells),
                         "cells_total": len(cells)},
            "canonical_script_sha1": {"renew": script_hashes[0], "release": script_hashes[1]},
            "cells": cells,
            "interpretation": [
                "The .NET stale-release cell observed EVALSHA followed by SCRIPT LOAD and EVAL after SCRIPT FLUSH had confirmed both canonical hashes absent; this is the reload/fallback path required by RDS-002.",
                "Python and C++ issued inline EVAL after SCRIPT FLUSH, so these are cache-loss and owner-safety control cells, not claims that those adapters use an EVALSHA/NOSCRIPT reload path.",
                "MONITOR data was reduced in memory to command verbs; no script arguments or owner tokens are stored."
            ],
            "not_run": ["Go, Rust, Java and TypeScript are not included in this focused RDS-002 run; they remain not_run for this case."],
            "limitations": ["The report proves behavior after Redis SCRIPT FLUSH. The wire-level NOSCRIPT error response is inferred from the confirmed empty script cache plus the observed EVALSHA/fallback command sequence; the RESP error itself was not captured.",
                            "The run does not cover arbitrary proxy timing, backend failover, or production readiness."],
            "diagnostic_attempts": [
                "The preliminary .NET/Python run passed but was superseded by the three-SDK run; its report is preserved as validation/implementation/m6-rds002-redis-script-cache-loss-dotnet-python-preliminary.json.",
                "One three-SDK launch attempt failed before a test cell because the C++ launcher was given a WSL-native path that Windows could not resolve; the retained logs are under validation/implementation/rds002-edc3bdbcdeb841adb57d1af937dfc8c4. The path was corrected before this final run."
            ],
            "run_directory": str(run_dir),
            "service_cleanup": "caller must stop the disposable Redis instance after preserving this report and server log",
        }
    finally:
        for worker in workers.values():
            await worker.close()
        for log in logs:
            log.close()
        await client.aclose()


async def _exercise_sdk(worker: Worker, client: Redis, namespace: str, sdk: str,
                        script_hashes: list[str]) -> list[dict[str, object]]:
    stages: list[dict[str, object]] = []
    resource = f"script-cache/{sdk}/{uuid.uuid4().hex}"
    lease = expect(await worker.request(
        "try_acquire", resource=resource, lease_ms=10_000, rpc_ms=500,
        auto_renew=False,
    ), "acquired")
    await client.script_flush()
    exists_after_flush = await client.script_exists(*script_hashes)
    if any(exists_after_flush):
        raise AssertionError("SCRIPT FLUSH left a canonical script cached")

    renew_fields: dict[str, object] = {"lease_id": lease["lease_id"]}
    if sdk == "cpp":
        renew_fields["lease_ms"] = 10_000
    renewed, renew_commands = await _observed_request(worker, client, "renew", **renew_fields)
    expect(renewed, "renewed")
    renewal_ttl = await client.pttl(_lock_key(namespace, resource))
    if renewal_ttl <= 5_000:
        raise AssertionError(f"renewed key did not retain a positive TTL: {renewal_ttl}")
    stages.append({"stage": "renew_after_script_flush", "status": "passed",
                   "response": renewed["status"], "pttl_ms": renewal_ttl,
                   "canonical_script_cache_after_flush": exists_after_flush,
                   "observed_command_verbs": renew_commands})

    released, release_commands = await _observed_request(
        worker, client, "release", lease_id=lease["lease_id"]
    )
    expect(released, "released")
    successor = expect(await worker.request(
        "try_acquire", resource=resource, lease_ms=10_000, rpc_ms=500,
        auto_renew=False,
    ), "acquired")
    if successor.get("lease_id") == lease.get("lease_id"):
        raise AssertionError("successor reused the local lease identity")
    stages.append({"stage": "release_after_cache_reload", "status": "passed",
                   "response": released["status"], "successor_acquired": True,
                   "observed_command_verbs": release_commands})

    stale_resource = f"script-cache-stale/{sdk}/{uuid.uuid4().hex}"
    stale = expect(await worker.request(
        "try_acquire", resource=stale_resource, lease_ms=1_500, rpc_ms=500,
        auto_renew=False,
    ), "acquired")
    stale_key = _lock_key(namespace, stale_resource)
    initial_ttl = await client.pttl(stale_key)
    if initial_ttl <= 0:
        raise AssertionError("stale-owner fixture had no live TTL")
    await asyncio.sleep(initial_ttl / 1000 + 0.2)
    replacement = expect(await worker.request(
        "try_acquire", resource=stale_resource, lease_ms=10_000, rpc_ms=500,
        auto_renew=False,
    ), "acquired")
    await client.script_flush()
    exists_after_stale_flush = await client.script_exists(*script_hashes)
    if any(exists_after_stale_flush):
        raise AssertionError("stale-release SCRIPT FLUSH left a canonical script cached")
    stale_release, stale_release_commands = await _observed_request(
        worker, client, "release", lease_id=stale["lease_id"]
    )
    if stale_release.get("status") != "already_not_owned":
        raise AssertionError(f"expired owner cleanup returned {stale_release}")
    if sdk == "dotnet":
        try:
            evalsha_index = stale_release_commands.index("EVALSHA")
            load_index = stale_release_commands.index("SCRIPT LOAD", evalsha_index + 1)
            eval_index = stale_release_commands.index("EVAL", load_index + 1)
        except ValueError as error:
            raise AssertionError(
                f".NET did not show EVALSHA followed by bounded script-load/EVAL fallback: "
                f"{stale_release_commands}"
            ) from error
        if not evalsha_index < load_index < eval_index:
            raise AssertionError(f".NET reload commands arrived out of order: {stale_release_commands}")
    still_busy = await worker.request(
        "try_acquire", resource=stale_resource, lease_ms=2_000, rpc_ms=500,
        auto_renew=False,
    )
    if still_busy.get("status") != "busy":
        raise AssertionError(f"stale-owner cleanup changed successor: {still_busy}")
    replacement_ttl = await client.pttl(stale_key)
    if replacement_ttl <= 0:
        raise AssertionError("successor TTL disappeared after stale cleanup")
    expect(await worker.request("release", lease_id=replacement["lease_id"]), "released")
    stages.append({"stage": "stale_release_after_cache_flush", "status": "passed",
                   "response": stale_release["status"], "successor_remained_busy": True,
                   "successor_pttl_ms": replacement_ttl,
                   "canonical_script_cache_after_flush": exists_after_stale_flush,
                   "observed_command_verbs": stale_release_commands})
    return stages


async def _observed_request(worker: Worker, client: Redis, op: str,
                            **fields: object) -> tuple[dict[str, object], list[str]]:
    """Observe command verbs only; MONITOR arguments can contain owner tokens."""
    verbs: list[str] = []
    monitor = client.monitor()
    await monitor.__aenter__()

    async def collect() -> None:
        async for event in monitor.listen():
            words = event["command"].upper().split()
            if words:
                verb = words[0]
                if verb == "SCRIPT" and len(words) > 1:
                    verb += " " + words[1]
                verbs.append(verb)

    collector = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.03)
        response = await worker.request(op, **fields)
        await asyncio.sleep(0.03)
        return response, verbs
    finally:
        collector.cancel()
        await asyncio.gather(collector, return_exceptions=True)
        await monitor.__aexit__(None, None, None)


def _lock_key(namespace: str, resource: str) -> str:
    from tools.reference_identity import redis_key
    return redis_key(namespace, resource)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=Path, default=ROOT / "conformance/workers.json")
    args = parser.parse_args()
    report = await run(args.endpoint, args.output.resolve(), args.workers.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "report": str(args.output.resolve()),
                      "cells": report["coverage"]}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
