"""Verify TYPE failures remain backend errors through real SDK workers."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from conformance.run import Worker, expect  # noqa: E402
from tools.provision import provision  # noqa: E402
from tools.reference_identity import redis_key  # noqa: E402

EXPECTED_STATUS = "backend_unavailable"
MARKER_TTL_MS = 300_000


async def run(endpoint: str, sdks: list[str], output: Path, workers_path: Path) -> int:
    namespace = "type-failure-" + uuid.uuid4().hex[:20]
    epoch = "local-type-failure-" + uuid.uuid4().hex
    await provision(endpoint, namespace, epoch)
    manifest = json.loads(workers_path.read_text(encoding="utf-8"))
    run_dir = output.parent / ("type-failure-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "worker-config.json"
    config.write_text(
        json.dumps({"backend": "redis", "endpoint": endpoint, "namespace": namespace, "epoch": epoch}),
        encoding="utf-8",
    )

    client = Redis.from_url(
        endpoint, retry=Retry(NoBackoff(), 0), socket_timeout=2, socket_connect_timeout=2
    )
    redis_version = str((await client.info("server")).get("redis_version", "unknown"))
    workers: dict[str, Worker] = {}
    logs = []
    cells = []
    resource = "classify-type-failure"
    key = redis_key(namespace, resource)
    marker = "preserve-" + uuid.uuid4().hex
    try:
        await client.set(key, marker, px=MARKER_TTL_MS)
        before = await client.get(key)
        for sdk in sdks:
            argv = manifest[sdk]["argv"] + [str(config)]
            log = (run_dir / f"{sdk}.stderr.log").open("wb")
            logs.append(log)
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=ROOT,
                env=os.environ.copy(),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
                start_new_session=(os.name != "nt"),
            )
            worker = Worker(process)
            workers[sdk] = worker
            expect(await worker.request("hello", timeout=60), "ok")

        for sdk, worker in workers.items():
            response = await worker.request(
                "try_acquire", resource=resource, lease_ms=20_000, rpc_ms=1_000, auto_renew=False
            )
            value_after = await client.get(key)
            ttl_after = await client.pttl(key)
            errors = []
            if response.get("status") != EXPECTED_STATUS:
                errors.append(f"expected {EXPECTED_STATUS}, received {response}")
            if response.get("remote_outcome_unknown") is True:
                errors.append("confirmed SET NX contention was incorrectly reported as an unknown grant")
            if before != marker.encode() or value_after != before:
                errors.append("existing string lock marker changed")
            if ttl_after <= 0:
                errors.append(f"existing lock key lost its TTL: {ttl_after}")
            cell = {
                "sdk": sdk,
                "status": "passed" if not errors else "failed",
                "response": response,
                "key_marker_preserved": value_after == before,
                "ttl_ms_after": ttl_after,
                "errors": errors,
            }
            cells.append(cell)
            print(json.dumps(cell), flush=True)
    finally:
        for worker in workers.values():
            await worker.close()
        for log in logs:
            log.close()
        await client.aclose()
        result = {
            "id": "RDS-001-TYPE-ERROR",
            "status": "passed" if cells and all(cell["status"] == "passed" for cell in cells) else "failed",
            "backend": f"Redis {redis_version}; default user denies TYPE; real JSONL SDK workers",
            "endpoint": endpoint,
            "redis_version": redis_version,
            "namespace": namespace,
            "epoch": epoch,
            "real_processes": True,
            "real_backend": True,
            "cells": cells,
            "run_directory": str(run_dir),
            "limitations": [
                "The disposable Redis authority denies only TYPE; it does not model transport loss or every ACL profile.",
                "The test seeds one existing string key and confirms its value and positive TTL survive classification failure.",
                "This proves backend-error classification, not lock mutual exclusion or production ACL readiness.",
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return int(not cells or any(cell["status"] != "passed" for cell in cells))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument(
        "--sdks",
        nargs="+",
        default=["dotnet", "python", "cpp", "typescript", "go", "rust", "java"],
        choices=["dotnet", "cpp", "python", "typescript", "go", "rust", "java"],
    )
    parser.add_argument("--workers", default="conformance/workers.json")
    parser.add_argument("--output", default="validation/implementation/m6-rds001-type-failure-seven-sdk.json")
    args = parser.parse_args()
    return asyncio.run(run(args.endpoint, args.sdks, ROOT / args.output, ROOT / args.workers))


if __name__ == "__main__":
    raise SystemExit(main())
