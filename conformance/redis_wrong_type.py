"""Exercise Redis wrong-type handling through real Dilock JSONL workers."""
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

EXPECTED_ERROR_STATUSES = {"protocol_mismatch"}


async def run(endpoint: str, sdks: list[str], output: Path, workers_path: Path) -> int:
    namespace = "rds001-" + uuid.uuid4().hex[:24]
    epoch = "rds001-local-" + uuid.uuid4().hex
    await provision(endpoint, namespace, epoch)
    manifest = json.loads(workers_path.read_text(encoding="utf-8"))
    run_dir = output.parent / ("rds001-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "worker-config.json"
    config.write_text(
        json.dumps(
            {"backend": "redis", "endpoint": endpoint, "namespace": namespace, "epoch": epoch}
        ),
        encoding="utf-8",
    )

    client = Redis.from_url(
        endpoint, retry=Retry(NoBackoff(), 0), socket_timeout=2, socket_connect_timeout=2
    )
    server_info = await client.info("server")
    redis_version = str(server_info.get("redis_version", "unknown"))
    workers: dict[str, Worker] = {}
    logs = []
    cells = []
    try:
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
            resource = "wrong-type/" + sdk + "/" + uuid.uuid4().hex
            key = redis_key(namespace, resource)
            marker = "preserve-" + uuid.uuid4().hex
            await client.lpush(key, marker)
            await client.pexpire(key, 300_000)
            before = {"type": await client.type(key), "values": await client.lrange(key, 0, -1)}
            response = await worker.request(
                "try_acquire", resource=resource, lease_ms=20_000, rpc_ms=500, auto_renew=False
            )
            after = {"type": await client.type(key), "values": await client.lrange(key, 0, -1)}
            ttl_ms = await client.pttl(key)
            state = "passed"
            errors = []
            if response.get("status") not in EXPECTED_ERROR_STATUSES:
                state = "failed"
                errors.append(f"expected a protocol/backend error, received {response}")
            if before["type"] != b"list" or before["values"] != [marker.encode()]:
                state = "failed"
                errors.append(f"unexpected initial key state: {before}")
            if after != before:
                state = "failed"
                errors.append(f"unexpected key state after acquire: {after}")
            if ttl_ms <= 0:
                state = "failed"
                errors.append(f"unexpected key TTL after acquire: {ttl_ms}")
            cell = {
                "sdk": sdk,
                "status": state,
                "resource": resource,
                "response": response,
                "key_type_before": before["type"].decode(),
                "key_type_after": after["type"].decode(),
                "marker_preserved": after["values"] == before["values"],
                "ttl_ms_after": ttl_ms,
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
            "id": "RDS-001",
            "status": "passed" if cells and all(c["status"] == "passed" for c in cells) else "failed",
            "backend": f"Redis {redis_version}; real JSONL SDK workers",
            "endpoint": endpoint,
            "redis_version": redis_version,
            "namespace": namespace,
            "epoch": epoch,
            "real_processes": True,
            "real_backend": True,
            "cells": cells,
            "run_directory": str(run_dir),
            "limitations": [
                "Uses one disposable Redis authority and an intentionally malformed list at each canonical lock key.",
                "Does not test every Redis error class, failover or production Redis configuration.",
                "Malformed keys are left untouched with a finite TTL and unique namespace for forensic inspection.",
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return int(not cells or any(c["status"] != "passed" for c in cells))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--sdks", nargs="+", default=["dotnet", "python", "cpp"],
                        choices=["dotnet", "cpp", "python", "typescript", "go", "rust", "java"])
    parser.add_argument("--output", default="validation/implementation/m6-redis-wrong-type.json")
    parser.add_argument("--workers", default="conformance/workers.json",
                        help="JSONL worker manifest (defaults to conformance/workers.json)")
    args = parser.parse_args()
    return asyncio.run(run(args.endpoint, args.sdks, ROOT / args.output, ROOT / args.workers))


if __name__ == "__main__":
    raise SystemExit(main())
