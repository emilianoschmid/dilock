"""Run two independent Scala test workers against a disposable local backend."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from conformance.run import Worker, expect, pair, spawn_process


async def provision(backend: str, endpoint: str, namespace: str, epoch: str) -> None:
    if backend == "redis":
        from tools.provision import provision as provision_redis

        await provision_redis(endpoint, namespace, epoch)
    else:
        from tools.provision_etcd import provision_etcd

        provision_etcd(endpoint, namespace, epoch)


async def main(args: argparse.Namespace) -> int:
    namespace = "conformance-m8-scala-" + uuid.uuid4().hex
    epoch = "local-disposable-m8-scala-1"
    run_dir = ROOT / "validation" / "implementation" / ("run-m8-scala-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "config.json"
    config.write_text(json.dumps(dict(backend=args.backend, endpoint=args.endpoint,
                                     namespace=namespace, epoch=epoch)), encoding="utf-8")
    result: dict[str, object] = {"sdk": "scala", "backend": args.backend, "status": "not_run"}
    workers: list[Worker] = []
    logs = []
    try:
        await provision(args.backend, args.endpoint, namespace, epoch)
    except Exception as exc:
        result.update(status="failed", phase="provision",
                      error=f"{type(exc).__name__}: {str(exc) or 'provision failed'}")
        return await finish(args, namespace, workers, logs, run_dir, result)

    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    pair.backend = args.backend
    for index in range(2):
        try:
            log_path = run_dir / f"scala-{index}.stderr.log"
            log = log_path.open("wb")
            logs.append(log)
            argv = manifest["scala"]["argv"] + [str(config)]
            process = await spawn_process(*argv, cwd=ROOT, env=os.environ.copy(),
                                          stdin=asyncio.subprocess.PIPE,
                                          stdout=asyncio.subprocess.PIPE, stderr=log,
                                          start_new_session=(os.name != "nt"), popen_on_windows=True)
            worker = Worker(process)
            workers.append(worker)
            hello = expect(await worker.request("hello", timeout=60), "ok")
            worker.capabilities = hello.get("capabilities", {})
        except Exception as exc:
            result.update(status="failed", phase="worker_startup",
                          error=f"{type(exc).__name__}: {str(exc) or 'worker failed before hello'}",
                          stderr_log=str(run_dir / f"scala-{index}.stderr.log"))
            return await finish(args, namespace, workers, logs, run_dir, result)

    started = time.monotonic()
    try:
        result.update(status="passed", scenarios=await pair(workers[0], workers[1], "scala-self-pair"))
    except Exception as exc:
        result.update(status="failed", phase="lifecycle", error=str(exc))
    result["seconds"] = round(time.monotonic() - started, 3)
    return await finish(args, namespace, workers, logs, run_dir, result)


async def finish(args: argparse.Namespace, namespace: str, workers: list[Worker], logs,
                 run_dir: Path, result: dict[str, object]) -> int:
    for worker in workers:
        try:
            await worker.close()
        except (OSError, RuntimeError, asyncio.TimeoutError, ConnectionError) as exc:
            if result.get("status") == "passed":
                result.update(status="failed", phase="cleanup", error=str(exc))
    for log in logs:
        log.close()
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"backend": args.backend, "endpoint": args.endpoint,
                                  "namespace": namespace, "real_processes": len(workers) == 2,
                                  "real_backend": True, "results": [result],
                                  "run_directory": str(run_dir)}, indent=2) + "\n", encoding="utf-8")
    return int(result.get("status") != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("redis", "etcd"), required=True)
    parser.add_argument("--workers", default="sdk/scala/conformance/workers.json")
    parser.add_argument("--endpoint", required=True, help="host-loopback endpoint for metadata provisioning")
    parser.add_argument("--output", required=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
