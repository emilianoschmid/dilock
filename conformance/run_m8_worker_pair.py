"""Run a real-backend lifecycle pair using two isolated instances of one M8 worker."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from conformance.run import Worker, pair, spawn_process, expect
from tools.provision import provision


async def main(args: argparse.Namespace) -> int:
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    if args.sdk not in manifest:
        raise ValueError(f"SDK is not in worker manifest: {args.sdk}")
    namespace = "conformance-m8-" + uuid.uuid4().hex
    run_dir = ROOT / "validation" / "implementation" / ("run-m8-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "config.json"
    config.write_text(json.dumps(dict(backend="redis", endpoint=args.endpoint,
                                     namespace=namespace, epoch="local-disposable-m8-1")),
                      encoding="utf-8")
    await provision(args.endpoint, namespace, "local-disposable-m8-1")
    pair.backend = "redis"
    workers: list[Worker] = []
    logs = []
    result: dict[str, object] = {"sdk": args.sdk, "backend": "redis", "status": "not_run"}
    try:
        for index in range(2):
            try:
                log_path = run_dir / f"{args.sdk}-{index}.stderr.log"
                log = log_path.open("wb")
                logs.append(log)
                argv = manifest[args.sdk]["argv"] + [str(config)]
                process = await spawn_process(*argv, cwd=ROOT, env=os.environ.copy(),
                                              stdin=asyncio.subprocess.PIPE,
                                              stdout=asyncio.subprocess.PIPE, stderr=log,
                                              start_new_session=(os.name != "nt"), popen_on_windows=True)
                worker = Worker(process)
                workers.append(worker)
                expect(await worker.request("hello", timeout=60), "ok")
            except Exception as exc:
                result.update(status="failed", phase="worker_startup",
                              error=f"{type(exc).__name__}: {str(exc) or 'worker failed before hello'}",
                              stderr_log=str(run_dir / f"{args.sdk}-{index}.stderr.log"))
                break
        if result["status"] != "failed":
            started = time.monotonic()
            try:
                result.update(status="passed", scenarios=await pair(workers[0], workers[1], "m8-" + args.sdk))
            except Exception as exc:
                result.update(status="failed", phase="lifecycle",
                              error=f"{type(exc).__name__}: {str(exc) or 'lifecycle scenario failed'}")
            result["seconds"] = round(time.monotonic() - started, 3)
    finally:
        for worker in workers:
            try:
                await worker.close()
            except (OSError, RuntimeError, asyncio.TimeoutError, ConnectionError):
                if result["status"] != "failed":
                    result.update(status="failed", phase="cleanup",
                                  error="worker cleanup failed; inspect retained stderr log")
        for log in logs:
            log.close()
        report = {"backend": "redis", "namespace": namespace, "real_processes": True,
                  "real_backend": True, "results": [result], "run_directory": str(run_dir)}
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return int(result["status"] != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", default="dart")
    parser.add_argument("--workers", default="conformance/workers-m8.json")
    parser.add_argument("--endpoint", required=True, help="host-loopback endpoint used only for metadata provisioning")
    parser.add_argument("--output", default="validation/implementation/m8-dart-redis-self-pair-20261007.json")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args)))
