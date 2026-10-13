"""Stress the three-instance etcd peer-proxy control channel without etcd."""

from __future__ import annotations

import argparse
import asyncio
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# ruff: noqa: E402
from conformance.etcd_peer_partition import WslPeerPartitionProxy


async def rejected_peer_connection(proxy: WslPeerPartitionProxy) -> None:
    if proxy.port is None:
        raise RuntimeError("peer proxy listener is not ready")
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", proxy.port), timeout=2
    )
    try:
        result = await asyncio.wait_for(reader.read(1), timeout=2)
        if result:
            raise AssertionError("partitioned peer proxy forwarded a connection")
    finally:
        writer.close()
        await writer.wait_closed()


async def run_smoke(args: argparse.Namespace) -> dict:
    binary = ROOT / ".tools/m6-etcd-peer-partition-proxy-linux"
    if not binary.is_file():
        raise FileNotFoundError(f"peer proxy binary is missing: {binary}")
    run_dir = ROOT / "validation/implementation" / (
        "etcd-peer-proxy-control-smoke-" + uuid.uuid4().hex
    )
    run_dir.mkdir(parents=True)
    proxies: list[WslPeerPartitionProxy] = []
    cycles = []
    cleanup_errors = []
    error = None
    status = "failed"
    try:
        for index in range(3):
            proxy_dir = run_dir / f"peer-proxy-{index + 1}"
            proxy_dir.mkdir()
            proxy = WslPeerPartitionProxy(
                proxy_dir, args.wsl_distribution, "127.0.0.1:1"
            )
            await proxy.start()
            proxies.append(proxy)

        for cycle in range(args.cycles):
            partitioned = await asyncio.gather(
                *(proxy.control("partition") for proxy in proxies)
            )
            if any(
                response["state"].get("partitioned") is not True
                for response in partitioned
            ):
                raise AssertionError(f"partition acknowledgments disagree: {partitioned}")
            before_blocked = [
                response["state"].get("blocked_connections", 0)
                for response in partitioned
            ]
            await asyncio.gather(*(rejected_peer_connection(proxy) for proxy in proxies))
            blocked_states = await asyncio.gather(
                *(
                    proxy.wait_state(
                        lambda value, floor=floor: value.get("blocked_connections", 0)
                        > floor
                    )
                    for proxy, floor in zip(proxies, before_blocked)
                )
            )
            healed = await asyncio.gather(*(proxy.control("heal") for proxy in proxies))
            if any(
                response["state"].get("partitioned") is not False
                for response in healed
            ):
                raise AssertionError(f"heal acknowledgments disagree: {healed}")
            states = await asyncio.gather(*(proxy.control("status") for proxy in proxies))
            snapshots = [response["state"] for response in states]
            if any(snapshot.get("active_connections") != 0 for snapshot in snapshots):
                raise AssertionError(f"unexpected active connections after cycle: {snapshots}")
            cycles.append(
                {
                    "cycle": cycle + 1,
                    "blocked_connections": [
                        snapshot.get("blocked_connections") for snapshot in blocked_states
                    ],
                    "partition_transitions": [
                        snapshot.get("partition_transitions") for snapshot in snapshots
                    ],
                }
            )
        status = "passed"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        for proxy in proxies:
            try:
                await proxy.control("heal")
            except Exception as exc:
                cleanup_errors.append(f"{proxy.run_dir.name}: {type(exc).__name__}: {exc}")
        results = await asyncio.gather(*(proxy.close() for proxy in proxies), return_exceptions=True)
        for proxy, result in zip(proxies, results):
            if isinstance(result, Exception):
                cleanup_errors.append(f"{proxy.run_dir.name}: {type(result).__name__}: {result}")
        report = {
            "id": "M6-ETCD-PEER-PROXY-CONTROL-THREE-INSTANCE-SMOKE",
            "date": date.today().isoformat(),
            "status": status,
            "scenario": "Three separate local etcd peer-path TCP proxies receive synchronous partition/status/heal requests while rejecting one incoming peer stream per cycle; no etcd process is started.",
            "participants": 3,
            "cycles_requested": args.cycles,
            "cycles_completed": len(cycles),
            "cycles": cycles,
            "proxy_shutdown_results": [
                result if isinstance(result, dict) else None for result in results
            ],
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "run_directory": str(run_dir),
            "error": error,
            "cleanup_errors": cleanup_errors,
            "not_run": ["etcd members", "SDK workers", "quorum-loss behavior"],
            "limitations": [
                "This is a control-channel/proxy smoke only; it does not validate Raft, etcd recovery, or any SDK behavior.",
                "Windows-to-WSL loopback control access is tested in this environment; production network isolation is not represented.",
            ],
        }
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=int, default=50)
    parser.add_argument(
        "--wsl-distribution",
        default=os.environ.get("DILOCK_WSL_DISTRIBUTION", "Ubuntu-26.04"),
    )
    parser.add_argument(
        "--output",
        default="validation/implementation/m6-etcd-peer-proxy-control-smoke.json",
    )
    args = parser.parse_args()
    if args.cycles < 1:
        parser.error("--cycles must be positive")
    report = asyncio.run(run_smoke(args))
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
