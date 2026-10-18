"""Verify Python and TypeScript PostgreSQL routes contend on one profile."""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = ROOT / "sdk" / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def node_command(worker: Path, dsn: str, namespace: str, epoch: str, mode: str) -> list[str]:
    return [
        "node",
        str(worker),
        "--dsn",
        dsn,
        "--namespace",
        namespace,
        "--epoch",
        epoch,
        "--mode",
        mode,
    ]


def run_node(worker: Path, dsn: str, namespace: str, epoch: str, mode: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            node_command(worker, dsn, namespace, epoch, mode),
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            node_command(worker, dsn, namespace, epoch, mode),
            124,
            stdout=exc.stdout or "",
            stderr=f"Node worker timed out after 30 seconds in mode {mode}\n",
        )


async def run_matrix(dsn: str, namespace: str, epoch: str, worker: Path) -> dict[str, object]:
    from dilock.core import AcquireOptions, Options
    from dilock.postgres import PostgresProvider
    from tools.provision_postgres import provision_postgres

    provision_postgres(dsn, namespace, epoch)
    checks: list[str] = []
    print("cross-language: provisioned", file=sys.stderr, flush=True)

    python_provider = PostgresProvider(dsn, namespace, epoch)
    python_lease = None
    try:
        python_lease = await python_provider.lock("cross-language-resource").acquire(
            AcquireOptions(wait=5.0, lease=Options(lease=5.0, rpc=1.0, cleanup=1.0))
        )
        print("cross-language: Python holds", file=sys.stderr, flush=True)
        contender = run_node(worker, dsn, namespace, epoch, "try")
        print(f"cross-language: Node try returned {contender.returncode}", file=sys.stderr, flush=True)
        if contender.returncode != 0 or contender.stdout.strip() != "BUSY":
            raise AssertionError(f"TypeScript did not observe Python ownership: {contender.stderr or contender.stdout}")
        checks.append("python-holder-typescript-contender")
        await python_lease.release()
        python_lease = None
    finally:
        if python_lease is not None:
            await python_lease.release()
        await python_provider.aclose()

    acquired = run_node(worker, dsn, namespace, epoch, "acquire-release")
    print(f"cross-language: Node acquire returned {acquired.returncode}", file=sys.stderr, flush=True)
    if acquired.returncode != 0 or acquired.stdout.splitlines()[-2:] != ["NODE_ACQUIRED", "NODE_RELEASED"]:
        raise AssertionError(f"TypeScript acquire/release failed: {acquired.stderr or acquired.stdout}")
    checks.append("typescript-acquire-release")

    node_holder = subprocess.Popen(
        node_command(worker, dsn, namespace, epoch, "hold"),
        cwd=ROOT,
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )
    try:
        ready = await asyncio.to_thread(node_holder.stdout.readline)
        print(f"cross-language: Node holder said {ready.strip()}", file=sys.stderr, flush=True)
        if ready.strip() != "READY":
            raise AssertionError(f"TypeScript holder did not become ready: {ready}")
        python_provider = PostgresProvider(dsn, namespace, epoch)
        try:
            if await python_provider.lock("cross-language-resource").try_acquire() is not None:
                raise AssertionError("Python contender unexpectedly acquired the TypeScript-held lock")
            checks.append("typescript-holder-python-contender")
            print("cross-language: Python contender observed Node holder", file=sys.stderr, flush=True)
        finally:
            await python_provider.aclose()
        node_holder.stdin.write("release\n")
        node_holder.stdin.flush()
        released = await asyncio.to_thread(node_holder.stdout.readline)
        print(f"cross-language: Node holder said {released.strip()}", file=sys.stderr, flush=True)
        return_code = await asyncio.to_thread(node_holder.wait)
        stderr = node_holder.stderr.read()
        if return_code != 0 or released.strip() != "RELEASED":
            raise AssertionError(f"TypeScript holder release failed: {stderr or released}")
        checks.append("typescript-session-release")
    finally:
        if node_holder.poll() is None:
            node_holder.kill()
            node_holder.wait()

    return {"status": "passed", "checks": checks}


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--namespace", default="ci-postgres-cross-language")
    parser.add_argument("--epoch", default="ci-postgres-cross-language-epoch")
    parser.add_argument("--node-worker", type=Path, default=ROOT / "conformance" / "postgres_cross_language_node.mjs")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = asyncio.run(run_matrix(args.dsn, args.namespace, args.epoch, args.node_worker))
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
