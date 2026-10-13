"""Exercise the test-only C ABI worker over the shared JSONL contract."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[4]
WORKER_BINARY = os.environ.get("DILOCK_C_WORKER_BINARY", "/tmp/dilock-c-conformance-worker")
WORKER_LD_LIBRARY_PATH = os.environ.get(
    "DILOCK_C_WORKER_LD_LIBRARY_PATH",
    "/tmp/c-m8-prefix/lib:/workspace/.tools/cppinstall/lib:"
    "/workspace/.tools/grpc-root/usr/lib/x86_64-linux-gnu:"
    "/workspace/.tools/cppdeps/usr/lib/x86_64-linux-gnu:"
    "/workspace/.tools/openssl-runtime/usr/lib/x86_64-linux-gnu:"
    "/usr/lib/x86_64-linux-gnu",
)


class LiveCWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def _exercise(self, backend: str, config: dict[str, object]) -> None:
        container = os.environ.get("DILOCK_C_WORKER_CONTAINER")
        self.assertTrue(container, "DILOCK_C_WORKER_CONTAINER must name a disposable container")
        config_path = ROOT / "validation/implementation" / f"c-worker-{backend}-{uuid.uuid4().hex}.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        process: asyncio.subprocess.Process | None = None
        try:
            relative = config_path.relative_to(ROOT).as_posix()
            process = await asyncio.create_subprocess_exec(
                "docker", "exec", "-i", "-e",
                f"LD_LIBRARY_PATH={WORKER_LD_LIBRARY_PATH}",
                container, WORKER_BINARY, f"/workspace/{relative}",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert process.stdin is not None and process.stdout is not None

            async def request(request_id: str, op: str, **fields: object) -> dict[str, object]:
                payload = json.dumps({"id": request_id, "op": op, **fields}) + "\n"
                process.stdin.write(payload.encode())
                await process.stdin.drain()
                line = await asyncio.wait_for(process.stdout.readline(), timeout=8)
                self.assertTrue(line, "C conformance worker exited before replying")
                return json.loads(line)

            hello = await request("hello", "hello")
            self.assertEqual(hello["status"], "ok")
            self.assertEqual(hello["sdk"], "c")

            acquired = await request("acquire", "acquire", resource=f"worker/{backend}",
                                     wait_ms=2000, lease_ms=3000)
            self.assertEqual(acquired["status"], "acquired", acquired)
            lease_id = acquired["lease_id"]
            self.assertTrue(isinstance(lease_id, str) and lease_id)
            self.assertGreaterEqual(acquired["effective_lease_ms"], 3000)
            self.assertEqual(acquired["effective_lease_ms"] % 1000, 0)
            if backend == "etcd":
                fence = acquired.get("fence")
                self.assertIsInstance(fence, dict)
                self.assertEqual(fence["epoch"], config["epoch"])
                self.assertGreater(int(fence["value"]), 0)
                self.assertEqual(await request("renew", "renew", lease_id=lease_id,
                                               lease_ms=3000),
                                 {"id": "renew", "status": "unsupported_capability",
                                  "capability": "manual_renewal"})
            else:
                self.assertEqual(await request("renew", "renew", lease_id=lease_id,
                                               lease_ms=3000),
                                 {"id": "renew", "status": "renewed"})

            self.assertEqual(await request("inspect", "inspect", lease_id=lease_id),
                             {"id": "inspect", "status": "held"})
            self.assertEqual(await request("release", "release", lease_id=lease_id),
                             {"id": "release", "status": "released"})
            state = await request("lost", "inspect", lease_id=lease_id)
            self.assertEqual(state, {"id": "lost", "status": "lost"})
            self.assertEqual(await request("repeat", "release", lease_id=lease_id),
                             {"id": "repeat", "status": "released"})
            self.assertEqual(await request("shutdown", "shutdown"),
                             {"id": "shutdown", "status": "ok",
                              "outstanding_cleanup_attempts": 0})
            process.stdin.close()
            self.assertEqual(await asyncio.wait_for(process.wait(), timeout=8), 0)
            stderr = await process.stderr.read() if process.stderr is not None else b""
            self.assertEqual(stderr, b"")
        finally:
            if process is not None and process.returncode is None:
                if process.stdin is not None:
                    process.stdin.close()
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                if process.stderr is not None:
                    await process.stderr.read()
            config_path.unlink(missing_ok=True)

    async def test_live_redis_worker(self) -> None:
        await self._exercise("redis", {
            "backend": "redis",
            "endpoint": "redis://127.0.0.1:6379",
            "namespace": "default",
            "epoch": "installed-consumer-epoch",
        })

    async def test_live_etcd_worker(self) -> None:
        await self._exercise("etcd", {
            "backend": "etcd",
            "endpoint": "http://etcd:2379",
            "namespace": "installed-consumer-etcd",
            "epoch": "installed-consumer-etcd-epoch",
        })


if __name__ == "__main__":
    unittest.main()
