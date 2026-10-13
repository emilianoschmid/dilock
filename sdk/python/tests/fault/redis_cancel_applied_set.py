"""Live regression for cancellation after Redis commits SET but delays its ACK."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import uuid
from urllib.parse import urlparse

from redis.asyncio import Redis

from dilock.core import Options, identity
from dilock.redis import RedisProvider


def proxy_stats(control_url: str, command: str) -> dict[str, object]:
    parsed = urlparse(control_url)
    if parsed.scheme != "tcp" or parsed.hostname is None or parsed.port is None:
        raise ValueError("proxy control endpoint must use tcp://host:port")
    with socket.create_connection((parsed.hostname, parsed.port), timeout=2) as client:
        client.sendall((command + "\n").encode())
        response = bytearray()
        while not response.endswith(b"\n"):
            chunk = client.recv(4096)
            if not chunk:
                raise OSError("proxy control endpoint closed before its response")
            response.extend(chunk)
        return json.loads(response)


async def main() -> None:
    proxy_url = os.environ["DILOCK_REDIS_URL"]
    verify_url = os.environ["DILOCK_REDIS_VERIFY_URL"]
    control_url = os.environ["DILOCK_REDIS_PROXY_CONTROL_URL"]
    namespace = "py-cancel-set-" + uuid.uuid4().hex[:20]
    epoch = "py-cancel-set-epoch"
    resource = "cancel-after-applied-set"
    metadata_key = f"dilock:namespace:{namespace}:meta"
    metadata = json.dumps({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "redis-single-authority-v1",
        "coordination_epoch": epoch,
        "fixed_lease_ms": None,
    }, separators=(",", ":"))
    admin = Redis.from_url(verify_url, decode_responses=False)
    await admin.set(metadata_key, metadata, nx=True)
    provider = RedisProvider(proxy_url, namespace, epoch)
    try:
        proxy_stats(control_url, "RESET")
        task = asyncio.create_task(provider.lock(resource).try_acquire(
            Options(lease=5, rpc=2, cleanup=2),
        ))

        deadline = asyncio.get_running_loop().time() + 3
        observed: dict[str, object] = {}
        while asyncio.get_running_loop().time() < deadline:
            observed = await asyncio.to_thread(proxy_stats, control_url, "STATS")
            if observed["delayed_set_count"] == 1:
                break
            await asyncio.sleep(0.005)
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise AssertionError(f"proxy never observed an applied SET: {observed}")

        # The proxy increments this counter only after forwarding SET to Redis
        # and consuming the successful server reply; the client ACK is delayed.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancellation during a committed SET must propagate")

        observed = await asyncio.to_thread(proxy_stats, control_url, "STATS")
        assert observed["delayed_set_count"] == 1, observed
        assert observed["eval_count"] == 1, observed
        assert observed["dropped_count"] == 0, observed
        lock_key = f"dilock:{{{identity(namespace, resource)}}}:lease"
        assert await admin.get(lock_key) is None, "owner-checked cleanup left the applied lock"
        print("PASS: Redis applied one SET, cancellation propagated, one owner cleanup EVAL removed the lock")
    finally:
        await provider.aclose()
        await admin.delete(metadata_key)
        await admin.aclose()


if __name__ == "__main__":
    asyncio.run(main())
