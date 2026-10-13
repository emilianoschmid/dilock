"""Exercise the public Python Redis API against a provisioned Redis authority."""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

from dilock import Options, ReleaseOutcome


async def main() -> None:
    endpoint = os.environ.get("DILOCK_REDIS_URL")
    namespace = os.environ.get("DILOCK_NAMESPACE")
    epoch = os.environ.get("DILOCK_EPOCH")
    if not endpoint or not namespace or not epoch:
        raise SystemExit(
            "Set DILOCK_REDIS_URL, DILOCK_NAMESPACE and DILOCK_EPOCH for a "
            "pre-provisioned Redis namespace."
        )

    from dilock.redis import RedisProvider

    resource = f"docs-example/{uuid.uuid4().hex}"
    options = Options(lease=5, rpc=1, cleanup=1)
    successor = None
    async with (
        RedisProvider(endpoint, namespace, epoch) as owner,
        RedisProvider(endpoint, namespace, epoch) as contender,
    ):
        owner_lock = owner.lock(resource)
        contender_lock = contender.lock(resource)
        lease = await owner_lock.try_acquire(options)
        if lease is None:
            raise RuntimeError("unique example resource was unexpectedly busy")
        try:
            busy = await contender_lock.try_acquire(options)
            if busy is not None:
                await busy.release()
                raise AssertionError("a second owner acquired an actively held resource")
            print("contender: acknowledged busy")

            outcome = await lease.release()
            if outcome != ReleaseOutcome.RELEASED:
                raise RuntimeError(f"owner release was not acknowledged: {outcome.value}")

            successor = await contender_lock.try_acquire(options)
            if successor is None:
                raise RuntimeError("contender remained busy after acknowledged release")
            successor.ensure_held()
            print("contender: acquired after owner release")
        finally:
            if successor is not None:
                await successor.release()
            else:
                await lease.release()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
