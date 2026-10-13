"""Run a bounded contention and successor-fencing example against etcd."""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

from dilock import Options, WaitTimeout
from dilock.etcd import EtcdProvider


async def main() -> None:
    endpoint = os.environ.get("DILOCK_ETCD_ENDPOINT")
    namespace = os.environ.get("DILOCK_NAMESPACE")
    epoch = os.environ.get("DILOCK_EPOCH")
    if not endpoint or not namespace or not epoch:
        raise SystemExit("Set DILOCK_ETCD_ENDPOINT, DILOCK_NAMESPACE and DILOCK_EPOCH for a pre-provisioned etcd namespace.")

    resource = f"docs-example/python-etcd/{uuid.uuid4().hex}"
    options = Options(lease=5, rpc=1, cleanup=1, auto_renew=True, require_fencing=True)
    async with EtcdProvider(endpoint, namespace, epoch) as owner_provider:
        async with EtcdProvider(endpoint, namespace, epoch) as contender_provider:
            owner_lock = owner_provider.lock(resource)
            contender_lock = contender_provider.lock(resource)
            owner = await owner_lock.acquire(wait=3, options=options)
            try:
                first_revision = owner.fence_revision
                if first_revision <= 0:
                    raise RuntimeError("owner fence was not verified")
                print(f"owner: acquired with epoch-scoped create_revision {epoch}/{first_revision}")
                try:
                    await contender_lock.acquire(wait=0.3, options=options)
                except WaitTimeout:
                    print("contender: bounded queue wait timed out under contention")
                else:
                    raise AssertionError("contender acquired while the owner still held the resource")
                outcome = await owner.release()
                if outcome.value != "released":
                    raise RuntimeError(f"owner release was not acknowledged: {outcome.value}")

                successor = await contender_lock.acquire(wait=3, options=options)
                try:
                    successor_revision = successor.fence_revision
                    if successor_revision <= first_revision:
                        raise RuntimeError("successor fencing revision did not advance")
                    print(f"contender: acquired after release with create_revision {successor_revision}")
                    outcome = await successor.release()
                    if outcome.value != "released":
                        raise RuntimeError(f"successor release was not acknowledged: {outcome.value}")
                finally:
                    if successor.state.value not in {"released", "released_unknown", "lost"}:
                        await successor.release()
            finally:
                if owner.state.value not in {"released", "released_unknown", "lost"}:
                    await owner.release()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
