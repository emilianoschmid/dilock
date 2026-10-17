"""Consul HTTP/KV session quickstart."""
from __future__ import annotations

import asyncio
import os

from dilock import AcquireOptions, Options
from dilock.consul import ConsulProvider


async def main() -> None:
    endpoint = os.environ.get("DILOCK_CONSUL_ENDPOINT", "http://127.0.0.1:28500")
    namespace = os.environ.get("DILOCK_NAMESPACE", "quickstart")
    epoch = os.environ.get("DILOCK_EPOCH")
    if not epoch:
        raise RuntimeError("DILOCK_EPOCH must name the approved coordination epoch")
    policy = AcquireOptions(
        wait=5.0,
        lease=Options(lease=10.0, rpc=2.0, cleanup=2.0,
                      auto_renew=True, require_fencing=True),
    )
    async with ConsulProvider(
        endpoint, namespace, epoch, token=os.environ.get("CONSUL_HTTP_TOKEN"),
        default_acquisition=policy,
    ) as provider:
        async with provider.lock("reports/daily").hold() as lease:
            print(f"Protected operation started with fence {lease.fence[0]}:{lease.fence[1]}.")


if __name__ == "__main__":
    asyncio.run(main())
