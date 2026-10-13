"""etcd quickstart: native Lock and async context managers."""
from __future__ import annotations

import asyncio
import logging
import os
import sys

from dilock import AcquireOptions, CleanupObservation, Options
from dilock.etcd import EtcdProvider


def observe_cleanup(observation: CleanupObservation) -> None:
    logging.warning(
        "cleanup backend=%s outcome=%s reason=%s",
        observation.backend, observation.outcome.value, observation.reason,
    )


async def main() -> None:
    endpoint = os.environ.get("DILOCK_ETCD_ENDPOINT", "http://127.0.0.1:2379")
    epoch = os.environ.get("DILOCK_EPOCH")
    if not epoch:
        raise RuntimeError("DILOCK_EPOCH must name the approved coordination epoch")
    namespace = os.environ.get("DILOCK_NAMESPACE", "default")
    configured = "--configured" in sys.argv
    shared_provider = "--shared-provider" in sys.argv
    policy = AcquireOptions(
        wait=10.0,
        lease=Options(lease=60.0, rpc=2.0, cleanup=2.0,
                      auto_renew=False, require_fencing=True),
    )
    async with EtcdProvider(
        endpoint, namespace, epoch,
        default_acquisition=policy if configured else AcquireOptions(),
        cleanup_observer=observe_cleanup,
    ) as provider:
        daily = provider.lock("reports/daily")
        if shared_provider:
            weekly = provider.lock("reports/weekly")
            async with daily.hold():
                async with weekly.hold():
                    print("Two distinct etcd resources share one provider and gRPC channel.")
        elif configured:
            async with daily.hold(policy):
                print("One etcd resource is held with the complete explicit policy.")
        else:
            async with daily.hold():
                print("One short protected etcd operation completed.")


if __name__ == "__main__":
    asyncio.run(main())
