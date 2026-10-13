"""Live current-source consumer used by etcd_unlock_ack_docker.py."""

from __future__ import annotations

import asyncio
import json
import os
from urllib.request import urlopen

from dilock import Options, ReleaseOutcome, State
from dilock import etcd as etcd_module


def proxy_stats(url: str) -> dict[str, int]:
    with urlopen(url, timeout=3) as response:
        return json.load(response)


async def run() -> dict[str, object]:
    endpoint = os.environ["DILOCK_ETCD_ENDPOINT"]
    namespace = os.environ["DILOCK_ETCD_NAMESPACE"]
    epoch = os.environ["DILOCK_ETCD_EPOCH"]
    expected = ReleaseOutcome.UNKNOWN if os.environ.get("DILOCK_DROP_REVOKE_ACK") == "1" else ReleaseOutcome.ALREADY_NOT_OWNED
    provider = etcd_module.EtcdProvider(endpoint, namespace, epoch)
    owner_key: bytes | None = None
    try:
        handle = provider.lock("same-resource")
        options = Options(lease=8, rpc=1, cleanup=2, auto_renew=False)
        owner = await handle.try_acquire(options)
        assert owner is not None, "owner acquisition failed"
        owner_key = owner._key
        owner_fence = owner.fence[1]

        outcome = await owner.release()
        assert outcome is expected, (outcome, expected)
        expected_state = State.RELEASED_UNKNOWN if expected is ReleaseOutcome.UNKNOWN else State.RELEASED
        assert owner.state is expected_state, owner.state
        assert await owner.release() is expected, "release did not return cached terminal outcome"

        successor = await handle.try_acquire(options)
        assert successor is not None, "successor failed to acquire after applied Unlock"
        assert successor.fence[1] > owner_fence, (owner.fence, successor.fence)

        # A second release of the terminal old owner must not issue cleanup
        # against the successor's distinct opaque key or lease.
        assert await owner.release() is expected
        assert successor._key is not None
        successor_key = successor._key
        current = await provider._kv.Range(
            etcd_module.rpc_pb2.RangeRequest(key=successor_key), timeout=1,
        )
        assert current.count == 1 and current.kvs[0].lease == successor._id, current

        successor_outcome = await successor.release()
        assert successor_outcome is ReleaseOutcome.RELEASED, successor_outcome
        assert (await provider._kv.Range(
            etcd_module.rpc_pb2.RangeRequest(key=successor_key), timeout=1,
        )).count == 0, "successor key remains after its acknowledged release"
        assert owner_key is not None
        assert (await provider._kv.Range(
            etcd_module.rpc_pb2.RangeRequest(key=owner_key), timeout=1,
        )).count == 0, "old owner's native key remains"

        stats = proxy_stats(os.environ["DILOCK_ETCD_PROXY_STATS_URL"])
        expected_revoke_ack = 1 if expected is ReleaseOutcome.UNKNOWN else 2
        expected_revoke_withheld = 1 if expected is ReleaseOutcome.UNKNOWN else 0
        assert stats["unlock_requests"] == 2, stats
        assert stats["unlock_applied"] == 2 and stats["unlock_withheld"] == 1, stats
        assert stats["revoke_requests"] == 2 and stats["revoke_applied"] == 2, stats
        assert stats["revoke_acknowledged"] == expected_revoke_ack, stats
        assert stats["revoke_withheld"] == expected_revoke_withheld, stats
        return {
            "status": "PASS",
            "release": outcome.value,
            "repeat_release": expected.value,
            "successor": "acquired",
            "successor_fence_increased": True,
            "successor_preserved_after_old_release": True,
            "successor_release": successor_outcome.value,
            "proxy": stats,
        }
    finally:
        await provider.aclose()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run()), sort_keys=True))
