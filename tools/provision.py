"""Explicit namespace administration, never invoked by ordinary SDK acquisition."""
import argparse
import asyncio
import json

from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry


async def provision(endpoint, namespace, epoch):
    from dilock import identity
    if not isinstance(namespace, str) or not isinstance(epoch, str):
        raise ValueError("namespace and epoch must be strings")
    identity(namespace, "validation")
    if not epoch or len(epoch.encode()) > 1024:
        raise ValueError("invalid epoch")
    metadata = dict(protocol_major=1, identity_scheme="sha256-framed-utf8-v1",
                    profile="redis-single-authority-v1", coordination_epoch=epoch,
                    fixed_lease_ms=None)
    async with Redis.from_url(endpoint, retry=Retry(NoBackoff(), 0),
                              socket_timeout=2, socket_connect_timeout=2) as redis:
        key = f"dilock:namespace:{namespace}:meta"
        created = await redis.set(key, json.dumps(metadata), nx=True)
        raw = await redis.get(key)
        if not raw or len(raw) > 8192 or json.loads(raw) != metadata:
            raise RuntimeError("metadata conflict; existing value was not modified")
        print("created" if created else "compatible existing metadata")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--epoch", required=True)
    args = parser.parse_args()
    asyncio.run(provision(args.endpoint, args.namespace, args.epoch))
