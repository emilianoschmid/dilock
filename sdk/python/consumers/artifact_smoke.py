"""Public-API smoke test executed outside the source checkout."""

from __future__ import annotations

import asyncio
import importlib.resources
import sys
from pathlib import Path

import grpc
import redis
from dilock import Options, identity
from dilock.etcd import EtcdProvider
from dilock.redis import RedisProvider


def main() -> None:
    package_file = Path(__import__("dilock").__file__).resolve()
    if "site-packages" not in package_file.parts and "dist-packages" not in package_file.parts:
        raise AssertionError(f"Dilock imported from outside the installed environment: {package_file}")
    if identity("default", "camera/123/reindex") != (
        "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6"
    ):
        raise AssertionError("installed identity API disagrees with the draft 1 vector")
    if Options().ttl_ms != 30_000:
        raise AssertionError("installed immutable options API returned an unexpected default")

    package = importlib.resources.files("dilock")
    for resource in ("py.typed", "redis-release.lua", "redis-renew.lua"):
        if not package.joinpath(resource).is_file():
            raise AssertionError(f"wheel omitted required package resource: {resource}")
    proto = importlib.resources.files("dilock._etcd_proto").joinpath("v3lock.proto")
    if not proto.is_file():
        raise AssertionError("wheel omitted the etcd protocol source resource")

    async def construct_and_close_providers() -> None:
        redis_provider = RedisProvider("redis://127.0.0.1:1", "consumer", "consumer-epoch")
        redis_provider.lock("consumer/resource")
        await redis_provider.aclose()

        etcd_provider = EtcdProvider("http://127.0.0.1:1", "consumer", "consumer-epoch")
        etcd_provider.lock("consumer/resource")
        await etcd_provider.aclose()

    asyncio.run(construct_and_close_providers())
    if not redis.__version__ or not grpc.__version__:
        raise AssertionError("optional backend dependencies were not installed")
    print(f"Installed Dilock consumer passed with {sys.version.split()[0]}.")


if __name__ == "__main__":
    main()
