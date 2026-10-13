"""Tests for the RDS-004 delayed acquisition proxy."""

from __future__ import annotations

import asyncio

from conformance.redis_fault_proxy import command_name, read_resp_frame
from conformance.redis_offline_queue import RedisOfflineQueueProxy


def _command(*items: bytes) -> bytes:
    return (
        b"*"
        + str(len(items)).encode()
        + b"\r\n"
        + b"".join(
            b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n" for item in items
        )
    )


def test_proxy_captures_unforwarded_set_and_replay_cannot_replace_successor() -> None:
    async def exercise() -> None:
        owners: dict[bytes, bytes] = {}
        commands: list[tuple[bytes, ...]] = []

        async def fake_redis(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                while frame := await read_resp_frame(reader):
                    assert isinstance(frame.value, list)
                    values = tuple(frame.value)
                    commands.append(values)
                    if command_name(frame) == b"SET":
                        key, value = values[1], values[2]
                        if key in owners:
                            writer.write(b"$-1\r\n")
                        else:
                            owners[key] = value
                            writer.write(b"+OK\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        upstream = await asyncio.start_server(fake_redis, "127.0.0.1", 0)
        upstream_port = int(upstream.sockets[0].getsockname()[1])
        key = b"dilock:{resource}:lease"
        proxy = await RedisOfflineQueueProxy("127.0.0.1", upstream_port, key).start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
            stale = _command(b"SET", key, b"stale-owner", b"NX", b"PX", b"8000")
            writer.write(stale)
            await writer.drain()
            assert await read_resp_frame(reader) is None
            writer.close()
            await writer.wait_closed()
            await asyncio.wait_for(proxy.first_target.wait(), 1)
            assert proxy.set_attempts == 1
            assert commands == []

            reader, writer = await asyncio.open_connection("127.0.0.1", upstream_port)
            writer.write(
                _command(b"SET", key, b"successor-owner", b"NX", b"PX", b"8000")
            )
            await writer.drain()
            assert (await read_resp_frame(reader)).raw == b"+OK\r\n"
            writer.close()
            await writer.wait_closed()

            outcomes = await proxy.replay()
            assert outcomes == ["$:'None'"]
            assert owners[key] == b"successor-owner"
            assert len(commands) == 2
        finally:
            await proxy.close()
            upstream.close()
            await upstream.wait_closed()

    asyncio.run(exercise())


def test_only_exact_target_set_is_held() -> None:
    async def exercise() -> None:
        forwarded = []

        async def fake_redis(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                while frame := await read_resp_frame(reader):
                    forwarded.append(command_name(frame))
                    writer.write(b"+PONG\r\n")
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        upstream = await asyncio.start_server(fake_redis, "127.0.0.1", 0)
        proxy = await RedisOfflineQueueProxy(
            "127.0.0.1", upstream.sockets[0].getsockname()[1], b"target"
        ).start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
            writer.write(_command(b"PING"))
            await writer.drain()
            assert (await read_resp_frame(reader)).raw == b"+PONG\r\n"
            writer.close()
            await writer.wait_closed()
            assert forwarded == [b"PING"]
            assert proxy.set_attempts == 0
            assert proxy.delayed == []
        finally:
            await proxy.close()
            upstream.close()
            await upstream.wait_closed()

    asyncio.run(exercise())
