"""Tests for the deterministic RESP acknowledgement-drop proxy."""
from __future__ import annotations

import asyncio

from conformance.redis_fault_proxy import RedisLostAckProxy, command_name, read_resp_frame


def test_resp_array_parser_preserves_set_command_bytes() -> None:
    async def exercise() -> None:
        reader = asyncio.StreamReader()
        expected = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nNX\r\n$2\r\nPX\r\n"
        reader.feed_data(expected)
        reader.feed_eof()
        frame = await read_resp_frame(reader)
        assert frame is not None
        assert frame.raw == expected
        assert command_name(frame) == b"SET"

    asyncio.run(exercise())


def test_proxy_drops_only_the_successful_set_reply_then_recovers() -> None:
    async def exercise() -> None:
        async def fake_redis(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while frame := await read_resp_frame(reader):
                    command = command_name(frame)
                    writer.write(b"+OK\r\n" if command == b"SET" else b"+PONG\r\n")
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        upstream = await asyncio.start_server(fake_redis, "127.0.0.1", 0)
        upstream_port = upstream.sockets[0].getsockname()[1]
        proxy = await RedisLostAckProxy("127.0.0.1", upstream_port).start()
        proxy.arm()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
            writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            await writer.drain()
            assert await read_resp_frame(reader) is None
            await asyncio.wait_for(proxy.injected.wait(), timeout=1)
            writer.close()
            await writer.wait_closed()

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
            writer.write(b"*1\r\n$4\r\nPING\r\n")
            await writer.drain()
            response = await read_resp_frame(reader)
            assert response is not None and response.raw == b"+PONG\r\n"
            writer.close()
            await writer.wait_closed()

            assert proxy.set_attempts == 1
            assert proxy.successful_sets == 1
            assert proxy.dropped_acknowledgements == 1
            assert proxy.errors == []
        finally:
            await proxy.close()
            upstream.close()
            await upstream.wait_closed()

    asyncio.run(exercise())
