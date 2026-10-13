"""Small RESP proxy for deterministic Redis fault-injection tests."""
from __future__ import annotations

import asyncio
import argparse
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class RespFrame:
    kind: bytes
    value: object
    raw: bytes


async def read_resp_frame(reader: asyncio.StreamReader) -> RespFrame | None:
    """Read one RESP2/RESP3 frame, preserving its exact wire bytes."""
    try:
        prefix = await reader.readexactly(1)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise
    line = await reader.readuntil(b"\r\n")
    header = prefix + line
    kind = prefix

    if kind in (b"+", b"-", b":", b",", b"#", b"_", b"("):
        value = line[:-2]
        return RespFrame(kind, value, header)

    if kind in (b"$", b"!", b"="):
        size = int(line[:-2])
        if size < 0:
            return RespFrame(kind, None, header)
        payload = await reader.readexactly(size + 2)
        if not payload.endswith(b"\r\n"):
            raise ValueError("malformed RESP bulk string terminator")
        return RespFrame(kind, payload[:-2], header + payload)

    if kind in (b"*", b"~", b">", b"%", b"|"):
        count = int(line[:-2])
        if count < 0:
            return RespFrame(kind, None, header)
        child_count = count * 2 if kind in (b"%", b"|") else count
        children: list[RespFrame] = []
        for _ in range(child_count):
            child = await read_resp_frame(reader)
            if child is None:
                raise EOFError("incomplete RESP aggregate")
            children.append(child)
        value = [child.value for child in children]
        raw = header + b"".join(child.raw for child in children)
        return RespFrame(kind, value, raw)

    raise ValueError(f"unsupported RESP frame prefix {kind!r}")


def command_name(frame: RespFrame) -> bytes | None:
    """Return the first array item for a normal RESP command."""
    if frame.kind != b"*" or not isinstance(frame.value, list) or not frame.value:
        return None
    first = frame.value[0]
    if isinstance(first, bytes):
        return first.upper()
    return None


class RedisLostAckProxy:
    """Forward RESP to Redis and drop one successful SET acknowledgement."""

    def __init__(self, upstream_host: str, upstream_port: int, *, host: str = "127.0.0.1") -> None:
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.host = host
        self.server: asyncio.AbstractServer | None = None
        self.armed = False
        self.set_attempts = 0
        self.successful_sets = 0
        self.dropped_acknowledgements = 0
        self.injected = asyncio.Event()
        self.errors: list[str] = []
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def port(self) -> int:
        if self.server is None or not self.server.sockets:
            raise RuntimeError("proxy is not started")
        return int(self.server.sockets[0].getsockname()[1])

    async def start(self) -> RedisLostAckProxy:
        self.server = await asyncio.start_server(self._handle, self.host, 0)
        return self

    def arm(self) -> None:
        self.set_attempts = 0
        self.successful_sets = 0
        self.dropped_acknowledgements = 0
        self.errors.clear()
        self.armed = True

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for writer in tuple(self._writers):
            writer.close()
        await asyncio.gather(
            *(writer.wait_closed() for writer in tuple(self._writers)),
            return_exceptions=True,
        )
        self._writers.clear()

    async def _handle(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(client_writer)
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.upstream_host, self.upstream_port
            )
            self._writers.add(upstream_writer)
            while True:
                request = await read_resp_frame(client_reader)
                if request is None:
                    break
                is_set = command_name(request) == b"SET"
                if is_set:
                    self.set_attempts += 1
                upstream_writer.write(request.raw)
                await upstream_writer.drain()
                response = await read_resp_frame(upstream_reader)
                if response is None:
                    break
                if is_set and response.kind == b"+" and response.value == b"OK":
                    self.successful_sets += 1
                    if self.armed:
                        self.armed = False
                        self.dropped_acknowledgements += 1
                        self.injected.set()
                        break
                client_writer.write(response.raw)
                await client_writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, BrokenPipeError):
            pass
        except Exception as exc:  # surfaced in the evidence instead of being swallowed
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            client_writer.close()
            if upstream_writer is not None:
                upstream_writer.close()
            await asyncio.gather(
                client_writer.wait_closed(),
                *( [upstream_writer.wait_closed()] if upstream_writer is not None else [] ),
                return_exceptions=True,
            )
            self._writers.discard(client_writer)
            if upstream_writer is not None:
                self._writers.discard(upstream_writer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream-host", required=True)
    parser.add_argument("--upstream-port", required=True, type=int)
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--retry-observation-seconds", default=0.3, type=float)
    parsed = parser.parse_args()

    async def run_cli() -> int:
        proxy = await RedisLostAckProxy(
            parsed.upstream_host, parsed.upstream_port, host=parsed.listen_host
        ).start()
        ready = Path(parsed.ready_file)
        result = Path(parsed.result_file)
        stop = Path(parsed.stop_file)
        # Rebind to the selected port through the same public constructor.
        actual_port = proxy.port
        ready.write_text(json.dumps({"port": actual_port}) + "\n", encoding="utf-8")
        proxy.arm()
        try:
            while not proxy.injected.is_set() and not stop.exists():
                await asyncio.sleep(0.025)
            if proxy.injected.is_set():
                await asyncio.sleep(parsed.retry_observation_seconds)
        finally:
            await proxy.close()
            result.write_text(
                json.dumps(
                    {
                        "set_attempts": proxy.set_attempts,
                        "successful_sets": proxy.successful_sets,
                        "dropped_acknowledgements": proxy.dropped_acknowledgements,
                        "errors": proxy.errors,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        return 0

    raise SystemExit(asyncio.run(run_cli()))
