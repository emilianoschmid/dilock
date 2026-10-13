#!/usr/bin/env python3
"""RESP fault proxy for Redis late SET replies and applied-command ACK loss."""

from __future__ import annotations

import json
import os
import socket
import socketserver
import threading
import time


UPSTREAM_HOST = "redis"
UPSTREAM_PORT = 6379
PROXY_PORT = 6379
CONTROL_PORT = 9100
lock = threading.Lock()
armed = False
eval_count = 0
dropped_count = 0
delayed_set_count = 0
last_reply = b""
last_script = b""
last_key = b""


def read_line(sock: socket.socket) -> bytes:
    result = bytearray()
    while not result.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise EOFError("connection closed while reading RESP line")
        result.extend(chunk)
    return bytes(result)


def read_request(sock: socket.socket) -> tuple[bytes, list[bytes]]:
    raw = bytearray()
    header = read_line(sock)
    raw.extend(header)
    if not header.startswith(b"*"):
        raise ValueError("expected RESP array command")
    count = int(header[1:-2])
    args: list[bytes] = []
    for _ in range(count):
        size_line = read_line(sock)
        raw.extend(size_line)
        if not size_line.startswith(b"$"):
            raise ValueError("expected RESP bulk command argument")
        size = int(size_line[1:-2])
        if size < 0:
            args.append(b"")
            continue
        value = bytearray()
        while len(value) < size + 2:
            chunk = sock.recv(size + 2 - len(value))
            if not chunk:
                raise EOFError("connection closed while reading RESP bulk argument")
            value.extend(chunk)
        raw.extend(value)
        args.append(bytes(value[:size]))
    return bytes(raw), args


def read_response(sock: socket.socket) -> bytes:
    first = sock.recv(1)
    if not first:
        raise EOFError("upstream closed before the Redis reply")
    if first in (b"+", b"-", b":", b"_"):
        return first + read_line(sock)
    if first in (b"$", b"!"):
        line = read_line(sock)
        size = int(line[:-2])
        if size < 0:
            return first + line
        body = bytearray()
        while len(body) < size + 2:
            chunk = sock.recv(size + 2 - len(body))
            if not chunk:
                raise EOFError("upstream closed during Redis bulk reply")
            body.extend(chunk)
        return first + line + bytes(body)
    if first in (b"*", b"~", b">", b"%"):
        line = read_line(sock)
        count = int(line[:-2])
        if count < 0:
            return first + line
        items = count * (2 if first == b"%" else 1)
        return first + line + b"".join(read_response(sock) for _ in range(items))
    raise ValueError(f"unsupported RESP reply type {first!r}")


def stats() -> dict[str, object]:
    with lock:
        return {
            "armed": armed,
            "eval_count": eval_count,
            "dropped_count": dropped_count,
            "delayed_set_count": delayed_set_count,
            "last_reply": last_reply.decode("ascii", errors="replace"),
            "last_script": last_script.decode("utf-8", errors="replace"),
            "last_key": last_key.decode("utf-8", errors="replace"),
        }


class ControlHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        global armed
        command = self.rfile.readline(128).strip().upper()
        if command == b"ARM":
            with lock:
                armed = True
            response = {"status": "armed"}
        elif command == b"STATS":
            response = stats()
        elif command == b"RESET":
            global eval_count, dropped_count, delayed_set_count, last_reply, last_script, last_key
            with lock:
                armed = False
                eval_count = dropped_count = delayed_set_count = 0
                last_reply = last_script = last_key = b""
            response = {"status": "reset"}
        else:
            response = {"status": "invalid-command"}
        self.wfile.write((json.dumps(response) + "\n").encode())


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        global armed, eval_count, dropped_count, delayed_set_count, last_reply, last_script, last_key
        client = self.request
        upstream: socket.socket | None = None
        try:
            upstream = socket.create_connection((UPSTREAM_HOST, UPSTREAM_PORT), timeout=3)
            client.settimeout(5)
            upstream.settimeout(5)
            while True:
                request, args = read_request(client)
                upstream.sendall(request)
                reply = read_response(upstream)
                is_eval = bool(args) and args[0].upper() == b"EVAL"
                is_lock_acquire = (
                    len(args) >= 5
                    and args[0].upper() == b"SET"
                    and b"NX" in [item.upper() for item in args[2:]]
                    and b"PX" in [item.upper() for item in args[2:]]
                    and reply == b"+OK\r\n"
                )
                if is_eval:
                    with lock:
                        eval_count += 1
                        should_drop = armed
                        if should_drop:
                            armed = False
                            dropped_count += 1
                            last_reply = reply
                            last_script = args[1] if len(args) > 1 else b""
                            last_key = args[3] if len(args) > 3 else b""
                else:
                    should_drop = False
                if is_lock_acquire:
                    delay_ms = int(os.environ.get("DILOCK_REDIS_DELAY_SET_REPLY_MS", "0"))
                    with lock:
                        if delay_ms > 0:
                            delayed_set_count += 1
                        elif os.environ.get("DILOCK_REDIS_DELAY_RENEW_REPLY_MS", "0") == "0":
                            armed = True
                    if delay_ms > 0:
                        # Redis has already applied SET. Delay only its success
                        # response so the SDK can cross its caller wait deadline.
                        time.sleep(delay_ms / 1000)
                if is_eval and b"PEXPIRE" in args[1] and reply == b":1\r\n":
                    # Delay an applied renewal ACK across the previous local
                    # window while keeping the newly extended server TTL alive.
                    time.sleep(int(os.environ.get("DILOCK_REDIS_DELAY_RENEW_REPLY_MS", "0")) / 1000)
                if should_drop:
                    # The complete server reply has been consumed, proving the
                    # command ran; close without forwarding that acknowledgement.
                    return
                client.sendall(reply)
        except (EOFError, OSError, ValueError):
            return
        finally:
            if upstream is not None:
                upstream.close()


class ThreadedTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with ThreadedTCPServer(("0.0.0.0", PROXY_PORT), ProxyHandler) as proxy, ThreadedTCPServer(
        ("0.0.0.0", CONTROL_PORT), ControlHandler
    ) as control:
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        control.serve_forever()
