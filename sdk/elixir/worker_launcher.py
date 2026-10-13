"""Run the current-source Elixir JSONL worker in a pinned local Docker image."""
from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import threading
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SDK = ROOT / "sdk" / "elixir"
IMAGE = "elixir:1.18.4-otp-27-alpine@sha256:7b0b369edb2f780f3776cfae85fb60f97d6f7c4aada7a1345ec1f0d1d041cb2c"
IMAGE_ID = "sha256:7b0b369edb2f780f3776cfae85fb60f97d6f7c4aada7a1345ec1f0d1d041cb2c"
TOOLS = ROOT / ".tools" / "elixir-hex-package-candidate-20261006"
HEX_ARCHIVE = TOOLS / "hex-2.5.1-installed.tar"
HEX_SHA256 = "8b43e2dc2482183fc8780f917f4c3e25c05d31229022d6abd2de840fc2b503ec"
REBAR = TOOLS / "rebar3"
REBAR_SHA256 = "17b83296517b53ab70f362306de95493151fa63e61bbae8b852769735937d965"


def has_sha256(path: Path, expected: str) -> bool:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest() == expected


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    network = os.environ.get("DILOCK_M8_DOCKER_NETWORK")
    if not network:
        print("DILOCK_M8_DOCKER_NETWORK must name a task-owned Docker network", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).resolve()
    try:
        config.relative_to(ROOT)
    except ValueError:
        print("worker config must be inside the repository for a read-only Docker mount", file=sys.stderr)
        return 2
    if not config.is_file():
        print("worker config does not exist", file=sys.stderr)
        return 2
    if not HEX_ARCHIVE.is_file() or not has_sha256(HEX_ARCHIVE, HEX_SHA256):
        print("the pinned local Hex archive is unavailable or has a different SHA-256", file=sys.stderr)
        return 2
    if not REBAR.is_file() or not has_sha256(REBAR, REBAR_SHA256):
        print("the pinned local rebar3 executable is unavailable or has a different SHA-256", file=sys.stderr)
        return 2

    inspected = subprocess.run(
        ["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, check=False,
    )
    if inspected.returncode or inspected.stdout.strip() != IMAGE_ID:
        print("the pinned local Elixir image is unavailable or has a different image ID", file=sys.stderr)
        return 2

    try:
        value = json.loads(config.read_text(encoding="utf-8"))
        value["endpoint"] = os.environ.get("DILOCK_M8_WORKER_ENDPOINT", value["endpoint"])
    except (OSError, ValueError, KeyError, TypeError):
        print("worker config is invalid", file=sys.stderr)
        return 2

    mapped_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json",
                                         dir=config.parent, delete=False) as mapped:
            mapped_path = Path(mapped.name)
            json.dump(value, mapped)
        trace_args = (
            ["-e", "DILOCK_M8_WORKER_TRACE=1"]
            if os.environ.get("DILOCK_M8_WORKER_TRACE") == "1"
            else []
        )
        trace_args.extend(["-e", "DILOCK_M8_WORKER_READY=1"])
        command = [
            "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
            "-v", f"{SDK.resolve()}:/sdk/elixir:ro",
            "-v", f"{mapped_path.resolve()}:/worker-config.json:ro",
            "-v", f"{HEX_ARCHIVE.resolve()}:/tmp/hex-installed.tar:ro",
            "-v", f"{REBAR.resolve()}:/tmp/rebar3:ro",
            *trace_args,
            IMAGE_ID, "sh", "-c",
            "mkdir -p /root/.mix/archives && tar -xf /tmp/hex-installed.tar -C /root/.mix/archives && "
            "cp /tmp/rebar3 /tmp/rebar3-local && chmod +x /tmp/rebar3-local && "
            "cp -a /sdk/elixir /tmp/dilock-elixir && cd /tmp/dilock-elixir && "
            "export MIX_REBAR3=/tmp/rebar3-local && "
            "mix compile 1>&2 && "
            "mix run --no-compile test_support/dilock-worker.exs /worker-config.json",
        ]
        stdout_log = config.parent / f"docker-stdout-{os.getpid()}.jsonl"
        with stdout_log.open("wb") as capture:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            assert process.stdout is not None

            ready = threading.Event()

            def forward_stdin() -> None:
                assert process.stdin is not None
                try:
                    if not ready.wait(timeout=90):
                        return
                    for request_line in sys.stdin.buffer:
                        process.stdin.write(request_line)
                        process.stdin.flush()
                except (BrokenPipeError, OSError):
                    pass
                finally:
                    try:
                        process.stdin.close()
                    except OSError:
                        pass

            def forward_stderr() -> None:
                assert process.stderr is not None
                try:
                    for diagnostic_line in iter(process.stderr.readline, b""):
                        if b"worker: ready" in diagnostic_line:
                            ready.set()
                        sys.stderr.buffer.write(diagnostic_line)
                        sys.stderr.buffer.flush()
                finally:
                    ready.set()

            stdin_thread = threading.Thread(target=forward_stdin, daemon=True)
            stderr_thread = threading.Thread(target=forward_stderr, daemon=True)
            stdin_thread.start()
            stderr_thread.start()
            for line in iter(process.stdout.readline, b""):
                capture.write(line)
                capture.flush()
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
            result = process.wait()
            stderr_thread.join(timeout=2)
            return result
    finally:
        if mapped_path is not None:
            mapped_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
