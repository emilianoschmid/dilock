"""Real-process directed pair tests. No lock algorithm lives in this harness."""
from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from conformance.process_control import resume_process_tree, suspend_process_tree

WORKER_SHUTDOWN_ACK_SECONDS = 5
WORKER_EXIT_SECONDS = 5
WORKER_READER_DRAIN_SECONDS = 2


class _PopenAsyncReader:
    """Async facade over a blocking Popen pipe, drained by one dedicated thread."""

    def __init__(self, stream):
        self.stream = stream
        self.loop = asyncio.get_running_loop()
        self.queue: asyncio.Queue[bytes | None | BaseException] = asyncio.Queue()
        self.thread = threading.Thread(target=self._pump, name="dilock-popen-reader", daemon=True)
        self.thread.start()

    def _pump(self) -> None:
        try:
            while chunk := self.stream.readline():
                self.loop.call_soon_threadsafe(self.queue.put_nowait, chunk)
        except BaseException as exc:
            try:
                self.loop.call_soon_threadsafe(self.queue.put_nowait, exc)
            except RuntimeError:
                return
        finally:
            try:
                self.loop.call_soon_threadsafe(self.queue.put_nowait, None)
            except RuntimeError:
                pass

    async def readline(self) -> bytes:
        item = await self.queue.get()
        if isinstance(item, BaseException):
            raise item
        return b"" if item is None else item

    async def read(self) -> bytes:
        chunks = []
        while chunk := await self.readline():
            chunks.append(chunk)
        return b"".join(chunks)


class _PopenAsyncWriter:
    """Buffer small JSONL requests and flush them off the event loop thread."""

    def __init__(self, stream, process_exited=None):
        self.stream = stream
        self.process_exited = process_exited
        self.pending = bytearray()
        self.closing = False

    def write(self, data: bytes) -> None:
        if self.closing:
            raise BrokenPipeError("worker stdin is closed")
        self.pending.extend(data)

    async def drain(self) -> None:
        if not self.pending:
            return
        data = bytes(self.pending)
        self.pending.clear()
        await asyncio.to_thread(self._write, data)

    def _write(self, data: bytes) -> None:
        self.stream.write(data)
        self.stream.flush()

    def close(self) -> None:
        if not self.closing:
            self.closing = True
            try:
                self.stream.close()
            except OSError:
                # On Windows, a child that exits between the shutdown check
                # and pipe close can invalidate the parent's stdin handle.
                # Teardown must preserve the worker's original stderr/result.
                if self.process_exited is None or not self.process_exited():
                    raise

    def is_closing(self) -> bool:
        return self.closing


class WindowsPopenProcess:
    """Popen child with asyncio-compatible JSONL pipes for Windows hosts."""

    def __init__(self, process: subprocess.Popen):
        self._process = process
        self.pid = process.pid
        self.stdin = (
            _PopenAsyncWriter(process.stdin, lambda: process.poll() is not None)
            if process.stdin is not None
            else None
        )
        self.stdout = _PopenAsyncReader(process.stdout) if process.stdout is not None else None
        self.stderr = _PopenAsyncReader(process.stderr) if process.stderr is not None else None

    @property
    def returncode(self):
        return self._process.poll()

    async def wait(self) -> int:
        return await asyncio.to_thread(self._process.wait)

    def terminate(self) -> None:
        self._process.terminate()

    def kill(self) -> None:
        self._process.kill()

    async def kill_tree(self) -> bool:
        """Stop this harness-owned process tree using Windows' built-in taskkill."""
        if self.returncode is not None:
            return True

        def taskkill() -> subprocess.CompletedProcess:
            return subprocess.run(
                ["taskkill.exe", "/PID", str(self.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=WORKER_EXIT_SECONDS,
            )

        try:
            result = await asyncio.to_thread(taskkill)
            stopped = result.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            stopped = False
        if self.returncode is None:
            self.kill()
        await asyncio.wait_for(self.wait(), WORKER_EXIT_SECONDS)
        return stopped


async def spawn_process(*argv, popen_on_windows: bool = False, **kwargs):
    """Start a child, using Popen only for the Windows callers that opt in."""
    if popen_on_windows and os.name == "nt":
        kwargs.pop("start_new_session", None)
        return WindowsPopenProcess(subprocess.Popen(argv, **kwargs))
    return await asyncio.create_subprocess_exec(*argv, **kwargs)


class Worker:
    def __init__(self, process):
        self.process = process
        self.capabilities = {}
        self.responses = {}
        self.counter = 0
        self.reader = asyncio.create_task(self.read())

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                response = json.loads(line)
                future = self.responses.pop(response.get("id"), None)
                if future is not None and not future.done():
                    future.set_result(response)
        except ConnectionError:
            # An intentionally killed worker can reset its stdout pipe on some
            # Windows asyncio transports. Treat that as EOF; pending requests
            # below still fail explicitly instead of becoming contention.
            pass
        for future in self.responses.values():
            if not future.done():
                future.set_exception(RuntimeError("worker exited without response"))

    async def request(self, op, *, timeout=15, **fields):
        id, future = await self.start_request(op, **fields)
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.responses.pop(id, None)

    async def start_request(self, op, **fields):
        """Send a request and return its id and response future for cancellation tests."""
        self.counter += 1
        id = str(self.counter)
        future = asyncio.get_running_loop().create_future()
        self.responses[id] = future
        self.process.stdin.write((json.dumps(dict(id=id, op=op, **fields)) + "\n").encode())
        try:
            await self.process.stdin.drain()
        except Exception:
            self.responses.pop(id, None)
            raise
        return id, future

    async def drain_reader(self) -> bool:
        """Wait for child stdout EOF, then bound transports that omit EOF."""
        if self.reader.done():
            await asyncio.gather(self.reader, return_exceptions=True)
            return True
        try:
            await asyncio.wait_for(asyncio.shield(self.reader), WORKER_READER_DRAIN_SECONDS)
            return True
        except asyncio.TimeoutError:
            # This is safe only after the child has exited: there can be no
            # further JSONL response to consume from its stdout pipe.
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
            return False

    async def kill_tree(self) -> bool:
        """Stop the worker and any SDK process launched by its wrapper."""
        if self.process.returncode is not None:
            return True
        if hasattr(self.process, "kill_tree"):
            tree_stopped = await self.process.kill_tree()
        elif os.name == "nt":
            killer = await asyncio.create_subprocess_exec(
                "taskkill.exe", "/PID", str(self.process.pid), "/T", "/F",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(killer.wait(), WORKER_EXIT_SECONDS)
            except asyncio.TimeoutError:
                killer.kill()
                await killer.wait()
            tree_stopped = killer.returncode == 0
        else:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
                tree_stopped = True
            except ProcessLookupError:
                tree_stopped = self.process.returncode is not None
        if self.process.returncode is None:
            self.process.kill()
        await asyncio.wait_for(self.process.wait(), WORKER_EXIT_SECONDS)
        return tree_stopped

    def suspend_tree(self) -> int:
        """Suspend the worker and its SDK child until ``resume_tree`` is called."""
        return suspend_process_tree(self.process.pid)

    def resume_tree(self) -> int:
        """Resume the process tree previously suspended by ``suspend_tree``."""
        return resume_process_tree(self.process.pid)

    async def close(self):
        try:
            if self.process.returncode is None:
                try:
                    await self.request("shutdown", timeout=WORKER_SHUTDOWN_ACK_SECONDS)
                except (OSError, RuntimeError, asyncio.TimeoutError, BrokenPipeError, ConnectionError):
                    # Preserve the original startup/scenario failure. The stderr
                    # log records why a worker exited without acknowledging shutdown.
                    pass
                if self.process.stdin is not None and not self.process.stdin.is_closing():
                    self.process.stdin.close()
                try:
                    await asyncio.wait_for(self.process.wait(), WORKER_EXIT_SECONDS)
                except asyncio.TimeoutError:
                    await self.kill_tree()
        finally:
            if self.process.returncode is None:
                await self.kill_tree()
            await self.drain_reader()


def expect(response, status):
    if response.get("status") != status:
        raise AssertionError(f"expected {status}, received {response}")
    return response


def capability_enabled(capabilities, name):
    """Read both protocol capability maps and legacy string-list advertisements."""
    if isinstance(capabilities, dict):
        return capabilities.get(name) is True
    if isinstance(capabilities, list):
        return name in capabilities
    return False


def expired_stale_release_status_allowed(backend, status):
    """Accept an unknown etcd release after expiry only if successor stays held."""
    allowed = {"already_not_owned", "released"}
    if backend == "etcd":
        # Unlock/Revoke acknowledgements can be ambiguous after the old lease
        # expires. The caller still verifies below that the successor remains
        # held, so this does not weaken owner-isolation assertions.
        allowed.add("release_unknown")
    return status in allowed


async def pair(holder, contender, label):
    resource = "pair/" + label + "/" + uuid.uuid4().hex
    # Java's native Redis client reserves a 2s RPC budget. Keep the shared
    # lease above that timeout while preserving margin for conservative clients.
    async def attempt(worker, ttl=2500):
        return await worker.request("try_acquire", resource=resource, lease_ms=ttl,
                                    auto_renew=False)
    if getattr(pair, "backend", "redis") == "etcd":
        def acquired(response, stage):
            if response.get("status") != "acquired":
                raise AssertionError(f"{stage}: expected acquired, received {response}")
            return response

        # Keep the requested TTL above Java's fixed 2s RPC budget while still
        # exercising integer-second expiry on etcd.
        async def queue(worker, wait_ms=5000, ttl=2500):
            request = dict(resource=resource, wait_ms=wait_ms, lease_ms=ttl)
            # Native etcd leases manage keepalive for their entire lock
            # lifecycle. Omit this optional Redis-oriented switch so workers
            # that cannot disable native keepalive can still join the common
            # lifecycle pair scenarios.
            return await worker.request("acquire", **request)
        a = acquired(await queue(holder), "initial holder acquire")
        waiting = asyncio.create_task(queue(contender))
        # Let the contender's native Lock RPC reach etcd before releasing the
        # predecessor. The response-pending check alone can pass while a slow
        # worker is still establishing its gRPC channel.
        await asyncio.sleep(0.5)
        if waiting.done():
            raise AssertionError(f"etcd contender did not remain queued: {await waiting}")
        expect(await holder.request("release", lease_id=a["lease_id"]), "released")
        b = acquired(await waiting, "handoff to first contender")
        repeated = await holder.request("release", lease_id=a["lease_id"])
        if repeated.get("status") not in ("released", "already_not_owned"):
            raise AssertionError(f"repeat release was not idempotent: {repeated}")
        waiting = asyncio.create_task(queue(holder))
        await asyncio.sleep(0.5)
        if waiting.done():
            raise AssertionError(f"successor etcd contender did not remain queued: {await waiting}")
        expect(await contender.request("release", lease_id=b["lease_id"]), "released")
        a = acquired(await waiting, "handoff back to original holder")
        expect(await holder.request("release", lease_id=a["lease_id"]), "released")

        if capability_enabled(getattr(holder, "capabilities", {}), "auto_renew_mandatory"):
            # Some native etcd APIs always keep the lease alive while the
            # owner is active. Verify that this safety behavior survives the
            # nominal TTL, then prove a repeated old-owner release cannot
            # disturb the next owner. Expiry-while-alive is inapplicable here.
            a = acquired(await queue(holder, 5000, 2500), "initial auto-renewing holder acquire")
            waiting = asyncio.create_task(queue(contender))
            await asyncio.sleep(3.5)
            if waiting.done():
                raise AssertionError(
                    f"mandatory etcd keepalive did not preserve ownership past TTL: {await waiting}"
                )
            expect(await holder.request("release", lease_id=a["lease_id"]), "released")
            b = acquired(await waiting, "handoff after mandatory auto-renew")
            repeated = await holder.request("release", lease_id=a["lease_id"])
            if repeated.get("status") not in ("released", "already_not_owned"):
                raise AssertionError(f"repeat release was not idempotent: {repeated}")
            waiting = asyncio.create_task(queue(holder))
            await asyncio.sleep(0.5)
            if waiting.done():
                raise AssertionError(f"stale etcd release disturbed the successor: {await waiting}")
            expect(await contender.request("release", lease_id=b["lease_id"]), "released")
            a = acquired(await waiting, "handoff after stale auto-renewed release check")
            expect(await holder.request("release", lease_id=a["lease_id"]), "released")
            return ["contention", "handoff", "repeat_release",
                    "auto_renew_past_ttl", "stale_release_after_handoff"]

        a = acquired(await queue(holder, 5000, 2500), "initial expiring holder acquire")
        effective_lease_ms = a.get("effective_lease_ms")
        if not isinstance(effective_lease_ms, int) or effective_lease_ms <= 0:
            raise AssertionError(f"initial etcd holder omitted its effective lease duration: {a}")
        # SDKs may reserve part of the granted TTL for the acquisition wait.
        # Expiry must be tested against the lease the holder actually received.
        await asyncio.sleep(max(3.5, effective_lease_ms / 1_000 + 0.25))
        b = acquired(await queue(contender), "acquire after expiry")
        old_release = await holder.request("release", lease_id=a["lease_id"])
        if not expired_stale_release_status_allowed(pair.backend, old_release.get("status")):
            raise AssertionError(f"unexpected expired etcd release: {old_release}")
        waiting = asyncio.create_task(queue(holder))
        await asyncio.sleep(0.5)
        if waiting.done():
            raise AssertionError(f"stale etcd release disturbed the successor: {await waiting}")
        expect(await contender.request("release", lease_id=b["lease_id"]), "released")
        a = acquired(await waiting, "handoff after stale release check")
        expect(await holder.request("release", lease_id=a["lease_id"]), "released")
        return ["contention", "handoff", "repeat_release", "expiry", "stale_release"]

    a = expect(await attempt(holder), "acquired")
    expect(await attempt(contender), "busy")
    expect(await holder.request("release", lease_id=a["lease_id"]), "released")
    b = expect(await attempt(contender), "acquired")
    repeated = await holder.request("release", lease_id=a["lease_id"])
    if repeated.get("status") not in ("released", "already_not_owned"):
        raise AssertionError(f"repeat release was not idempotent: {repeated}")
    expect(await attempt(holder), "busy")
    expect(await contender.request("release", lease_id=b["lease_id"]), "released")
    a = expect(await attempt(holder, 2500), "acquired")
    await asyncio.sleep(3.5)
    b = expect(await attempt(contender, 2500), "acquired")
    expect(await holder.request("release", lease_id=a["lease_id"]), "already_not_owned")
    expect(await attempt(holder), "busy")
    expect(await contender.request("release", lease_id=b["lease_id"]), "released")
    return ["contention", "handoff", "repeat_release", "expiry", "stale_release"]


async def main(args):
    namespace = "conformance-" + uuid.uuid4().hex
    run_dir = ROOT / "validation/implementation" / ("run-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config = run_dir / "config.json"
    config.write_text(json.dumps(dict(backend=args.backend, endpoint=args.endpoint,
                                     namespace=namespace, epoch="local-disposable-1")))
    results = []
    if args.backend == "redis":
        from tools.provision import provision

        provision_fn = lambda: provision(args.endpoint, namespace, "local-disposable-1")
    else:
        from tools.provision_etcd import provision_etcd

        provision_fn = lambda: provision_etcd(args.endpoint, namespace, "local-disposable-1")
    try:
        provision_result = provision_fn()
        if asyncio.iscoroutine(provision_result):
            await provision_result
    except Exception as exc:
        results.append(dict(backend=args.backend, phase="provision", status="failed",
                            error=str(exc)))
        evidence = dict(backend=args.backend, endpoint=args.endpoint, namespace=namespace,
                        real_processes=False, real_backend=False, results=results,
                        run_directory=str(run_dir))
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, indent=2) + "\n")
        print(results[-1], flush=True)
        return 1
    manifest = json.loads((ROOT / args.workers).read_text())
    selected = args.sdks or list(manifest)
    workers, logs = {}, []
    pair.backend = args.backend
    startup_failed = False
    try:
        for sdk in selected:
            try:
                argv = manifest[sdk]["argv"] + [str(config)]
                log = (run_dir / (sdk + ".stderr.log")).open("wb")
                logs.append(log)
                process = await spawn_process(*argv, cwd=ROOT, env=os.environ.copy(),
                             stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=log,
                             start_new_session=(os.name != "nt"), popen_on_windows=True)
                workers[sdk] = Worker(process)
                hello = expect(await workers[sdk].request("hello", timeout=60), "ok")
                workers[sdk].capabilities = hello.get("capabilities", {})
            except Exception as exc:
                result = dict(holder=sdk, contender=None, backend=args.backend,
                              phase="worker_startup", status="failed",
                              error=f"{type(exc).__name__}: {str(exc) or 'worker failed before hello'}",
                              stderr_log=str(run_dir / (sdk + ".stderr.log")))
                results.append(result)
                print(result, flush=True)
                startup_failed = True
                break
        for a, b in (() if startup_failed else itertools.permutations(selected, 2)):
            start = time.monotonic()
            try:
                scenarios = await pair(workers[a], workers[b], a + "-" + b)
                result = dict(holder=a, contender=b, backend=args.backend, status="passed",
                              scenarios=scenarios)
            except Exception as exc:
                result = dict(holder=a, contender=b, backend=args.backend, status="failed",
                              error=str(exc))
            result["seconds"] = round(time.monotonic()-start, 3)
            results.append(result)
            print(result, flush=True)
    finally:
        for worker in workers.values():
            await worker.close()
        for log in logs:
            log.close()
        evidence = dict(backend=args.backend, endpoint=args.endpoint, namespace=namespace,
                        real_processes=bool(workers),
                        real_backend=True, results=results, run_directory=str(run_dir))
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(evidence, indent=2) + "\n")
    return int(startup_failed or any(r["status"] != "passed" for r in results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--workers", default="conformance/workers.json")
    parser.add_argument("--backend", default="redis", choices=["redis", "etcd"])
    parser.add_argument("--endpoint")
    parser.add_argument("--output")
    sys.path.insert(0, str(ROOT))
    args = parser.parse_args()
    if args.endpoint is None:
        args.endpoint = "redis://127.0.0.1:16379" if args.backend == "redis" else "http://127.0.0.1:22379"
    if args.output is None:
        args.output = f"validation/implementation/{args.backend}-pairs.json"
    sys.exit(asyncio.run(main(args)))
