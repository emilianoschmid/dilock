"""Local evidence formatting checks for the real-process fault runner."""
import asyncio
import time

import pytest

from conformance import nway
from conformance.nway import safe_endpoint
from conformance.run import Worker


def test_safe_endpoint_redacts_password_without_changing_authority() -> None:
    assert safe_endpoint("redis://:a%2Fb@127.0.0.1:16379/0") == "redis://:***@127.0.0.1:16379/0"


def test_safe_endpoint_preserves_username_but_redacts_password() -> None:
    assert safe_endpoint("redis://runner:secret@redis.local:6379") == "redis://runner:***@redis.local:6379"


def test_safe_endpoint_leaves_uncredentialed_urls_unchanged() -> None:
    assert safe_endpoint("http://127.0.0.1:22379") == "http://127.0.0.1:22379"


class _Stdin:
    def __init__(self) -> None:
        self.closed = False

    def is_closing(self) -> bool:
        return self.closed

    def write(self, _data: bytes) -> None:
        pass

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _HangingProcess:
    def __init__(self) -> None:
        self.returncode = None
        self.stdin = _Stdin()
        self.exited = asyncio.Event()
        self.killed = False

    async def wait(self) -> None:
        await self.exited.wait()

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9
        self.exited.set()


def test_worker_close_bounds_shutdown_and_reader_drain(monkeypatch) -> None:
    monkeypatch.setattr("conformance.run.WORKER_SHUTDOWN_ACK_SECONDS", 0.01)
    monkeypatch.setattr("conformance.run.WORKER_EXIT_SECONDS", 0.01)
    monkeypatch.setattr("conformance.run.WORKER_READER_DRAIN_SECONDS", 0.01)

    async def kill_fake_tree(worker: Worker) -> bool:
        worker.process.kill()
        await worker.process.wait()
        return True

    monkeypatch.setattr(Worker, "kill_tree", kill_fake_tree)

    async def exercise() -> None:
        worker = object.__new__(Worker)
        process = _HangingProcess()
        worker.process = process
        worker.responses = {}
        worker.counter = 0
        worker.reader = asyncio.create_task(asyncio.Event().wait())
        await asyncio.wait_for(worker.close(), timeout=0.5)
        assert process.killed
        assert worker.reader.cancelled()

    asyncio.run(exercise())


def test_bounded_stage_reports_the_specific_timeout() -> None:
    async def exercise() -> None:
        with pytest.raises(TimeoutError, match="stage quorum_acquire exceeded"):
            await nway.run_bounded_stage(
                "quorum_acquire", asyncio.Event().wait(), timeout_seconds=0.01
            )

    asyncio.run(exercise())


class _Process:
    def __init__(self) -> None:
        self.killed = False
        self.returncode = None

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> None:
        return None

    async def kill_tree(self) -> bool:
        self.kill()
        await self.wait()
        return True


class _Worker:
    def __init__(self, *, owner: bool) -> None:
        self.process = _Process()
        self.reader = asyncio.get_running_loop().create_future()
        self.reader.set_result(None)
        self.owner = owner
        self.capabilities = {"auto_renew": True}
        self.calls = []

    async def request(self, operation: str, **fields):
        self.calls.append((operation, fields))
        if self.owner:
            return {"status": "acquired", "lease_id": "owner-lease"}
        if operation == "release":
            return {"status": "released"}
        if self.owner_process.killed:
            return {"status": "acquired", "lease_id": "successor-lease"}
        return {"status": "busy" if operation == "try_acquire" else "timeout"}

    async def drain_reader(self) -> bool:
        return True

    async def kill_tree(self) -> bool:
        return await self.process.kill_tree()


def test_renewed_owner_survives_original_ttl_then_dies_with_process(monkeypatch) -> None:
    async def no_wait(_seconds: float) -> None:
        return None

    monkeypatch.setattr(nway.asyncio, "sleep", no_wait)

    async def exercise(backend: str) -> None:
        owner, successor = _Worker(owner=True), _Worker(owner=False)
        successor.owner_process = owner.process
        report = await nway.run_renewed_owner_kill(
            {"owner": owner, "successor": successor}, backend, "test",
            redis_lease_ms=40, etcd_lease_ms=40,
        )
        assert report["status"] == "passed"
        assert report["pre_kill_after_original_ttl"] == ("busy" if backend == "redis" else "timeout")
        assert report["post_kill"] == "acquired"
        assert owner.process.killed
        acquire_call = owner.calls[0]
        assert acquire_call[1]["auto_renew"] is True
        assert any(call[0] == "release" for call in successor.calls)

    asyncio.run(exercise("redis"))
    asyncio.run(exercise("etcd"))


def test_process_kill_uses_host_worker_and_keeps_pre_expiry_window() -> None:
    class Process:
        def __init__(self) -> None:
            self.returncode = None
            self.killed = False

        async def kill_tree(self) -> bool:
            self.killed = True
            self.returncode = -9
            return True

        async def wait(self) -> None:
            return None

    state = {"expires_at": 0.0}

    class KillWorker:
        def __init__(self, *, owner: bool) -> None:
            self.process = Process()
            self.owner = owner
            self.calls = []

        async def request(self, operation: str, **fields):
            self.calls.append((operation, fields))
            if operation == "release":
                return {"status": "released"}
            if self.owner:
                state["expires_at"] = time.monotonic() + fields["lease_ms"] / 1000
                return {"status": "acquired", "lease_id": "owner"}
            if time.monotonic() < state["expires_at"]:
                return {"status": "busy" if operation == "try_acquire" else "timeout"}
            return {"status": "acquired", "lease_id": "successor"}

        async def kill_tree(self) -> bool:
            return await self.process.kill_tree()

        async def drain_reader(self) -> bool:
            return True

    cpp_wrapper, python_owner, typescript_successor = (
        KillWorker(owner=False), KillWorker(owner=True), KillWorker(owner=False)
    )

    async def exercise() -> None:
        report = await nway.run_killed_owner(
            {"cpp": cpp_wrapper, "python": python_owner, "typescript": typescript_successor},
            "redis", "unit", redis_lease_ms=100, expiry_grace_ms=50,
        )
        assert report["status"] == "passed"
        assert report["killed_owner_sdk"] == "python"
        assert report["successor_sdk"] == "cpp"
        assert report["pre_expiry"] == "busy"
        assert report["lease_ms"] == 100
        assert report["pre_expiry_elapsed_ms"] < report["lease_ms"]
        assert python_owner.process.killed

    asyncio.run(exercise())


class _PauseProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode = None
        self.paused = False


class _PauseState:
    def __init__(self) -> None:
        self.held_by = None


class _PauseWorker:
    def __init__(self, pid: int, state: _PauseState, status_only_inspect: bool = False) -> None:
        self.process = _PauseProcess(pid)
        self.state = state
        self.calls = []
        self.contender_attempts = 0
        self.status_only_inspect = status_only_inspect

    def suspend_tree(self) -> int:
        assert not self.process.paused
        self.process.paused = True
        return 2

    def resume_tree(self) -> int:
        assert self.process.paused
        self.process.paused = False
        return 2

    async def request(self, operation: str, **fields):
        self.calls.append((operation, fields))
        if operation == "inspect":
            assert not self.process.paused
            assert fields["lease_id"] == f"lease-{self.process.pid}"
            if self.status_only_inspect:
                return {"status": "lost"}
            return {"status": "ok", "state": "lost"}
        if operation == "release":
            if self.state.held_by == self.process.pid:
                self.state.held_by = None
                return {"status": "released"}
            return {"status": "already_not_owned"}
        if operation in ("try_acquire", "acquire"):
            self.contender_attempts += 1
            if self.state.held_by is None:
                self.state.held_by = self.process.pid
                return {"status": "acquired", "lease_id": f"lease-{self.process.pid}"}
            if any(worker.process.paused for worker in getattr(self.state, "workers", [])):
                self.state.held_by = self.process.pid
                return {"status": "acquired", "lease_id": f"lease-{self.process.pid}"}
            if self.contender_attempts == 1:
                return {"status": "busy" if operation == "try_acquire" else "timeout"}
            return {"status": "busy" if operation == "try_acquire" else "timeout"}
        raise AssertionError(operation)


def test_pause_owner_beyond_ttl_resumes_alive_with_lost_lease(monkeypatch) -> None:
    async def no_wait(_seconds: float) -> None:
        return None

    monkeypatch.setattr(nway.asyncio, "sleep", no_wait)
    monkeypatch.setattr(nway, "process_scope", lambda pid: {
        "kind": "isolated_process_group", "process_group_id": pid,
    })

    async def exercise(backend: str, status_only_inspect: bool = False) -> None:
        state = _PauseState()
        owner = _PauseWorker(101, state, status_only_inspect=status_only_inspect)
        successor = _PauseWorker(202, state)
        state.workers = [owner, successor]
        report = await nway.run_paused_owner(
            {"owner": owner, "successor": successor}, backend, "unit")
        assert report["status"] == "passed"
        assert report["sdk_cannot_stop_resumed_process_effects"] is True
        cell = report["cells"][0]
        assert cell["resumed_owner_state"] == "lost"
        assert cell["resumed_owner_process_alive"] is True
        assert cell["process_ownership_disjoint"] is True
        assert cell["effects_forcibly_stopped"] is False
        owner_inspect = [fields for op, fields in owner.calls if op == "inspect"]
        assert owner_inspect == [{"lease_id": "lease-101"}]
        assert not any(op == "renew" for op, _ in owner.calls)
        if backend == "etcd":
            assert cell["pre_pause_rpc_ms"] > cell["pre_pause_wait_ms"]
            assert cell["pre_pause_wait_ms"] + cell["pre_pause_rpc_ms"] < cell["lease_ms"]
        else:
            assert cell["pre_pause_rpc_ms"] == 500
        assert not owner.process.paused

    asyncio.run(exercise("redis"))
    asyncio.run(exercise("etcd"))
    asyncio.run(exercise("redis", status_only_inspect=True))


class _CancellationWorker:
    def __init__(self) -> None:
        self.pending = {}
        self.lease = 0

    async def request(self, operation: str, **fields):
        if operation == "acquire":
            self.lease += 1
            return {"status": "acquired", "lease_id": f"lease-{self.lease}"}
        if operation == "cancel_request":
            target = self.pending[fields["target_id"]]
            target.set_result({"status": "cancelled", "remote_outcome_unknown": False})
            return {"status": "ok"}
        if operation == "release":
            return {"status": "released"}
        raise AssertionError(operation)

    async def start_request(self, operation: str, **fields):
        assert operation == "acquire"
        future = asyncio.get_running_loop().create_future()
        target_id = "pending-acquire"
        self.pending[target_id] = future
        return target_id, future


def test_cancel_wait_scenario_requires_explicit_cancellation_and_reacquire(monkeypatch) -> None:
    async def no_wait(_seconds: float) -> None:
        return None

    monkeypatch.setattr(nway.asyncio, "sleep", no_wait)

    async def exercise() -> None:
        worker = _CancellationWorker()
        report = await nway.run_cancel_wait({"fake": worker}, "redis", "unit")
        assert report["status"] == "passed"
        cell = report["cells"][0]
        assert cell["sdk"] == "fake"
        assert cell["post_cancel_reacquire"] == "acquired"
        assert cell["remote_outcome_unknown"] is False

    asyncio.run(exercise())
