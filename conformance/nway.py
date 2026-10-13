"""Exercise all native SDK workers against one backend under multi-way contention.

This is a real-process integration test. It does not inject partitions or claim
availability/fencing properties beyond the recorded single-backend run.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import traceback
import uuid
from urllib.parse import unquote, urlsplit, urlunsplit
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from conformance.process_control import process_scope  # noqa: E402 -- repository root is added above for script execution.
from conformance.run import Worker, expect  # noqa: E402 -- repository root is added above for script execution.


def safe_endpoint(endpoint: str) -> str:
    """Keep credentials out of retained JSON evidence and console output."""
    parsed = urlsplit(endpoint)
    if parsed.password is None:
        return endpoint
    user = parsed.username or ""
    host = parsed.hostname or ""
    if parsed.port is not None:
        host += f":{parsed.port}"
    return urlunsplit((parsed.scheme, f"{user}:***@{host}", parsed.path,
                       parsed.query, parsed.fragment))


async def wait_one(tasks: dict[str, asyncio.Task], timeout: float = 12.0) -> tuple[str, dict]:
    done, _ = await asyncio.wait(tasks.values(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    if not done:
        raise TimeoutError("no queued SDK acquired after predecessor release")
    await asyncio.sleep(0.5)
    completed = {name: task for name, task in tasks.items() if task.done()}
    replies = {}
    for name, task in completed.items():
        try:
            replies[name] = task.result()
        except Exception as exc:
            replies[name] = {"worker_error": f"{type(exc).__name__}: {exc}"}
    winners = [name for name, reply in replies.items() if reply.get("status") == "acquired"]
    if len(winners) != 1 or len(completed) != 1:
        raise AssertionError(f"expected one completed lock winner, replies={replies}")
    winner = winners[0]
    return winner, replies[winner]


async def run_nway(workers: dict[str, Worker], backend: str, suffix: str) -> dict:
    resource = f"m6/nway/{suffix}/{uuid.uuid4().hex}"
    names = list(workers)
    winners: list[str] = []
    if backend == "redis":
        responses = await asyncio.gather(*(
            worker.request("try_acquire", resource=resource, lease_ms=30000,
                           rpc_ms=1000, auto_renew=False)
            for worker in workers.values()
        ))
        acquired = [name for name, result in zip(names, responses) if result.get("status") == "acquired"]
        busy = [name for name, result in zip(names, responses) if result.get("status") == "busy"]
        if len(acquired) != 1 or len(busy) != len(names) - 1:
            raise AssertionError(f"initial contenders: acquired={acquired}, busy={busy}, replies={responses}")
        name = acquired[0]
        winners.append(name)
        expect(await workers[name].request("release", lease_id=responses[names.index(name)]["lease_id"]), "released")
        remaining = [candidate for candidate in names if candidate not in winners]
        while remaining:
            replies = await asyncio.gather(*(
                workers[candidate].request("try_acquire", resource=resource, lease_ms=30000,
                                           rpc_ms=1000, auto_renew=False)
                for candidate in remaining
            ))
            next_winners = [candidate for candidate, reply in zip(remaining, replies)
                            if reply.get("status") == "acquired"]
            if len(next_winners) != 1:
                raise AssertionError(f"round contenders: acquired={next_winners}, replies={replies}")
            name = next_winners[0]
            reply = replies[remaining.index(name)]
            winners.append(name)
            expect(await workers[name].request("release", lease_id=reply["lease_id"]), "released")
            remaining.remove(name)
    else:
        print(f"n-way etcd acquisition started for {len(names)} SDK workers", flush=True)
        pending = {
            name: asyncio.create_task(worker.request(
                "acquire", resource=resource, wait_ms=30000, lease_ms=30000,
                rpc_ms=500, auto_renew=False,
            ))
            for name, worker in workers.items()
        }
        try:
            while pending:
                winner, reply = await wait_one(pending)
                expect(reply, "acquired")
                winners.append(winner)
                print(f"n-way etcd winner {len(winners)}/{len(names)}: {winner}", flush=True)
                expect(await workers[winner].request("release", lease_id=reply["lease_id"]), "released")
                pending.pop(winner)
        finally:
            for task in pending.values():
                task.cancel()
            await asyncio.gather(*pending.values(), return_exceptions=True)
    if len(winners) != len(names) or set(winners) != set(names):
        raise AssertionError(f"not every SDK acquired exactly once: {winners}")
    return {"id": "nway_contention", "status": "passed", "participants": names,
            "winner_order": winners, "exclusive_winner_count": len(winners)}


async def run_cancel_wait(workers: dict[str, Worker], backend: str, suffix: str) -> dict:
    """Cancel a real worker's queued acquire while a live worker holds the key."""
    names = list(workers)
    if not names:
        raise RuntimeError("cancellation scenario requires at least one worker")
    holder_name = names[0]
    holder = workers[holder_name]
    cells = []
    for contender_name, contender in workers.items():
        resource = f"m6/cancel-wait/{backend}/{suffix}/{uuid.uuid4().hex}"
        held = None
        target_future = None
        try:
            acquire_fields = dict(resource=resource, wait_ms=15000, lease_ms=6000,
                                  rpc_ms=2000, auto_renew=False)
            held = expect(await holder.request("acquire", **acquire_fields), "acquired")
            target_id, target_future = await contender.start_request("acquire", **acquire_fields)
            await asyncio.sleep(0.35)
            if target_future.done():
                raise AssertionError(
                    f"{contender_name} acquire did not remain pending behind {holder_name}: "
                    f"{target_future.result()}")

            cancel_ack = await contender.request("cancel_request", target_id=target_id)
            if cancel_ack.get("status") not in ("ok", "cancel_requested"):
                raise AssertionError(f"unexpected cancel acknowledgement: {cancel_ack}")
            if cancel_ack.get("found") is False or cancel_ack.get("cancelled") is False:
                raise AssertionError(f"worker did not find pending request {target_id}: {cancel_ack}")
            cancelled = await asyncio.wait_for(target_future, 10)
            outcome = cancelled.get("status")
            remote_unknown = cancelled.get("remote_outcome_unknown")
            if outcome not in ("cancelled", "acquisition_unknown"):
                raise AssertionError(f"expected cancelled or explicitly unknown acquisition, got {cancelled}")
            if not isinstance(remote_unknown, bool):
                raise AssertionError(f"cancellation did not report remote outcome certainty: {cancelled}")
            if outcome == "acquisition_unknown" and remote_unknown is not True:
                raise AssertionError(f"acquisition_unknown must retain uncertainty: {cancelled}")

            expect(await holder.request("release", lease_id=held["lease_id"]), "released")
            held = None
            successor = expect(await contender.request("acquire", timeout=15, **acquire_fields), "acquired")
            expect(await contender.request("release", lease_id=successor["lease_id"]), "released")
            cells.append({"sdk": contender_name, "holder_sdk": holder_name,
                          "status": "passed", "acquisition_result": outcome,
                          "remote_outcome_unknown": remote_unknown,
                          "cancel_ack": cancel_ack.get("status"),
                          "post_cancel_reacquire": "acquired"})
        except Exception as exc:
            cells.append({"sdk": contender_name, "holder_sdk": holder_name,
                          "status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        finally:
            if target_future is not None and not target_future.done():
                try:
                    await contender.request("cancel_request", target_id=target_id, timeout=3)
                    await asyncio.wait_for(target_future, 5)
                except Exception:
                    pass
            if held is not None:
                try:
                    await holder.request("release", lease_id=held["lease_id"], timeout=5)
                except Exception:
                    pass
    return {"id": "cancel_acquire_during_wait", "backend": backend,
            "status": "passed" if all(cell["status"] == "passed" for cell in cells) else "failed",
            "real_processes": True, "real_backend": True, "cells": cells}


async def run_bounded_stage(name: str, operation, timeout_seconds: float):
    print(f"stage started: {name}", flush=True)
    try:
        result = await asyncio.wait_for(operation, timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"stage {name} exceeded {timeout_seconds:g}s") from exc
    print(f"stage completed: {name}", flush=True)
    return result


async def run_killed_owner(
    workers: dict[str, Worker], backend: str, suffix: str, *,
    redis_lease_ms: int = 30000, etcd_lease_ms: int = 30000,
    expiry_grace_ms: int = 1500,
) -> dict:
    # Earlier fault scenarios intentionally kill their owner process. Select
    # two live workers so this scenario remains meaningful in a composed run.
    available = {name: worker for name, worker in workers.items()
                 if worker.process.returncode is None}
    if len(available) < 2:
        raise RuntimeError("process-kill owner scenario requires two live SDK workers")
    # Prefer Python, TypeScript, or .NET workers for this scenario: the core
    # manifest launches them directly on the host. Docker-backed launchers can
    # expose only the `docker run` client as their host process; killing that
    # wrapper may close JSONL stdin and let the container worker exit cleanly,
    # which is not equivalent to killing the actual lock owner.
    owner_name = next(
        (name for name in ("python", "typescript", "dotnet") if name in available),
        next(iter(available)),
    )
    successor_name = next(name for name in available if name != owner_name)
    owner = available[owner_name]
    successor = available[successor_name]
    resource = f"m6/killed-owner/{suffix}/{uuid.uuid4().hex}"
    # This scenario kills native and Docker-wrapped workers. On Windows,
    # terminating and draining a Docker-backed process can take several
    # seconds; keep enough lease time to observe that death did not release
    # ownership early, then wait for the exact TTL before checking successor
    # acquisition.
    ttl_ms = etcd_lease_ms if backend == "etcd" else redis_lease_ms
    # The native etcd Lock RPC remains pending while another owner holds the
    # key. Give it a transport budget longer than the caller's wait deadline.
    rpc_ms = 5000 if backend == "etcd" else 500
    fields = dict(resource=resource, lease_ms=ttl_ms, rpc_ms=rpc_ms, auto_renew=False)
    if backend == "redis":
        expect(await owner.request("try_acquire", **fields), "acquired")
    else:
        expect(await owner.request("acquire", wait_ms=5000, **fields), "acquired")
    acquired_at = time.monotonic()
    owner_tree_killed = await owner.kill_tree()
    if not owner_tree_killed:
        raise RuntimeError("failed to confirm terminated SDK owner process tree")
    await owner.process.wait()
    owner_stdout_closed = await owner.drain_reader()
    pre_expiry_elapsed_ms = round((time.monotonic() - acquired_at) * 1000)
    if pre_expiry_elapsed_ms >= ttl_ms:
        raise TimeoutError(
            "worker termination consumed the full lease TTL before the pre-expiry check; "
            f"elapsed_ms={pre_expiry_elapsed_ms}, lease_ms={ttl_ms}"
        )

    if backend == "redis":
        before_expiry = expect(await successor.request("try_acquire", **fields), "busy")
    else:
        before_expiry = expect(await successor.request("acquire", wait_ms=1200, **fields), "timeout")

    await asyncio.sleep((ttl_ms + expiry_grace_ms) / 1000)
    if backend == "redis":
        next_lease = expect(await successor.request("try_acquire", **fields), "acquired")
    else:
        next_lease = expect(await successor.request("acquire", wait_ms=5000, **fields), "acquired")
    expect(await successor.request("release", lease_id=next_lease["lease_id"]), "released")
    return {"id": "process_kill_owner_expiry", "status": "passed", "backend": backend,
            "killed_owner_sdk": owner_name, "successor_sdk": successor_name,
            "lease_ms": ttl_ms, "pre_expiry": before_expiry["status"],
            "pre_expiry_elapsed_ms": pre_expiry_elapsed_ms, "post_expiry": "acquired",
            "killed_owner_process_tree": owner_tree_killed,
            "killed_owner_stdout_eof": owner_stdout_closed}


async def run_renewed_owner_kill(workers: dict[str, Worker], backend: str, suffix: str,
                                *, redis_lease_ms: int = 7000,
                                etcd_lease_ms: int = 10000) -> dict:
    """Prove renewal extends a live owner, then process death stops renewal.

    This is process-failure evidence for one local backend member. It does not
    simulate a network partition or establish that a paused application stops
    using resources after losing its lease.
    """
    # Earlier fault scenarios intentionally kill their owner process. Choose
    # a live owner that advertises automatic renewal, then any other live SDK
    # as successor. Some workers intentionally do not implement auto-renew.
    available = {name: worker for name, worker in workers.items()
                 if worker.process.returncode is None}
    capable = {name: worker for name, worker in available.items()
               if _supports_capability(worker.capabilities, "auto_renew")}
    if not capable:
        return {"id": "renewed_owner_process_kill", "status": "skipped",
                "backend": backend, "reason": "no live worker declares auto_renew",
                "unsupported_owner_sdks": sorted(available)}
    if len(available) < 2:
        return {"id": "renewed_owner_process_kill", "status": "skipped",
                "backend": backend, "reason": "no distinct live successor worker",
                "renewal_owner_sdks": sorted(capable)}
    owner_name, owner = next(iter(capable.items()))
    successor_name, successor = next((name, worker) for name, worker in available.items()
                                     if name != owner_name)
    excluded_owner_sdks = sorted(set(available) - set(capable))
    resource = f"m6/renewed-owner-kill/{backend}/{suffix}/{uuid.uuid4().hex}"
    ttl_ms = etcd_lease_ms if backend == "etcd" else redis_lease_ms
    rpc_ms = 5000 if backend == "etcd" else 500
    fields = dict(resource=resource, lease_ms=ttl_ms, rpc_ms=rpc_ms, auto_renew=True)
    successor_fields = {**fields, "auto_renew": False}
    if backend == "redis":
        expect(await owner.request("try_acquire", **fields), "acquired")
    else:
        expect(await owner.request("acquire", wait_ms=5000, **fields), "acquired")

    # Let the original server TTL elapse while the owner process stays live.
    # A contender must still observe the lock, proving renewal made progress.
    await asyncio.sleep(ttl_ms / 1000 + 1.0)
    if backend == "redis":
        still_held = expect(await successor.request("try_acquire", **successor_fields), "busy")
    else:
        still_held = expect(await successor.request("acquire", wait_ms=1200,
                                                   **successor_fields), "timeout")

    # Killing the process terminates its renewal loop. The backend TTL is then
    # allowed to expire naturally before the successor can acquire.
    owner_tree_killed = await owner.kill_tree()
    if not owner_tree_killed:
        raise RuntimeError("failed to confirm terminated renewed owner process tree")
    await owner.process.wait()
    owner_stdout_closed = await owner.drain_reader()
    if backend == "redis":
        deadline = time.monotonic() + ttl_ms / 1000 + 6.0
        after_kill = None
        while time.monotonic() < deadline:
            response = await successor.request("try_acquire", **successor_fields)
            if response.get("status") == "acquired":
                after_kill = response
                break
            expect(response, "busy")
            await asyncio.sleep(0.25)
        if after_kill is None:
            raise TimeoutError("renewed Redis lease did not expire after owner process death")
    else:
        after_kill = expect(await successor.request("acquire", wait_ms=ttl_ms + 8000,
                                                    **successor_fields), "acquired")
    expect(await successor.request("release", lease_id=after_kill["lease_id"]), "released")
    return {"id": "renewed_owner_process_kill", "status": "passed", "backend": backend,
            "renewed_owner_sdk": owner_name, "successor_sdk": successor_name,
            "excluded_owner_sdks": excluded_owner_sdks,
            "lease_ms": ttl_ms, "pre_kill_after_original_ttl": still_held["status"],
            "post_kill": "acquired", "killed_owner_process_tree": owner_tree_killed,
            "killed_owner_stdout_eof": owner_stdout_closed}


def _supports_capability(capabilities: object, name: str) -> bool:
    """Accept both worker capability maps and the original JSONL string lists."""
    if isinstance(capabilities, dict):
        return capabilities.get(name) is True
    if isinstance(capabilities, list):
        return name in capabilities
    return False


async def run_paused_owner(workers: dict[str, Worker], backend: str, suffix: str) -> dict:
    """Pause each owner past expiry, then prove it resumes alive with a lost lease.

    This exercises OS process suspension and backend lease expiry. Dilock cannot
    forcibly stop application code or external effects when a process resumes.
    """
    available = {name: worker for name, worker in workers.items()
                 if worker.process.returncode is None}
    if len(available) < 2:
        raise RuntimeError("pause/resume scenario requires two live SDK workers")
    names = list(available)
    ttl_ms = 4000 if backend == "redis" else 6000
    # The native etcd Lock RPC can remain pending behind the current owner.
    # Its transport budget must outlive this test's shorter wait deadline or a
    # healthy lock wait is misreported as a backend failure.
    pre_pause_wait_ms = 1200 if backend == "etcd" else None
    # TypeScript bounds its native Lock RPC at wait_ms + rpc_ms. Keep that
    # aggregate below the attempt lease TTL while letting rpc_ms exceed the
    # shared contention wait so the lease does not expire during its own RPC.
    rpc_ms = 500 if backend == "redis" else 1500
    cells = []
    for index, (owner_name, owner) in enumerate(available.items()):
        successor_name = names[(index + 1) % len(names)]
        successor = available[successor_name]
        resource = f"m6/paused-owner/{backend}/{suffix}/{uuid.uuid4().hex}"
        fields = dict(resource=resource, lease_ms=ttl_ms, rpc_ms=rpc_ms, auto_renew=False)
        held = None
        successor_lease = None
        paused = False
        scope = process_scope(owner.process.pid)
        successor_scope = process_scope(successor.process.pid)
        if scope["kind"] == "windows_descendants_of_worker":
            if set(scope["process_ids"]) & set(successor_scope["process_ids"]):
                raise RuntimeError("owner and successor process trees overlap")
        elif scope["process_group_id"] == successor_scope["process_group_id"]:
            raise RuntimeError("owner and successor do not have separate process groups")
        try:
            if backend == "redis":
                held = expect(await owner.request("try_acquire", **fields), "acquired")
                expect(await successor.request("try_acquire", **fields), "busy")
            else:
                held = expect(await owner.request("acquire", wait_ms=5000, **fields), "acquired")
                before_expiry = expect(await successor.request(
                    "acquire", wait_ms=pre_pause_wait_ms, **fields), "timeout")

            suspended_units = owner.suspend_tree()
            paused = True
            await asyncio.sleep(ttl_ms / 1000 + 1.5)
            if backend == "redis":
                successor_lease = expect(await successor.request("try_acquire", **fields), "acquired")
            else:
                successor_lease = expect(await successor.request(
                    "acquire", wait_ms=ttl_ms + 8000, **fields), "acquired")

            resumed_units = owner.resume_tree()
            paused = False
            if owner.process.returncode is not None:
                raise AssertionError("resumed owner worker process is no longer alive")
            inspection = await owner.request("inspect", lease_id=held["lease_id"])
            # C++ reports its local lease state directly in `status` (`held` or
            # `lost`), while the other workers return status=ok plus `state`.
            # Accept only the exact terminal observation expected here; do not
            # turn backend errors or other states into a successful check.
            if inspection.get("status") == "lost" and "state" not in inspection:
                resumed_state = "lost"
            else:
                resumed_lease = expect(inspection, "ok")
                resumed_state = resumed_lease.get("state")
            if resumed_state != "lost":
                raise AssertionError(
                    "resumed expired owner must report local lease state lost; "
                    f"got {resumed_state!r}"
                )
            expect(await successor.request("release", lease_id=successor_lease["lease_id"]), "released")
            successor_lease = None
            cell = {"sdk": owner_name, "successor_sdk": successor_name,
                    "status": "passed", "lease_ms": ttl_ms,
                    "pre_pause_contention": "busy" if backend == "redis" else before_expiry["status"],
                    "pre_pause_rpc_ms": rpc_ms,
                    "pre_pause_wait_ms": pre_pause_wait_ms,
                    "successor_after_expiry": "acquired",
                    "resumed_owner_state": resumed_state,
                    "resumed_owner_process_alive": True,
                    "process_scope": scope, "successor_process_scope": successor_scope,
                    "process_ownership_disjoint": True, "suspended_units": suspended_units,
                    "resumed_units": resumed_units,
                    "effects_forcibly_stopped": False}
            cells.append(cell)
        except Exception as exc:
            cells.append({"sdk": owner_name, "successor_sdk": successor_name,
                          "status": "failed", "error": f"{type(exc).__name__}: {exc}",
                          "process_scope": scope, "successor_process_scope": successor_scope})
        finally:
            if paused:
                try:
                    owner.resume_tree()
                except Exception:
                    pass
            if successor_lease is not None:
                try:
                    await successor.request("release", lease_id=successor_lease["lease_id"], timeout=5)
                except Exception:
                    pass
            if held is not None:
                try:
                    await owner.request("release", lease_id=held["lease_id"], timeout=5)
                except Exception:
                    pass
    return {"id": "pause_owner_beyond_ttl_and_resume", "backend": backend,
            "status": "passed" if all(cell["status"] == "passed" for cell in cells) else "failed",
            "real_processes": True, "real_backend": True,
            "sdk_cannot_stop_resumed_process_effects": True, "cells": cells}


async def main(args: argparse.Namespace) -> int:
    if not args.endpoint:
        args.endpoint = os.environ.get("DILOCK_NWAY_ENDPOINT")
    if not args.endpoint:
        args.endpoint = "redis://127.0.0.1:16379" if args.backend == "redis" else "http://127.0.0.1:22379"
    namespace = "m6-nway-" + uuid.uuid4().hex
    epoch = "m6-local-disposable-v1"
    if args.backend == "redis":
        from tools.provision import provision

        await provision(args.endpoint, namespace, epoch)
    else:
        from tools.provision_etcd import provision_etcd

        provision_etcd(args.endpoint, namespace, epoch)
    manifest = json.loads((ROOT / args.workers).read_text(encoding="utf-8"))
    selected = args.sdks or list(manifest)
    run_dir = ROOT / "validation/implementation" / ("nway-" + args.backend + "-" + uuid.uuid4().hex)
    run_dir.mkdir(parents=True)
    config_dir = ROOT / ".tools" / ("m6-nway-config-" + uuid.uuid4().hex)
    config_dir.mkdir(parents=True)
    config = config_dir / "config.json"
    base_config = dict(backend=args.backend, endpoint=args.endpoint,
                       namespace=namespace, epoch=epoch)
    config.write_text(json.dumps(base_config), encoding="utf-8")
    workers: dict[str, Worker] = {}
    capabilities: dict[str, object] = {}
    logs = []
    results = []
    status = "failed"
    stage = "worker_startup"
    try:
        for sdk in selected:
            stage = f"worker_startup:{sdk}"
            log = (run_dir / (sdk + ".stderr.log")).open("wb")
            logs.append(log)
            worker_config = config
            if sdk == "cpp" and args.backend == "redis":
                parsed = urlsplit(args.endpoint)
                redis_config = {
                    "host": parsed.hostname,
                    "port": parsed.port or 6379,
                    "username": unquote(parsed.username) if parsed.username else "",
                    "password": unquote(parsed.password) if parsed.password else "",
                    "database": int(parsed.path.lstrip("/") or "0"),
                    "namespace": namespace,
                    "coordination_epoch": epoch,
                    "rpc_timeout_ms": 1000,
                }
                worker_config = config_dir / "config-cpp.json"
                worker_config.write_text(json.dumps({"backend": "redis", "redis": redis_config}),
                                         encoding="utf-8")
            elif sdk == "cpp" and args.cpp_endpoint:
                worker_config = config_dir / "config-cpp.json"
                worker_config.write_text(json.dumps({**base_config, "endpoint": args.cpp_endpoint}),
                                         encoding="utf-8")
            argv = manifest[sdk]["argv"] + [str(worker_config)]
            proc = await asyncio.create_subprocess_exec(*argv, cwd=ROOT, env=os.environ.copy(),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=log,
                start_new_session=(os.name != "nt"))
            workers[sdk] = Worker(proc)
            hello = expect(await workers[sdk].request("hello", timeout=60), "ok")
            workers[sdk].capabilities = hello.get("capabilities", {})
            capabilities[sdk] = workers[sdk].capabilities
            print(f"worker ready: {sdk}", flush=True)
        if args.only_renewed_owner:
            stage = "renewed_owner_process_kill"
            renewed_result = await run_bounded_stage(
                stage, run_renewed_owner_kill(workers, args.backend, namespace), timeout_seconds=120)
            results.append(renewed_result)
            if renewed_result["status"] not in ("passed", "skipped"):
                raise RuntimeError("renewed-owner scenario failed")
            status = "passed" if renewed_result["status"] == "passed" else "skipped"
        elif args.only_pause_owner:
            stage = "pause_owner_beyond_ttl_and_resume"
            pause_result = await run_bounded_stage(
                stage, run_paused_owner(workers, args.backend, namespace), timeout_seconds=300)
            results.append(pause_result)
            if pause_result["status"] != "passed":
                raise RuntimeError("one or more SDK pause/resume cells failed")
        else:
            if not args.only_cancel_wait:
                stage = "nway_contention"
                results.append(await run_bounded_stage(
                    stage, run_nway(workers, args.backend, namespace), timeout_seconds=120))
            stage = "cancel_acquire_during_wait"
            cancellation_result = await run_bounded_stage(
                stage, run_cancel_wait(workers, args.backend, namespace), timeout_seconds=180)
            results.append(cancellation_result)
            if cancellation_result["status"] != "passed":
                raise RuntimeError("one or more SDK cancellation cells failed")
            if not args.only_cancel_wait:
                stage = "renewed_owner_process_kill"
                results.append(await run_bounded_stage(
                    stage, run_renewed_owner_kill(workers, args.backend, namespace), timeout_seconds=120))
                # The renewed-owner scenario kills its renewing worker. Run it
                # before the generic owner-kill scenario so a sole
                # auto-renew-capable worker is not removed before this check.
                stage = "process_kill_owner_expiry"
                results.append(await run_bounded_stage(
                    stage, run_killed_owner(workers, args.backend, namespace), timeout_seconds=90))
        status = "passed"
    except Exception as exc:
        results.append({"id": "nway_or_fault_scenario", "status": "failed",
                        "stage": stage, "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc()})
        print(results[-1], flush=True)
    finally:
        await asyncio.gather(*(worker.close() for worker in workers.values()), return_exceptions=True)
        for log in logs:
            log.close()
        report = {"backend": args.backend, "endpoint": safe_endpoint(args.endpoint), "namespace": namespace,
                  "cpp_endpoint": args.cpp_endpoint,
                  "real_processes": True, "real_backend": True, "participants": selected,
                  "worker_capabilities": capabilities,
                  "only_renewed_owner": args.only_renewed_owner,
                  "status": status, "results": results, "run_directory": str(run_dir)}
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
    return int(status != "passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("redis", "etcd"), required=True)
    parser.add_argument("--endpoint", help="Backend URL; DILOCK_NWAY_ENDPOINT is preferred for credentialed URLs.")
    parser.add_argument("--cpp-endpoint", help="Optional endpoint reachable from the C++ worker's host, such as WSL.")
    parser.add_argument("--sdks", nargs="*")
    parser.add_argument("--only-cancel-wait", action="store_true",
                        help="Run only the queued-acquisition cancellation scenario.")
    parser.add_argument("--only-pause-owner", action="store_true",
                        help="Run only the pause-beyond-TTL-and-resume scenario.")
    parser.add_argument("--only-renewed-owner", action="store_true",
                        help="Run only the capability-aware renewed-owner process-kill scenario.")
    parser.add_argument("--workers", default="conformance/workers.json")
    parser.add_argument("--output", required=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
