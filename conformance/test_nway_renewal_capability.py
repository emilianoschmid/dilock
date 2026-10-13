"""Focused tests for capability-aware renewed-owner fault selection."""
from __future__ import annotations

import asyncio
import unittest

from conformance.nway import run_renewed_owner_kill


class FakeWorker:
    def __init__(self, *, auto_renew: bool, state: dict[str, bool]):
        self.capabilities = {"auto_renew": auto_renew}
        self.process = self
        self.returncode = None
        self.state = state

    async def request(self, op: str, **fields):
        if op == "try_acquire":
            if self.state["held"]:
                return {"status": "busy"}
            self.state["held"] = True
            return {"status": "acquired", "lease_id": "fake-lease"}
        if op == "release":
            self.state["held"] = False
            return {"status": "released"}
        raise AssertionError(f"unexpected operation: {op}")

    async def kill_tree(self) -> bool:
        self.returncode = -9
        self.state["held"] = False
        return True

    async def wait(self) -> int:
        return self.returncode

    async def drain_reader(self) -> bool:
        return True


class RenewedOwnerCapabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsupported_java_like_worker_is_only_a_successor(self):
        state = {"held": False}
        workers = {
            "java": FakeWorker(auto_renew=False, state=state),
            "python": FakeWorker(auto_renew=True, state=state),
        }
        result = await run_renewed_owner_kill(
            workers, "redis", "capability-test", redis_lease_ms=1
        )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["renewed_owner_sdk"], "python")
        self.assertEqual(result["successor_sdk"], "java")
        self.assertEqual(result["excluded_owner_sdks"], ["java"])

    async def test_no_renewal_capability_is_explicitly_skipped(self):
        workers = {"java": FakeWorker(auto_renew=False, state={"held": False})}
        result = await run_renewed_owner_kill(workers, "redis", "no-capability")
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["unsupported_owner_sdks"], ["java"])

    async def test_string_list_capability_advertisement_is_supported(self):
        state = {"held": False}
        workers = {
            "dotnet": FakeWorker(auto_renew=False, state=state),
            "java": FakeWorker(auto_renew=False, state=state),
        }
        workers["dotnet"].capabilities = ["redis", "renewal", "auto_renew"]
        result = await run_renewed_owner_kill(
            workers, "redis", "list-capability-test", redis_lease_ms=1
        )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["renewed_owner_sdk"], "dotnet")
        self.assertEqual(result["successor_sdk"], "java")


if __name__ == "__main__":
    unittest.main()
