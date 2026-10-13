"""Protocol-only checks for the test-scoped C ABI conformance worker."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[4]
WORKER = Path(os.environ.get("DILOCK_C_WORKER", ROOT / ".tools/dilock-c-conformance-worker"))
LAUNCHER = ROOT / "sdk/c/tests/conformance/launch_worker.py"


class CWorkerProtocolTests(unittest.TestCase):
    def test_strict_jsonl_envelopes_and_bounded_input(self) -> None:
        container = os.environ.get("DILOCK_C_WORKER_CONTAINER")
        self.assertTrue(container or WORKER.is_file(),
                        "set DILOCK_C_WORKER_CONTAINER or DILOCK_C_WORKER")
        with tempfile.TemporaryDirectory(prefix="dilock-c-worker-",
                                         dir=ROOT / "validation/implementation") as directory:
            config = Path(directory) / "config.json"
            config.write_text(json.dumps({
                "backend": "redis",
                "endpoint": "redis://127.0.0.1:6379",
                "namespace": "worker-protocol-test",
                "epoch": "worker-protocol-test-epoch",
            }), encoding="utf-8")
            requests = [
                json.dumps({"id": "hello", "op": "hello"}),
                json.dumps({"id": "extra", "op": "hello", "credential": "must-not-echo"}),
                "{ malformed",
                json.dumps({"id": "unknown-op", "op": "replace_coordinator"}),
                json.dumps({"id": "shutdown", "op": "shutdown"}),
            ]
            requests.insert(3, "x" * (64 * 1024 + 1))
            command = ([sys.executable, str(LAUNCHER), str(config)] if container else
                       [str(WORKER), str(config)])
            result = subprocess.run(
                command,
                input="\n".join(requests) + "\n",
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
                env=os.environ.copy(),
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        responses = [json.loads(line) for line in result.stdout.splitlines()]
        by_id = {response["id"]: response for response in responses if "id" in response}
        self.assertEqual(by_id["hello"]["status"], "ok")
        self.assertEqual(by_id["hello"]["sdk"], "c")
        self.assertEqual(by_id["extra"]["status"], "invalid_argument")
        self.assertEqual(by_id["unknown-op"]["status"], "invalid_argument")
        self.assertEqual(by_id["shutdown"]["status"], "ok")
        self.assertNotIn("must-not-echo", result.stdout)
        self.assertTrue(any(item.get("error") == "malformed request" for item in responses))
        self.assertTrue(any(item.get("error") == "input line exceeds 64 KiB" for item in responses))


if __name__ == "__main__":
    unittest.main()
