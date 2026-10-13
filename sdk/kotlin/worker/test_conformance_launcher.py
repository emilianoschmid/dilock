import importlib.util
import json
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("conformance_launcher.py")
SPEC = importlib.util.spec_from_file_location("conformance_launcher", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ConformanceLauncherTest(unittest.TestCase):
    def test_removes_only_runner_deadline_from_acquire(self):
        source = b'{"id":"r1","op":"acquire","resource":"a/b","rpc_ms":300,"wait_ms":10}\n'
        actual = launcher.strip_harness_rpc_deadline(source)
        self.assertEqual(
            {"id": "r1", "op": "acquire", "resource": "a/b", "wait_ms": 10},
            json.loads(actual),
        )
        self.assertTrue(actual.endswith(b"\n"))

    def test_removes_deadline_from_try_acquire(self):
        actual = launcher.strip_harness_rpc_deadline(b'{"id":"r2","op":"try_acquire","rpc_ms":200}\n')
        self.assertEqual({"id": "r2", "op": "try_acquire"}, json.loads(actual))

    def test_preserves_non_acquire_requests_byte_for_byte(self):
        for source in (
            b'{"id":"r3","op":"hello","rpc_ms":100}\n',
            b'{"id":"r4","op":"release","lease_id":"l","rpc_ms":100}\n',
            b'{"id":"r5","op":"acquire","resource":"x"}\n',
        ):
            with self.subTest(source=source):
                self.assertEqual(source, launcher.strip_harness_rpc_deadline(source))

    def test_preserves_malformed_or_non_object_json_for_strict_worker_rejection(self):
        for source in (b"not-json\n", b"[]\n", b"null\n"):
            with self.subTest(source=source):
                self.assertEqual(source, launcher.strip_harness_rpc_deadline(source))


if __name__ == "__main__":
    unittest.main()
