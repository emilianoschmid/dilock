"""Keep the JSON Schema aligned with the normative worker protocol."""

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WorkerMessageSchemaTests(unittest.TestCase):
    def test_schema_covers_all_documented_operations(self) -> None:
        schema = json.loads((ROOT / "conformance" / "worker-message.schema.json").read_text())
        documented = {
            "hello",
            "acquire",
            "try_acquire",
            "renew",
            "release",
            "inspect",
            "cancel_request",
            "shutdown",
        }
        self.assertEqual(documented, set(schema["properties"]["op"]["enum"]))

    def test_renew_requires_lease_id(self) -> None:
        schema = json.loads((ROOT / "conformance" / "worker-message.schema.json").read_text())
        conditions = schema["allOf"]
        required_lease_id = {
            operation
            for condition in conditions
            for operation in condition.get("if", {}).get("properties", {}).get("op", {}).get("enum", [])
            if "lease_id" in condition.get("then", {}).get("required", [])
        }
        self.assertIn("renew", required_lease_id)


if __name__ == "__main__":
    unittest.main()
