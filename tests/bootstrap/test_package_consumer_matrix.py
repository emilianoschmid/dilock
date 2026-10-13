"""Fail-closed contract for exact package-consumer evidence."""
from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
import sys
import unittest
from unittest.mock import patch

from tools.package_consumers import (
    DriverError,
    DRIVER_PATHS,
    SDK_NAMES,
    environment_blocked,
    initial_report,
    normalize_driver_cell,
    package_build_python,
    package_build_site_packages,
    require_complete,
    run_external_sdk_consumer,
    safe_sdist_target,
    validate_report,
)

ROOT = Path(__file__).resolve().parents[2]


class PackageConsumerMatrixTests(unittest.TestCase):
    def test_configured_consumer_driver_paths_match_checkout_case(self):
        for sdk, relative_path in DRIVER_PATHS.items():
            current = ROOT
            for part in PurePosixPath(relative_path).parts:
                names = {entry.name for entry in current.iterdir()}
                self.assertIn(part, names, f"{sdk} consumer path has wrong case or is missing: {relative_path}")
                current /= part
            self.assertTrue(current.is_file(), f"{sdk} consumer driver is not a file: {relative_path}")

    def test_initial_matrix_keeps_unimplemented_and_go_states_explicit(self):
        report = initial_report()
        self.assertEqual(report["overall_status"], "incomplete")
        self.assertEqual(set(report["results"]), set(SDK_NAMES))
        self.assertEqual(report["results"]["go"]["status"], "blocked")
        self.assertIn("owner-qualified Go module identity", report["results"]["go"]["reason"])
        self.assertEqual(report["results"]["python"]["status"], "not_run")
        validate_report(report)

    def test_require_all_rejects_incomplete_matrix(self):
        with self.assertRaisesRegex(DriverError, "incomplete"):
            require_complete(initial_report())

    def test_go_cannot_be_forged_as_passed_before_driver_identity_gate_changes(self):
        report = initial_report()
        report["results"]["go"] = {
            "status": "passed",
            "reason": "",
            "artifacts": [
                {"filename": "core.zip", "sha256": "0" * 64},
                {"filename": "redis.zip", "sha256": "1" * 64},
            ],
            "commands": [
                {"argv": ["build"], "exit_code": 0},
                {"argv": ["consume"], "exit_code": 0},
            ],
            "consumer_workspace": "unused",
        }
        with self.assertRaisesRegex(DriverError, "Go must remain BLOCKED"):
            validate_report(report)

    def test_schema_allows_future_confirmed_go_pass_but_requires_all_sdk_passes(self):
        schema = json.loads((ROOT / "tools/package_consumer_matrix.schema.json").read_text(encoding="utf-8"))
        go_constraints = schema["properties"]["results"]["properties"]["go"]["allOf"]
        self.assertEqual(go_constraints[1]["properties"]["status"]["enum"], ["blocked", "passed"])
        passed_gate = schema["allOf"][0]["then"]["properties"]["results"]["properties"]
        self.assertEqual(set(passed_gate), set(SDK_NAMES))
        self.assertTrue(all(item["properties"]["status"]["const"] == "passed"
                            for item in passed_gate.values()))

    def test_external_driver_nonzero_exit_cannot_be_aggregated_as_passed(self):
        child = {
            "schema_version": 1,
            "status": "passed",
            "reason": "fixture said pass",
            "artifacts": [{"filename": "package.nupkg", "sha256": "a" * 64}],
            "commands": [{"argv": ["build"], "exit_code": 0}],
            "consumer_workspace": "outside-source-tree",
        }
        command = {"argv": ["driver"], "exit_code": 7}
        result = normalize_driver_cell("dotnet", child, 7, command, Path("scratch"))
        self.assertEqual(result["status"], "failed")
        self.assertIn("exited nonzero", result["reason"])
        self.assertEqual(result["commands"][-1]["exit_code"], 7)

    def test_external_driver_must_report_a_reason(self):
        child = {
            "schema_version": 1,
            "status": "passed",
            "reason": None,
            "artifacts": [{"filename": "package.jar", "sha256": "b" * 64}],
            "commands": [{"argv": ["build"], "exit_code": 0}],
            "consumer_workspace": "external",
        }
        result = normalize_driver_cell("java", child, 0, {"argv": ["driver"], "exit_code": 0}, Path("scratch"))
        self.assertEqual(result["status"], "failed")
        self.assertIn("non-empty reason", result["reason"])

    def test_external_driver_artifact_map_is_normalized_for_aggregate(self):
        child = {
            "schema_version": 1,
            "status": "passed",
            "reason": "exact npm artifacts passed their consumers",
            "artifacts": {
                "core": {"filename": "dilock-core.tgz", "sha256": "a" * 64},
                "redis": {"filename": "dilock-redis.tgz", "sha256": "b" * 64},
            },
            "commands": [{"cwd": "sdk/typescript", "argv": ["npm", "test"], "exit_code": 0}],
            "consumers": {"core": "passed", "redis": "passed"},
            "consumer_workspace": "outside-source-tree",
        }
        result = normalize_driver_cell("typescript", child, 0, {"argv": ["driver"], "exit_code": 0}, Path("scratch"))
        self.assertEqual(result["status"], "passed")
        self.assertEqual([artifact["unit"] for artifact in result["artifacts"]], ["core", "redis"])
        self.assertEqual(result["consumer_checks"], {"core": "passed", "redis": "passed"})
        report = initial_report()
        report["results"]["typescript"] = result
        validate_report(report)

    def test_missing_offline_npm_artifact_is_blocked_not_failed(self):
        child = {
            "schema_version": 1,
            "status": "failed",
            "reason": "npm error ENOTCACHED: no cached response is available",
            "artifacts": {"core": {"filename": "dilock-core.tgz", "sha256": "c" * 64}},
            "commands": [{"argv": ["npm", "install", "--offline"], "exit_code": 1}],
            "consumer_workspace": "outside-source-tree",
        }
        result = normalize_driver_cell("typescript", child, 1, {"argv": ["driver"], "exit_code": 1}, Path("scratch"))
        self.assertEqual(result["status"], "blocked")
        self.assertIn("local cache", result["reason"])

    def test_missing_dotnet_targeting_pack_is_blocked_not_failed(self):
        child = {
            "schema_version": 1,
            "status": "failed",
            "reason": "NETSDK1127: Microsoft.NETCore.App targeting pack is not installed",
            "artifacts": [],
            "commands": [{"argv": ["dotnet", "pack"], "exit_code": 1}],
            "consumer_workspace": "outside-source-tree",
        }
        result = normalize_driver_cell("dotnet", child, 1, {"argv": ["driver"], "exit_code": 1}, Path("scratch"))
        self.assertEqual(result["status"], "blocked")
        self.assertIn("targeting pack", result["reason"])

    def test_missing_external_driver_is_not_run_not_passed(self):
        result = run_external_sdk_consumer(Path("nonexistent-checkout"), "java", Path("uncreated"))
        self.assertEqual(result["status"], "not_run")
        self.assertIn("missing", result["reason"].lower())

    def test_python_build_prefers_repository_tool_venv(self):
        pinned = Path("repo-venv-python")
        with patch("tools.package_consumers.venv_python", return_value=pinned), \
             patch.object(Path, "is_file", return_value=True):
            self.assertEqual(package_build_python(Path("repo")), pinned)

    def test_python_build_falls_back_when_repository_tool_venv_is_absent(self):
        with patch("tools.package_consumers.venv_python", return_value=Path("missing-python")), \
             patch.object(Path, "is_file", return_value=False):
            self.assertEqual(package_build_python(Path("repo")), Path(sys.executable))

    def test_python_build_can_use_an_external_configured_interpreter(self):
        configured = Path("external-build-python")
        with patch("tools.package_consumers.venv_python", return_value=Path("repo-python")), \
             patch.object(Path, "is_file", return_value=True):
            self.assertEqual(package_build_python(Path("repo"), configured), configured.resolve())

    def test_python_sdist_uses_the_configured_build_environment(self):
        if sys.platform == "win32":
            configured = Path("C:/external/venv/Scripts/python.exe")
            expected = Path("C:/external/venv/Lib/site-packages")
        else:
            configured = Path("/external/venv/bin/python")
            expected = Path("/external/venv/lib/site-packages")
        self.assertEqual(
            package_build_site_packages(Path("repo"), configured),
            expected,
        )

    def test_host_permission_and_missing_offline_wheel_are_blocked_not_failed(self):
        self.assertTrue(environment_blocked("ERROR: [WinError 5] Acceso denegado"))
        self.assertTrue(environment_blocked("No matching distribution found for grpcio==1.84.0"))
        self.assertFalse(environment_blocked("AssertionError: package has the wrong API"))

    def test_sdist_extraction_rejects_path_traversal(self):
        with self.assertRaisesRegex(DriverError, "unsafe path"):
            safe_sdist_target(Path("scratch/source"), "package/../../outside.py")
        with self.assertRaisesRegex(DriverError, "unsafe path"):
            safe_sdist_target(Path("scratch/source"), "C:\\outside.py")


if __name__ == "__main__":
    unittest.main()
