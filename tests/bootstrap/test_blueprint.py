"""Tests of blueprint helpers only; no distributed-lock runtime is exercised."""
from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from tools.bootstrap_check import (
    check_task_graph,
    is_generated,
    is_packaged_validation_snapshot,
    is_validation_workspace_snapshot,
    iter_source_files,
)
from tools.check_name import lookup, targets
from tools.reference_identity import new_redis_owner_value, redis_key, resource_id
from tools.release_readiness import unresolved_checks

ROOT = Path(__file__).resolve().parents[2]


class IdentityTests(unittest.TestCase):
    def test_all_valid_golden_vectors(self):
        vectors = json.loads((ROOT / "spec/vectors/identity.json").read_text(encoding="utf-8"))
        for item in vectors["valid"]:
            with self.subTest(item=item["id"]):
                self.assertEqual(resource_id(item["namespace"], item["resource"]), item["sha256"])
                self.assertEqual(redis_key(item["namespace"], item["resource"]), item["redis_key"])
                self.assertEqual(item["etcd_lock_name"], "/dilock/locks/" + item["sha256"])

    def test_all_invalid_golden_vectors(self):
        vectors = json.loads((ROOT / "spec/vectors/identity.json").read_text(encoding="utf-8"))
        for item in vectors["invalid"]:
            with self.subTest(item=item["id"]):
                with self.assertRaises(ValueError):
                    resource_id(item["namespace"], item["resource"])

    def test_normalization_is_not_implicit(self):
        self.assertNotEqual(resource_id("x", "café"), resource_id("x", "cafe\u0301"))
        self.assertNotEqual(resource_id("x", "Resource"), resource_id("x", "resource"))
        self.assertNotEqual(resource_id("x", "resource"), resource_id("x", "resource "))

    def test_namespace_changes_identity(self):
        self.assertNotEqual(resource_id("a", "r"), resource_id("b", "r"))

    def test_owner_generation_has_exact_format_and_length(self):
        values = [new_redis_owner_value() for _ in range(100)]
        self.assertTrue(all(re.fullmatch(r"dilock/1/[0-9a-f]{64}", value) for value in values))
        self.assertEqual(len(set(values)), 100)  # Smoke test, not a proof of entropy.


class TaskGraphTests(unittest.TestCase):
    def test_valid_graph(self):
        check_task_graph([{"id": "a", "depends_on": []}, {"id": "b", "depends_on": ["a"]}])

    def test_cycle_rejected(self):
        with self.assertRaises(ValueError):
            check_task_graph([{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}])

    def test_missing_dependency_rejected(self):
        with self.assertRaises(ValueError):
            check_task_graph([{"id": "a", "depends_on": ["missing"]}])

    def test_duplicate_task_rejected(self):
        with self.assertRaises(ValueError):
            check_task_graph([{"id": "a", "depends_on": []}, {"id": "a", "depends_on": []}])

    def test_root_temporary_outputs_are_excluded_from_source_checks(self):
        self.assertTrue(is_generated(ROOT / ".tmp" / "consumer" / "README.md"))
        self.assertTrue(is_generated(ROOT / "sdk" / "julia" / "validation" / ".tmp-online-depot" / "README.md"))
        self.assertTrue(is_generated(ROOT / "sdk" / "php" / "vendor" / "composer" / "README.md"))
        self.assertFalse(is_generated(ROOT / "docs" / "README.md"))

    def test_package_readme_snapshots_are_not_repo_docs(self):
        self.assertTrue(is_packaged_validation_snapshot(
            ROOT / "sdk" / "crystal" / "validation" / "consumer" / "package" / "README.md"
        ))
        self.assertTrue(is_packaged_validation_snapshot(
            ROOT / "sdk" / "dotnet" / "validation" / "fsharp-vb-current-source-exact-package-live-docker-20261010"
            / "consumers" / "fsharp" / "packages" / "dilock.core" / "README.md"
        ))
        self.assertFalse(is_packaged_validation_snapshot(
            ROOT / ".local-review" / "sdk-validation" / "crystal" / "VALIDATION.md"
        ))

    def test_validation_workspace_copies_are_not_repo_docs(self):
        self.assertTrue(is_validation_workspace_snapshot(
            ROOT / "sdk" / "go" / "validation" / "run-20261010" / "workspace" / "sdk" / "go" / "README.md"
        ))
        self.assertTrue(is_validation_workspace_snapshot(
            ROOT / "sdk" / "go" / "validation" / "run-20261010" / "packages" / "sdk" / "go" / "README.md"
        ))
        self.assertFalse(is_validation_workspace_snapshot(
            ROOT / "sdk" / "go" / "README.md"
        ))

    def test_source_walk_prunes_generated_trees_before_descent(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            (root / "docs").mkdir()
            (root / ".tools" / "deep" / "cache").mkdir(parents=True)
            (root / "sdk" / "julia" / "validation" / ".tmp-online-depot" / "packages").mkdir(parents=True)
            (root / "node_modules" / "nested").mkdir(parents=True)
            (root / "vendor" / "third-party" / "nested").mkdir(parents=True)
            (root / "docs" / "guide.md").write_text("source", encoding="utf-8")
            (root / ".tools" / "deep" / "cache" / "manifest.json").write_text("{}", encoding="utf-8")
            (root / "node_modules" / "nested" / "README.md").write_text("generated", encoding="utf-8")
            (root / "sdk" / "julia" / "validation" / ".tmp-online-depot" / "packages" / "README.md").write_text("generated", encoding="utf-8")
            (root / "vendor" / "third-party" / "nested" / "README.md").write_text("upstream", encoding="utf-8")

            self.assertEqual(list(iter_source_files(root, ".md")), [root / "docs" / "guide.md"])
            self.assertEqual(list(iter_source_files(root, ".json")), [])


class SupportMatrixTests(unittest.TestCase):
    def test_unconformant_sdks_do_not_claim_another_sdks_runtime_pass(self):
        matrix_path = ROOT / "planning/support-matrix.json"
        if not matrix_path.is_file():
            self.skipTest("workspace-only support evidence is not part of the source delivery")
        matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
        unconformant = [entry for entry in matrix["entries"] if entry["status"] != "conformant"]
        self.assertTrue(unconformant)
        for entry in unconformant:
            with self.subTest(sdk=entry["sdk"], backend=entry["backend"]):
                self.assertFalse(any("passed for this SDK" in item for item in entry["evidence"]))


class EvidenceTests(unittest.TestCase):
    def test_pending_is_not_ready(self):
        self.assertEqual(unresolved_checks({"checks": [{"id": "test", "state": "pending", "evidence": []}]}), ["test"])

    def test_verified_without_evidence_is_not_ready(self):
        self.assertEqual(unresolved_checks({"checks": [{"id": "test", "state": "verified", "evidence": []}]}), ["test"])

    def test_blank_evidence_is_not_ready(self):
        self.assertEqual(unresolved_checks({"checks": [{"id": "test", "state": "verified", "evidence": [" "]}]}), ["test"])

    def test_verified_with_evidence_is_structurally_complete_only(self):
        self.assertEqual(unresolved_checks({"checks": [{"id": "test", "state": "verified", "evidence": ["report-id"]}]}), [])

    def test_empty_or_duplicate_evidence_document_rejected(self):
        for value in ({"checks": []}, {"checks": [{"id":"a"}, {"id":"a"}]}):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    unresolved_checks(value)


class NameCheckTests(unittest.TestCase):
    def test_expected_targets(self):
        output = targets("dilock")
        self.assertEqual(len(output), 10)
        self.assertTrue(all(url.startswith("https://") for _, url in output))

    def test_bad_slug_rejected(self):
        for slug in ("../bad", "BAD", "", "a/b", "https://example.com"):
            with self.subTest(slug=slug):
                with self.assertRaises(ValueError):
                    targets(slug)

    def test_network_error_is_unverified_not_available(self):
        with patch("tools.check_name.urlopen", side_effect=URLError("offline")):
            result = lookup("registry", "https://example.invalid/test", 1)
        self.assertEqual(result["result"], "unverified")

    def test_404_does_not_prove_reservability(self):
        error = HTTPError("https://example.invalid/test", 404, "Not Found", {}, BytesIO())
        with patch("tools.check_name.urlopen", side_effect=error):
            result = lookup("registry", "https://example.invalid/test", 1)
        self.assertEqual(result["result"], "not_found_not_reserved")

    def test_rate_limit_is_unverified(self):
        error = HTTPError("https://example.invalid/test", 429, "Too many requests", {}, BytesIO())
        with patch("tools.check_name.urlopen", side_effect=error):
            result = lookup("registry", "https://example.invalid/test", 1)
        self.assertEqual(result["result"], "unverified")


if __name__ == "__main__":
    unittest.main()
