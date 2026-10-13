"""Release boundary regressions: frozen bytes, safe inputs and honest states."""
from __future__ import annotations

import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from tools import release


class CatalogReleaseTests(unittest.TestCase):
    def temporary(self):
        parent = Path(__file__).resolve().parents[2] / ".tmp" / "catalog-release-tests"
        parent.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(dir=parent)

    def fixture(self, root: Path):
        (root / "tools").mkdir()
        entries = []
        for key, deps in [("engine", []), ("facade", ["engine"])]:
            (root / "sdk" / key).mkdir(parents=True)
            (root / "sdk" / key / "source.txt").write_text(key)
            entries.append({"id": key, "source_include": [f"sdk/{key}/**"], "dependencies": deps,
                            "delivery_kind": "native" if not deps else "facade", "build": {}})
        (root / "LICENSE").write_text("license")
        release.write(root / release.CATALOG, {"schema_version": 1, "entries": entries})

    def test_catalog_requires_unique_sdk_ids(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"].append(data["entries"][0])
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "each SDK exactly once"):
                release.catalog(root)

    def test_pruning_excludes_evidence_vendor_and_local_database(self):
        with self.temporary() as folder:
            root = Path(folder)
            for name in ["sdk/x/source.py", "sdk/x/validation/a.json", "sdk/x/vendor/private.txt",
                         "sdk/x/.npmrc", "sdk/x/.env.local", ".tools/key.txt",
                         ".local-review/notes.md", "planning/tasks.json", "dump.rdb"]:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("content")
            self.assertEqual(release.source_files(root), ["sdk/x/source.py"])

    def test_snapshot_contains_dependency_and_license_but_is_not_native_pass(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            manifest = release.prepare(root, output, "0.1.0-beta.1", ["facade"])
            self.assertEqual(manifest["scope"], ["engine", "facade"])
            with tarfile.open(output / manifest["results"]["facade"]["source_delivery"]["path"]) as archive:
                self.assertIn("sdk/engine/source.txt", archive.getnames())
                self.assertIn("LICENSE", archive.getnames())
                metadata = json.load(archive.extractfile("DILOCK-SOURCE-DELIVERY.json"))
                self.assertFalse(metadata["compiled"])
            self.assertEqual(release.summary(manifest)["native_builds"], {"NOT_RUN": 2})
            self.assertEqual(release.summary(manifest)["beta_eligible"], 0)
            release.integrity(output)

    def test_mutated_frozen_source_and_package_are_rejected(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            manifest = release.prepare(root, output, "0.1.0-beta.1", ["engine"])
            path = output / "source/sdk/engine/source.txt"
            original = path.read_bytes()
            path.write_text("different")
            with self.assertRaisesRegex(release.ReleaseError, "frozen source changed"):
                release.integrity(output)
            path.write_bytes(original)
            package = output / manifest["results"]["engine"]["source_delivery"]["path"]
            package.write_bytes(b"corrupted")
            with self.assertRaisesRegex(release.ReleaseError, "candidate artifact changed"):
                release.integrity(output)

    def test_archive_is_repeatable_and_candidate_cannot_be_overwritten(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            first = root / "artifacts/first"
            second = root / "artifacts/second"
            a = release.prepare(root, first, "0.1.0-beta.1", ["engine"])
            b = release.prepare(root, second, "0.1.0-beta.1", ["engine"])
            self.assertEqual(a["results"]["engine"]["source_delivery"]["sha256"],
                             b["results"]["engine"]["source_delivery"]["sha256"])
            with self.assertRaisesRegex(release.ReleaseError, "already exists"):
                release.prepare(root, first, "0.1.0-beta.1", ["engine"])

    def test_cycles_traversal_and_mutable_images_are_rejected(self):
        with self.assertRaises(release.ReleaseError):
            release.ordered({"a": {"dependencies": ["b"]}, "b": {"dependencies": ["a"]}}, ["a"])
        for relative in ["../secrets", "/etc/passwd", "C:/secrets", "sdk\\secrets"]:
            with self.assertRaises(release.ReleaseError):
                release.inside(Path.cwd(), relative)
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["build"]["image"] = "python:latest"
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "immutable"):
                release.catalog(root)

    def test_publisher_is_not_a_recipe(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["build"]["argv"] = [["npm", "publish"]]
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "publisher command forbidden"):
                release.catalog(root)

    def test_split_package_output_must_cover_each_unit(self):
        artifacts = [{"path": "builds/typescript/native/dilock-core-0.1.0.tgz"},
                     {"path": "builds/typescript/native/dilock-redis-0.1.0.tgz"}]
        self.assertEqual(release.missing_artifacts(artifacts,
                         ["dilock-core-*.tgz", "dilock-redis-*.tgz", "dilock-etcd-*.tgz"]),
                         ["dilock-etcd-*.tgz"])

    def test_both_verification_flags_require_consumers_even_after_build_pass(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            manifest = release.prepare(root, output, "0.1.0-beta.1", ["engine"])
            manifest["results"]["engine"]["native_build"]["state"] = "PASS"
            release.write(output / "manifest.json", manifest)
            with patch("sys.argv", ["release.py", "--repo", str(root), "verify", "--candidate", str(output),
                                    "--require-native", "--require-consumers"]):
                self.assertEqual(release.main(), 1)

    def test_summary_distinguishes_native_packages_from_source_and_facade_outputs(self):
        base = {
            "source_delivery": {"state": "PASS"},
            "installed_consumer": {"state": "NOT_RUN"},
            "beta_eligible": False,
        }
        manifest = {"results": {
            "native": {**base, "native_build": {"state": "PASS", "output_role": "package"}},
            "source": {**base, "native_build": {"state": "PASS", "output_role": "build-validation"}},
            "facade": {**base, "native_build": {"state": "PASS", "output_role": "facade-component"}},
        }}
        report = release.summary(manifest)
        self.assertEqual(report["native_package_builds"], {"PASS": 1})
        self.assertEqual(report["source_build_validations"], {"PASS": 1})
        self.assertEqual(report["other_delivery_builds"], {"PASS": 1})


if __name__ == "__main__":
    unittest.main()
