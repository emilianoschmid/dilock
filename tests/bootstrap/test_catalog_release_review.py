"""Independent review regressions for candidate integrity and recipe validation."""
from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools import release


class CatalogReleaseReviewTests(unittest.TestCase):
    def temporary(self):
        parent = Path(__file__).resolve().parents[2] / ".tmp" / "catalog-release-review-tests"
        parent.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(dir=parent)

    def fixture(self, root: Path):
        (root / "tools").mkdir()
        (root / "sdk" / "engine").mkdir(parents=True)
        (root / "sdk" / "engine" / "source.txt").write_text("recorded input")
        (root / "LICENSE").write_text("license")
        release.write(root / release.CATALOG, {"schema_version": 1, "entries": [{
            "id": "engine", "source_include": ["sdk/engine/**"], "dependencies": [],
            "delivery_kind": "native", "build": {}, "consumer": {},
        }]})

    def test_unrecorded_frozen_build_input_is_rejected(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            release.prepare(root, output, "0.1.0-beta.1", ["engine"])
            # Builders copy the complete snapshot. Added configuration is an
            # unrecorded build input even though all originally hashed files match.
            (output / "source" / "Directory.Build.props").write_text("extra compiler input")
            with self.assertRaises(release.ReleaseError):
                release.integrity(output)

    def test_added_cache_tree_is_an_unrecorded_build_input(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            release.prepare(root, output, "0.1.0-beta.1", ["engine"])
            # Snapshot validation must enumerate everything copied by a builder;
            # workspace pruning rules must not hide subsequently added caches.
            injected = output / "source" / "sdk" / "engine" / "node_modules" / "package.json"
            injected.parent.mkdir(parents=True)
            injected.write_text('{"name":"unrecorded"}')
            with self.assertRaises(release.ReleaseError):
                release.integrity(output)

    def test_consumer_image_must_be_immutable(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["consumer"] = {"image": "python:latest", "argv": [["python", "-V"]]}
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "immutable"):
                release.catalog(root)

    def test_consumer_argv_must_be_structured_strings(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["consumer"] = {"argv": ["python -V"]}
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "argv"):
                release.catalog(root)

    def test_consumer_recipe_cannot_publish(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["consumer"] = {"argv": [["npm", "publish"]]}
            release.write(root / release.CATALOG, data)
            with self.assertRaisesRegex(release.ReleaseError, "publisher"):
                release.catalog(root)

    def test_unrecorded_staged_package_cannot_enter_consumer_globs(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            output = root / "artifacts/candidate"
            manifest = release.prepare(root, output, "0.1.0-beta.1", ["engine"])
            packages = release.stage_packages(output, manifest, ["engine"])
            # Consumers use artifact globs; a stale unrecorded file must never
            # become an additional dependency merely by existing in this mount.
            (packages / "engine" / "stale-package.tgz").write_bytes(b"unrecorded artifact")
            with self.assertRaises(release.ReleaseError):
                release.stage_packages(output, manifest, ["engine"])

    def test_cleanup_timeout_preserves_original_failed_attempt(self):
        with self.temporary() as folder:
            root = Path(folder)
            self.fixture(root)
            data = release.read(root / release.CATALOG)
            data["entries"][0]["build"] = {
                "image": "sha256:" + "0" * 64, "cwd": "sdk/engine",
                "argv": [["true"]], "output_role": "build-validation",
            }
            release.write(root / release.CATALOG, data)
            output = root / "artifacts/candidate"
            release.prepare(root, output, "0.1.0-beta.1", ["engine"])

            def timeout_after_start(argv, timeout=20):
                if argv[1] == "run":
                    raise subprocess.TimeoutExpired(argv, timeout, output=b"compiler started\n", stderr=b"compiler diagnostic\n")
                if argv[1] == "rm":
                    raise subprocess.TimeoutExpired(argv, timeout)
                return subprocess.CompletedProcess(argv, 0, "cached image\n", "")

            with patch.object(release, "run", side_effect=timeout_after_start):
                manifest = release.build(output, ["engine"], 1, root)
            self.assertEqual(manifest["results"]["engine"]["native_build"]["state"], "FAILED")
            self.assertEqual(release.read(output / "manifest.json")["results"]["engine"]["native_build"]["state"], "FAILED")
            self.assertIn("compiler started", (output / "builds/engine/stdout.log").read_text())
            self.assertIn("compiler diagnostic", (output / "builds/engine/stderr.log").read_text())


if __name__ == "__main__":
    unittest.main()
