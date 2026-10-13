from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from conformance.etcd_fencing_resource import FencedResource, ROOT


class FencedResourceTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory(
            prefix="dilock-fence-resource-", dir=ROOT / ".tmp"
        )
        self.resource = FencedResource(
            Path(self._temporary.name) / "resource.sqlite3", "shared", "epoch-a"
        )

    def tearDown(self):
        self._temporary.cleanup()

    def test_rejects_stale_or_replayed_revision_without_mutating_value(self):
        self.assertTrue(self.resource.write("epoch-a", 10, "owner-a")["accepted"])
        self.assertTrue(self.resource.write("epoch-a", 11, "owner-b")["accepted"])

        for revision in (10, 11):
            with self.subTest(revision=revision):
                result = self.resource.write("epoch-a", revision, "stale-owner")
                self.assertEqual(result["reason"], "stale_fence")
                self.assertFalse(result["accepted"])
                self.assertEqual(
                    self.resource.current_value(),
                    {"owner": "owner-b", "epoch": "epoch-a", "revision": 11},
                )

    def test_wrong_epoch_does_not_advance_high_watermark(self):
        self.resource.write("epoch-a", 10, "owner-a")
        result = self.resource.write("epoch-old", 99, "restored-stale-owner")
        self.assertEqual(result["reason"], "epoch_mismatch")
        self.assertFalse(result["accepted"])
        self.assertEqual(
            self.resource.current_value(),
            {"owner": "owner-a", "epoch": "epoch-a", "revision": 10},
        )
        self.assertTrue(self.resource.write("epoch-a", 11, "owner-b")["accepted"])

    def test_concurrent_equal_revisions_have_one_commit(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(
                lambda owner: self.resource.write("epoch-a", 20, owner),
                ("owner-a", "owner-b"),
            ))
        self.assertEqual(sum(result["accepted"] for result in results), 1)
        accepted_owner = next(result for result in results if result["accepted"])
        self.assertEqual(accepted_owner["high"], 20)
        self.assertIn(self.resource.current_value()["owner"], ("owner-a", "owner-b"))


if __name__ == "__main__":
    unittest.main()
