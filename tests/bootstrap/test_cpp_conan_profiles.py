"""Keep CI's Conan dependency profiles aligned with the C++ build profiles."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CppConanProfileWorkflowTests(unittest.TestCase):
    def test_nightly_requests_the_optional_etcd_conan_profile(self):
        nightly = (ROOT / ".github/workflows/nightly.yml").read_text(encoding="utf-8")
        self.assertIn(
            'conan install sdk/cpp/conanfile.py --output-folder .tools/ci/cpp-deps '
            '--build=missing -s build_type=Release -o "&:with_etcd=True"',
            nightly,
        )
        self.assertIn("-DDILOCK_BUILD_ETCD=ON", nightly)

    def test_pr_keeps_the_default_redis_only_conan_profile(self):
        recipe = (ROOT / "sdk/cpp/conanfile.py").read_text(encoding="utf-8")
        pr = (ROOT / ".github/workflows/pr.yml").read_text(encoding="utf-8")
        self.assertIn(
            'default_options = {"with_etcd": False, "with_consul": False, "shared": False}',
            recipe,
        )
        self.assertIn("conan install conanfile.py --output-folder ../../.tools/ci/cpp-deps --build=missing -s build_type=Release", pr)
        self.assertIn("-DDILOCK_BUILD_ETCD=OFF", pr)


if __name__ == "__main__":
    unittest.main()
