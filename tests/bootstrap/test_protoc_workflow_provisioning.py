"""Keep Rust CI's protoc toolchain explicitly pinned and checksum-verified."""
from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
PROTOC_VERSION = "31.1"
PROTOC_ARCHIVE = f"protoc-{PROTOC_VERSION}-linux-x86_64.zip"
PROTOC_SHA256 = "96553041f1a91ea0efee963cb16f462f5985b4d65365f3907414c360044d8065"
PROTOC_URL = (
    "https://github.com/protocolbuffers/protobuf/releases/download/"
    f"v{PROTOC_VERSION}/{PROTOC_ARCHIVE}"
)


def step_block(source: str, name: str) -> str:
    marker = f"      - name: {name}\n"
    start = source.index(marker)
    following = re.search(r"\n      - name: ", source[start + len(marker):])
    end = start + len(marker) + following.start() if following else len(source)
    return source[start:end]


class RustProtocWorkflowTests(unittest.TestCase):
    def test_pr_and_nightly_install_the_same_checked_release_asset(self):
        blocks = {}
        for workflow_name in ("pr.yml", "nightly.yml"):
            source = (ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")
            block = step_block(source, "Install pinned protoc for Rust etcd-client")
            blocks[workflow_name] = block
            self.assertIn(f"version={PROTOC_VERSION}", block)
            self.assertIn(
                '"https://github.com/protocolbuffers/protobuf/releases/download/'
                'v${version}/${archive}"',
                block,
            )
            self.assertIn('archive="protoc-${version}-linux-x86_64.zip"', block)
            self.assertIn(PROTOC_SHA256, block)
            self.assertIn("curl --fail --location --retry 0", block)
            self.assertRegex(block, r"sha256sum\s+--check\s+--status")
            self.assertIn('unzip -q "$download" -d "$install_dir"', block)
            self.assertIn('"$install_dir/bin/protoc" --version | grep -Fx "libprotoc $version"', block)
            self.assertIn('echo "PROTOC=$install_dir/bin/protoc" >> "$GITHUB_ENV"', block)
            self.assertIn('echo "$install_dir/bin" >> "$GITHUB_PATH"', block)
        run_blocks = {
            workflow: block.split("        run: |\n", 1)[1]
            for workflow, block in blocks.items()
        }
        self.assertEqual(run_blocks["pr.yml"], run_blocks["nightly.yml"])

    def test_provisioning_precedes_rust_compilation_in_both_workflows(self):
        pr = (ROOT / ".github/workflows/pr.yml").read_text(encoding="utf-8")
        nightly = (ROOT / ".github/workflows/nightly.yml").read_text(encoding="utf-8")
        self.assertLess(
            pr.index("Install pinned protoc for Rust etcd-client"),
            pr.index("Test Rust SDK"),
        )
        self.assertLess(
            nightly.index("Install pinned protoc for Rust etcd-client"),
            nightly.index("Build and test Rust SDK and conformance worker"),
        )
        self.assertIn("if: matrix.sdk == 'rust'", step_block(
            pr, "Install pinned protoc for Rust etcd-client"
        ))


if __name__ == "__main__":
    unittest.main()
