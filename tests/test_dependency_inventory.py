"""Focused tests for the local dependency inventory evidence parsers."""

from __future__ import annotations

import hashlib
import sys
import unittest
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import dependency_inventory as inventory  # noqa: E402


class DependencyInventoryTests(unittest.TestCase):
    def test_nuget_parser_uses_resolved_lock_version_and_hash(self) -> None:
        path = inventory.ROOT / "sdk/dotnet/Dilock.Redis/packages.lock.json"
        rows = inventory.parse_nuget_lock(path)
        row = next(row for row in rows if row["name"] == "StackExchange.Redis")
        self.assertEqual(row["purl"], f"pkg:nuget/StackExchange.Redis@{row['version']}")
        self.assertEqual(row["hashes"][0]["alg"], "SHA-512")
        props = {p["name"]: p["value"] for p in row["properties"]}
        self.assertEqual(props["dilock:source-file-sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(props["dilock:dependency-scope"], "Direct")

    def test_npm_parser_keeps_scoped_name_and_integrity(self) -> None:
        path = inventory.ROOT / "sdk/typescript/package-lock.json"
        rows = inventory.parse_npm_lock(path)
        row = next(row for row in rows if row["name"] == "@grpc/grpc-js")
        self.assertEqual(row["purl"], f"pkg:npm/@grpc/grpc-js@{row['version']}")
        self.assertEqual(row["hashes"][0]["alg"], "SHA-512")

    def test_cargo_and_go_keep_only_resolved_registry_evidence(self) -> None:
        cargo = inventory.ROOT / "sdk/rust/Cargo.lock"
        cargo_rows = inventory.parse_cargo_lock(cargo)
        self.assertTrue(cargo_rows)
        self.assertTrue(all(len(row["hashes"][0]["content"]) == 64 for row in cargo_rows))

        go = inventory.ROOT / "sdk/go/go.sum"
        go_rows = inventory.parse_go_sum(go)
        self.assertTrue(go_rows)
        self.assertTrue(all("hashes" not in row for row in go_rows))
        self.assertTrue(all(any(p["name"] == "dilock:go-module-h1" for p in row["properties"]) for row in go_rows))

    def test_python_requires_exact_pin_and_java_resolves_local_property(self) -> None:
        py = inventory.ROOT / "sdk/python/pyproject.toml"
        py_rows = inventory.parse_python_project(py)
        pyproject = tomllib.loads(py.read_text(encoding="utf-8"))
        declarations = list(pyproject["build-system"]["requires"])
        project = pyproject["project"]
        declarations.extend(project.get("dependencies", []))
        for extra, optional in project.get("optional-dependencies", {}).items():
            if extra != "test":
                declarations.extend(optional)
        exact_declarations = [declaration for declaration in declarations if "==" in declaration]
        self.assertEqual(
            {row["name"] for row in py_rows},
            {declaration.split("==", maxsplit=1)[0].strip() for declaration in exact_declarations},
        )
        for row in py_rows:
            requirement = next(
                prop["value"] for prop in row["properties"]
                if prop["name"] == "dilock:declared-requirement"
            )
            self.assertEqual(requirement, f"{row['name']}=={row['version']}")
        self.assertTrue(all(any(p["name"] == "dilock:evidence-quality" and "no-transitive-lock" in p["value"] for p in r["properties"]) for r in py_rows))

        pom = inventory.ROOT / "sdk/java/redis/pom.xml"
        java_rows = inventory.parse_java_pom(pom)
        jedis = next(row for row in java_rows if row["name"] == "redis.clients:jedis")
        namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
        parent_pom = ET.parse(inventory.ROOT / "sdk/java/pom.xml").getroot()
        self.assertEqual(
            jedis["version"],
            parent_pom.findtext("m:properties/m:jedis.version", namespaces=namespace),
        )
        self.assertEqual(jedis["purl"], f"pkg:maven/redis.clients/jedis@{jedis['version']}")
        etcd_rows = inventory.parse_java_pom(inventory.ROOT / "sdk/java/etcd/pom.xml")
        jetcd = next(row for row in etcd_rows if row["name"] == "io.etcd:jetcd-grpc")
        self.assertEqual(
            jetcd["version"],
            parent_pom.findtext("m:properties/m:jetcd.version", namespaces=namespace),
        )

    def test_inventory_marks_missing_transitive_evidence_and_go_owner(self) -> None:
        bom, report = inventory.inventory(inventory.ROOT)
        by_sdk = {row["sdk"]: row for row in report["coverage"]}
        self.assertEqual(len(by_sdk), 7)
        self.assertEqual(by_sdk["cpp"]["status"], "unknown")
        self.assertIn("owner-qualified path remains unresolved", by_sdk["go"]["notes"])
        self.assertFalse(report["complete"])
        self.assertFalse(report["network_accessed"])
        java_names = {row["name"] for row in bom["components"] if row["type"] == "library" and row["purl"].startswith("pkg:maven/")}
        self.assertIn("redis.clients:jedis", java_names)
        self.assertIn("io.etcd:jetcd-grpc", java_names)
        self.assertNotIn("org.dilock:dilock-core", java_names)
        self.assertFalse(any("${" in row["version"] for row in bom["components"]))


if __name__ == "__main__":
    unittest.main()
