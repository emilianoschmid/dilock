"""Generate and verify a local staged-artifact inventory and partial CycloneDX SBOM.

The explicit input allowlist is intentionally narrower than a release manifest:
it only describes build artifacts that are currently present in this checkout.
No registry, network, Git identity, owner, or license information is inferred.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from urllib.parse import quote
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / "tools/release-artifacts.json"
INVENTORY = ROOT / "validation/implementation/m6-local-artifact-inventory.json"
SBOM = ROOT / "validation/implementation/m6-local-artifact-sbom.cdx.json"


def _read_text(data: bytes) -> str:
    return data.decode("utf-8-sig")


def _metadata(path: Path, ecosystem: str) -> tuple[str, str, str]:
    """Read package identity from the built artifact, not from its filename."""
    if ecosystem == "nuget":
        with zipfile.ZipFile(path) as archive:
            names = [
                name for name in archive.namelist() if name.lower().endswith(".nuspec")
            ]
            if len(names) != 1:
                raise ValueError(f"expected one nuspec in {path}")
            node = ElementTree.fromstring(archive.read(names[0]))

        def tag(name: str) -> ElementTree.Element | None:
            return next((n for n in node.iter() if n.tag.split("}")[-1] == name), None)

        metadata = tag("metadata")
        if metadata is None:
            raise ValueError(f"missing nuspec metadata in {path}")
        values = {n.tag.split("}")[-1]: (n.text or "").strip() for n in metadata}
        name, version = values.get("id", ""), values.get("version", "")
        return name, version, f"pkg:nuget/{name.lower()}@{version}"
    if ecosystem == "pypi":
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as archive:
                candidates = [
                    n for n in archive.namelist() if n.endswith(".dist-info/METADATA")
                ]
                if len(candidates) != 1:
                    raise ValueError(f"expected one wheel METADATA in {path}")
                data = archive.read(candidates[0])
        else:
            with tarfile.open(path, "r:gz") as archive:
                candidates = [
                    m for m in archive.getmembers() if m.name.endswith("/PKG-INFO")
                ]
                if len(candidates) != 1:
                    raise ValueError(f"expected one sdist PKG-INFO in {path}")
                stream = archive.extractfile(candidates[0])
                if stream is None:
                    raise ValueError(f"unreadable sdist metadata in {path}")
                data = stream.read()
        fields = _headers(_read_text(data))
        name, version = fields.get("name", ""), fields.get("version", "")
        return name, version, f"pkg:pypi/{name.lower()}@{version}"
    if ecosystem == "maven":
        with zipfile.ZipFile(path) as archive:
            properties = [
                n
                for n in archive.namelist()
                if n.startswith("META-INF/maven/") and n.endswith("/pom.properties")
            ]
            if len(properties) != 1:
                raise ValueError(f"expected one Maven pom.properties in {path}")
            fields = _properties(_read_text(archive.read(properties[0])))
        group, name, version = (
            fields.get("groupId", ""),
            fields.get("artifactId", ""),
            fields.get("version", ""),
        )
        return name, version, f"pkg:maven/{group}/{name}@{version}"
    if ecosystem == "cargo":
        import tomllib

        with tarfile.open(path, "r:gz") as archive:
            manifests = [
                m
                for m in archive.getmembers()
                if PurePosixPath(m.name).name == "Cargo.toml"
            ]
            if len(manifests) != 1:
                raise ValueError(f"expected one Cargo.toml in {path}")
            stream = archive.extractfile(manifests[0])
            if stream is None:
                raise ValueError(f"unreadable Cargo.toml in {path}")
            package = tomllib.loads(_read_text(stream.read())).get("package", {})
        name, version = package.get("name", ""), package.get("version", "")
        return name, version, f"pkg:cargo/{name}@{version}"
    if ecosystem == "npm":
        with tarfile.open(path, "r:gz") as archive:
            candidates = [
                m
                for m in archive.getmembers()
                if PurePosixPath(m.name).name == "package.json"
            ]
            if len(candidates) != 1:
                raise ValueError(f"expected one package.json in {path}")
            stream = archive.extractfile(candidates[0])
            if stream is None:
                raise ValueError(f"unreadable package.json in {path}")
            package = json.loads(_read_text(stream.read()))
        name, version = package.get("name", ""), package.get("version", "")
        return name, version, f"pkg:npm/{quote(name, safe='/')}@{version}"
    if ecosystem == "cmake":
        # CMake's install/export layout is the package metadata for this local
        # archive. Read identity from its generated ConfigVersion file rather
        # than trusting the staged filename.
        with tarfile.open(path, "r:gz") as archive:
            members = [m for m in archive.getmembers() if m.isfile()]
            versions = [
                m
                for m in members
                if m.name.endswith("/lib/cmake/DilockCpp/DilockCppConfigVersion.cmake")
            ]
            configs = [
                m
                for m in members
                if m.name.endswith("/lib/cmake/DilockCpp/DilockCppConfig.cmake")
            ]
            if len(versions) != 1 or len(configs) != 1:
                raise ValueError(
                    f"expected one DilockCpp CMake config and version file in {path}"
                )
            stream = archive.extractfile(versions[0])
            if stream is None:
                raise ValueError(f"unreadable DilockCpp version metadata in {path}")
            import re

            match = re.search(
                r'^set\(PACKAGE_VERSION "([^"]+)"\)$',
                _read_text(stream.read()),
                re.MULTILINE,
            )
            if match is None:
                raise ValueError(f"missing PACKAGE_VERSION in {path}")
            version = match.group(1)
        return (
            "DilockCpp",
            version,
            f"pkg:generic/dilock-cpp-cmake@{version}?arch=x86_64&distro=linux",
        )
    raise ValueError(f"unsupported ecosystem: {ecosystem}")


def _headers(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            break
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip().lower()] = value.strip()
    return result


def _properties(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")) and "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def _package_format(path: Path) -> str:
    lower = path.name.lower()
    if lower.endswith(".tar.gz"):
        return "tar.gz"
    return path.suffix.removeprefix(".").lower()


def build_documents(
    timestamp: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    config = json.loads(INPUTS.read_text(encoding="utf-8"))
    records = []
    components: list[dict[str, Any]] = []
    paths: set[str] = set()
    for item in config["artifacts"]:
        rel = item["path"]
        if rel in paths:
            raise ValueError(f"duplicate artifact path: {rel}")
        paths.add(rel)
        path = (ROOT / rel).resolve()
        if ROOT not in path.parents or not path.is_file():
            raise ValueError(f"artifact is missing or outside repository: {rel}")
        name, version, purl = _metadata(path, item["ecosystem"])
        if not name or not version:
            raise ValueError(f"artifact has incomplete identity metadata: {rel}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        records.append(
            {
                "path": rel,
                "ecosystem": item["ecosystem"],
                "name": name,
                "version": version,
                "package_format": _package_format(path),
                "purl": purl,
                "sha256": digest,
                "bytes": path.stat().st_size,
                "exact_artifact_consumer_tested": bool(item["consumer_tested"]),
            }
        )
        # Each hash identifies one exact archive. Do not combine different
        # files with the same package URL into one component: a Python wheel
        # and sdist have different bytes, and duplicate same-version NuGet
        # archives can expose an accidental rebuild/version collision.
        component = {
            "bom-ref": "urn:dilock:staged-artifact:" + quote(rel, safe="/-._~"),
            "type": "library",
            "name": name,
            "version": version,
            "purl": purl,
            "hashes": [{"alg": "SHA-256", "content": digest}],
            "properties": [{"name": "dilock:staged-artifact", "value": rel}],
        }
        components.append(component)

    excluded_artifacts = []
    for item in config.get("excluded_artifacts", []):
        rel = item["path"]
        if rel in paths:
            raise ValueError(f"artifact cannot be both staged and excluded: {rel}")
        path = (ROOT / rel).resolve()
        if ROOT not in path.parents or not path.is_file():
            raise ValueError(f"excluded artifact is missing or outside repository: {rel}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        expected = item.get("sha256")
        if expected and digest != expected:
            raise ValueError(f"excluded artifact checksum changed: {rel}")
        excluded_artifacts.append(
            {"path": rel, "sha256": digest, "reason": item["reason"]}
        )

    artifact_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for record in records:
        identity = (
            record["ecosystem"],
            record["name"],
            record["version"],
            record["package_format"],
        )
        artifact_groups.setdefault(identity, []).append(record)
    conflicts = [
        {
            "ecosystem": identity[0],
            "name": identity[1],
            "version": identity[2],
            "package_format": identity[3],
            "artifacts": [r["path"] for r in group],
            "sha256": sorted({r["sha256"] for r in group}),
        }
        for identity, group in artifact_groups.items()
        if len({r["sha256"] for r in group}) > 1
    ]

    inventory = {
        "schema_version": 1,
        "status": "partial_local_inventory",
        "source": "tools/release-artifacts.json",
        "artifact_count": len(records),
        "artifacts": records,
        "excluded_artifacts": excluded_artifacts,
        "same_identity_different_bytes": conflicts,
        "not_represented": config["not_represented"],
        "limitations": [
            "Inventory covers only explicit local staged files; it does not cover every SDK distribution form.",
            "No registry or remote data was queried, and no package was published.",
            "Checksums prove byte identity for these files only, not provenance, reproducibility of builds, or completeness.",
        ],
    }
    created = timestamp or datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "timestamp": created,
            "properties": [
                {
                    "name": "dilock:scope",
                    "value": "staged-first-party-package-artifacts-only",
                },
                {"name": "dilock:dependency-closure", "value": "not-included"},
                {"name": "dilock:status", "value": "partial"},
            ],
        },
        "components": sorted(components, key=lambda c: (c["purl"], c["bom-ref"])),
        "compositions": [
            {
                "aggregate": "incomplete",
                "assemblies": sorted(c["bom-ref"] for c in components),
            }
        ],
    }
    return inventory, sbom


def _canonical(document: dict[str, Any]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--write",
        action="store_true",
        help="write current checksums and a newly timestamped SBOM",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="verify saved outputs against the current artifact bytes",
    )
    args = parser.parse_args()
    try:
        timestamp = None
        if args.check and SBOM.exists():
            timestamp = json.loads(SBOM.read_text(encoding="utf-8"))["metadata"][
                "timestamp"
            ]
        inventory, sbom = build_documents(timestamp)
        if args.write:
            INVENTORY.write_bytes(_canonical(inventory))
            SBOM.write_bytes(_canonical(sbom))
            print(
                f"Wrote {len(inventory['artifacts'])} local artifact records and a partial CycloneDX SBOM."
            )
            return 0
        failures = [
            str(path.relative_to(ROOT))
            for path, expected in ((INVENTORY, inventory), (SBOM, sbom))
            if not path.exists() or path.read_bytes() != _canonical(expected)
        ]
        if failures:
            print("STALE OR MISSING: " + ", ".join(failures))
            return 1
        print(
            f"PASS: {len(inventory['artifacts'])} staged artifact checksums and SBOM package metadata match current files."
        )
        return 0
    except (
        OSError,
        ValueError,
        KeyError,
        zipfile.BadZipFile,
        tarfile.TarError,
        ElementTree.ParseError,
    ) as exc:
        print(f"ARTIFACT INVENTORY ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
