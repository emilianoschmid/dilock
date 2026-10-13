"""Build a local-only, partial CycloneDX inventory from checked-in SDK metadata.

This deliberately does not resolve dependencies or contact package registries.
The JSON report describes per-ecosystem coverage and the exact evidence files
used to create each component.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "validation" / "implementation"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _component(
    ecosystem: str,
    name: str,
    version: str,
    source: Path,
    quality: str,
    *,
    purl: str | None = None,
    hashes: list[dict[str, str]] | None = None,
    properties: dict[str, str] | None = None,
) -> dict[str, Any]:
    props = {
        "dilock:ecosystem": ecosystem,
        "dilock:evidence-quality": quality,
        "dilock:source-file": source.relative_to(ROOT).as_posix(),
        "dilock:source-file-sha256": sha256(source),
    }
    props.update(properties or {})
    result: dict[str, Any] = {
        "type": "library",
        "name": name,
        "version": version,
        "properties": [{"name": k, "value": v} for k, v in sorted(props.items())],
    }
    if purl:
        result["purl"] = purl
    if hashes:
        result["hashes"] = hashes
    return result


def _b64_digest(value: str, algorithm: str) -> dict[str, str] | None:
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, base64.binascii.Error):
        return None
    expected = {"SHA-512": 64, "SHA-256": 32}.get(algorithm)
    if expected is None or len(raw) != expected:
        return None
    return {"alg": algorithm, "content": raw.hex()}


def parse_nuget_lock(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    result = []
    for framework, packages in data["dependencies"].items():
        for name, item in packages.items():
            if item.get("type") == "Project" or not item.get("resolved"):
                continue
            version = item["resolved"]
            hashes = _b64_digest(item.get("contentHash", ""), "SHA-512")
            result.append(
                _component(
                    "nuget", name, version, path, "lockfile-resolved",
                    purl=f"pkg:nuget/{name}@{version}",
                    hashes=[hashes] if hashes else None,
                    properties={
                        "dilock:dependency-scope": item.get("type", "unknown"),
                        "dilock:target-framework": framework,
                        "dilock:lock-content-hash": item.get("contentHash", ""),
                    },
                )
            )
    return result


def parse_npm_lock(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    result = []
    root = data.get("packages", {}).get("", {})
    direct = set(root.get("dependencies", {})) | set(root.get("devDependencies", {}))
    for package_path, item in data.get("packages", {}).items():
        if not package_path.startswith("node_modules/") or not item.get("version"):
            continue
        name = package_path[len("node_modules/"):]
        version = item["version"]
        integrity = item.get("integrity", "")
        hashes = []
        for part in integrity.split():
            match = re.fullmatch(r"(sha512|sha256)-([A-Za-z0-9+/=]+)", part)
            if match:
                converted = _b64_digest(match.group(2), match.group(1).upper().replace("SHA", "SHA-"))
                if converted:
                    hashes.append(converted)
        result.append(
            _component(
                "npm", name, version, path, "lockfile-resolved",
                purl=f"pkg:npm/{name}@{version}", hashes=hashes or None,
                properties={
                    "dilock:dependency-scope": "direct" if name in direct else "transitive-or-dev",
                    "dilock:integrity": integrity,
                    "dilock:resolved-url": item.get("resolved", ""),
                },
            )
        )
    return result


def parse_cargo_lock(path: Path) -> list[dict[str, Any]]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    result = []
    for package in data.get("package", []):
        if not package.get("source", "").startswith("registry+"):
            continue
        name, version = package["name"], package["version"]
        checksum = package.get("checksum")
        hashes = [{"alg": "SHA-256", "content": checksum}] if re.fullmatch(r"[0-9a-f]{64}", checksum or "") else None
        result.append(
            _component(
                "cargo", name, version, path, "lockfile-resolved",
                purl=f"pkg:cargo/{name}@{version}", hashes=hashes,
                properties={"dilock:registry-source": package["source"]},
            )
        )
    return result


def parse_go_sum(path: Path) -> list[dict[str, Any]]:
    result = []
    seen = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) != 3 or fields[1].endswith("/go.mod"):
            continue
        module, version, checksum = fields
        key = (module, version)
        if key in seen:
            continue
        seen.add(key)
        # Go h1 checksums are recorded verbatim as evidence; they are not a raw
        # file digest and are therefore intentionally not mapped to BOM hashes.
        result.append(
            _component(
                "golang", module, version, path, "checksum-identified-module",
                purl=f"pkg:golang/{module}@{version}",
                properties={"dilock:go-module-h1": checksum},
            )
        )
    return result


def parse_python_project(path: Path) -> list[dict[str, Any]]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    declarations = list(data.get("build-system", {}).get("requires", []))
    project = data.get("project", {})
    declarations.extend(project.get("dependencies", []))
    for values in project.get("optional-dependencies", {}).values():
        declarations.extend(values)
    result = []
    for declaration in declarations:
        match = re.fullmatch(r"\s*([A-Za-z0-9_.-]+)\s*==\s*([A-Za-z0-9_.+-]+)\s*", declaration)
        if not match:
            continue
        name, version = match.groups()
        result.append(
            _component(
                "pypi", name, version, path, "direct-exact-pin-no-transitive-lock",
                purl=f"pkg:pypi/{name.lower().replace('_', '-') }@{version}",
                properties={"dilock:declared-requirement": declaration},
            )
        )
    return result


def parse_java_pom(path: Path) -> list[dict[str, Any]]:
    root = ET.parse(path).getroot()
    ns = {"m": root.tag.split("}")[0].lstrip("{")}
    properties: dict[str, str] = {}
    managed: dict[tuple[str, str], tuple[str, str]] = {}

    # Resolve the local parent chain so module POMs inherit the reactor's exact
    # dependency versions without requiring Maven or network access.
    ancestors: list[ET.Element] = []
    current_root = root
    current_path = path.resolve()
    seen: set[Path] = {current_path}
    while True:
        parent = current_root.find("m:parent", ns)
        if parent is None:
            break
        relative = parent.findtext("m:relativePath", default="../pom.xml", namespaces=ns)
        if not relative.strip():
            break
        parent_path = (current_path.parent / relative.strip()).resolve()
        if parent_path in seen or not parent_path.is_file():
            break
        seen.add(parent_path)
        current_path = parent_path
        current_root = ET.parse(current_path).getroot()
        ancestors.append(current_root)

    # Parent properties and dependencyManagement values are inherited, then
    # the current module overrides them where it declares a value of its own.
    for pom_root in [*reversed(ancestors), root]:
        properties.update(
            {
                node.tag.split("}")[-1]: (node.text or "").strip()
                for node in pom_root.findall("m:properties/*", ns)
            }
        )
        for dependency in pom_root.findall(
            "m:dependencyManagement/m:dependencies/m:dependency", ns
        ):
            group = dependency.findtext("m:groupId", default="", namespaces=ns).strip()
            artifact = dependency.findtext("m:artifactId", default="", namespaces=ns).strip()
            version = dependency.findtext("m:version", default="", namespaces=ns).strip()
            scope = dependency.findtext("m:scope", default="compile", namespaces=ns).strip()
            if group and artifact and version:
                managed[(group, artifact)] = (version, scope)

    project_group = root.findtext("m:groupId", default="", namespaces=ns).strip()
    if not project_group:
        project_group = next(
            (
                pom_root.findtext("m:groupId", default="", namespaces=ns).strip()
                for pom_root in ancestors
                if pom_root.findtext("m:groupId", default="", namespaces=ns).strip()
            ),
            "",
        )
    project_version = root.findtext("m:version", default="", namespaces=ns).strip()
    if not project_version:
        project_version = root.findtext("m:parent/m:version", default="", namespaces=ns).strip()
    if project_group:
        properties.setdefault("project.groupId", project_group)
    if project_version:
        properties.setdefault("project.version", project_version)

    def resolve(value: str) -> str:
        for _ in range(10):
            match = re.fullmatch(r"\$\{([^}]+)\}", value)
            if not match:
                break
            replacement = properties.get(match.group(1))
            if replacement is None or replacement == value:
                break
            value = replacement
        return value

    result = []
    for dep in root.findall("m:dependencies/m:dependency", ns):
        group = (dep.findtext("m:groupId", default="", namespaces=ns)).strip()
        artifact = (dep.findtext("m:artifactId", default="", namespaces=ns)).strip()
        # Do not report Dilock's own reactor modules as third-party libraries.
        if project_group and group == project_group:
            continue
        version = (dep.findtext("m:version", default="", namespaces=ns)).strip()
        managed_version, managed_scope = managed.get((group, artifact), ("", ""))
        if not version:
            version = managed_version
        version = resolve(version)
        if not group or not artifact or not version:
            continue
        scope = (dep.findtext("m:scope", default=managed_scope or "compile", namespaces=ns)).strip()
        result.append(
            _component(
                "maven", f"{group}:{artifact}", version, path,
                "direct-pom-version-transitive-resolution-absent",
                purl=f"pkg:maven/{group}/{artifact}@{version}",
                properties={"dilock:dependency-scope": scope},
            )
        )
    return result


def inventory(root: Path = ROOT) -> tuple[dict[str, Any], dict[str, Any]]:
    global ROOT
    original_root = ROOT
    ROOT = root.resolve()
    try:
        inputs = {
            "dotnet": [ROOT / "sdk/dotnet/Dilock.Redis/packages.lock.json", ROOT / "sdk/dotnet/Dilock.Etcd/packages.lock.json"],
            "typescript": [ROOT / "sdk/typescript/package-lock.json"],
            "rust": [ROOT / "sdk/rust/Cargo.lock"],
            "go": [ROOT / "sdk/go/go.sum"],
            "python": [ROOT / "sdk/python/pyproject.toml"],
            "java": [
                ROOT / "sdk/java/core/pom.xml",
                ROOT / "sdk/java/redis/pom.xml",
                ROOT / "sdk/java/etcd/pom.xml",
            ],
            "cpp": [ROOT / "sdk/cpp/conanfile.py"],
        }
        parsers = {
            "dotnet": parse_nuget_lock,
            "typescript": parse_npm_lock,
            "rust": parse_cargo_lock,
            "go": parse_go_sum,
            "python": parse_python_project,
            "java": parse_java_pom,
        }
        components: list[dict[str, Any]] = []
        coverage = []
        for language, sources in inputs.items():
            language_components: list[dict[str, Any]] = []
            source_entries = []
            for source in sources:
                if source.is_file():
                    source_entries.append({"path": source.relative_to(ROOT).as_posix(), "sha256": sha256(source)})
                    if language in parsers:
                        language_components.extend(parsers[language](source))
            components.extend(language_components)
            notes = {
                "dotnet": "NuGet lockfiles resolve direct and transitive package versions and include NuGet content hashes.",
                "typescript": "npm lockfile resolves direct, development, and transitive package versions; integrity is included when present.",
                "rust": "Cargo.lock resolves registry packages with crates.io checksums; workspace path packages are excluded.",
                "go": "go.sum identifies module versions with Go h1 checksums; the Dilock module owner-qualified path remains unresolved.",
                "python": "Only exact direct build/project optional pins are included; no transitive Python lockfile is checked in.",
                "java": "Direct Maven POM dependencies only; no Maven dependency lock or resolved transitive graph is checked in.",
                "cpp": "Dependency graph is unknown: Conan recipe declarations exist, but no resolved Conan lock/graph metadata is checked in.",
            }[language]
            coverage.append({
                "sdk": language,
                "source_files": source_entries,
                "component_count": len(language_components),
                "status": "partial" if language_components else "unknown",
                "notes": notes,
            })

        bom = {
            "bomFormat": "CycloneDX",
            "specVersion": "1.5",
            "version": 1,
            "metadata": {
                "component": {"type": "application", "name": "dilock-seven-sdk-dependency-inventory", "version": "0.1.0-dev"},
                "properties": [{"name": "dilock:scope", "value": "partial local inventory; not a release SBOM"}],
            },
            "components": sorted(components, key=lambda c: (c["properties"][0]["value"], c["name"].lower(), c["version"])),
        }
        report = {
            "schema_version": 1,
            "generated_by": "tools/dependency_inventory.py",
            "network_accessed": False,
            "complete": False,
            "component_count": len(components),
            "coverage": coverage,
            "limitations": [
                "No registries or remote metadata were queried.",
                "No licenses, source hashes, or package coordinates are guessed.",
                "C++ resolved dependency inventory is unknown without a Conan lock/graph.",
                "Python and Java lack checked-in transitive lock data.",
                "This partial inventory does not establish release or production readiness.",
            ],
        }
        return bom, report
    finally:
        ROOT = original_root


def write_outputs(root: Path = ROOT) -> tuple[Path, Path]:
    bom, report = inventory(root)
    out = root / "validation" / "implementation"
    out.mkdir(parents=True, exist_ok=True)
    bom_path = out / "m6-dependency-bom-cyclonedx-1.5.json"
    report_path = out / "m6-dependency-coverage-20261004.json"
    bom_path.write_text(json.dumps(bom, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return bom_path, report_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if generated outputs differ from checked-in evidence")
    args = parser.parse_args()
    bom, report = inventory()
    out = ROOT / "validation" / "implementation"
    expected = {
        out / "m6-dependency-bom-cyclonedx-1.5.json": json.dumps(bom, indent=2, sort_keys=True) + "\n",
        out / "m6-dependency-coverage-20261004.json": json.dumps(report, indent=2, sort_keys=True) + "\n",
    }
    if args.check:
        stale = [str(path.relative_to(ROOT)) for path, content in expected.items() if not path.is_file() or path.read_text(encoding="utf-8") != content]
        if stale:
            print("STALE: " + ", ".join(stale))
            return 1
        print(f"PASS: {report['component_count']} components; coverage is explicitly partial")
        return 0
    for path, content in expected.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"WROTE: {path.relative_to(ROOT)}")
    print(f"COMPONENTS: {report['component_count']}; all seven SDK coverage cells recorded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
