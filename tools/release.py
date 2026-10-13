"""Prepare frozen catalog source deliveries and run local Docker package recipes.

This command has no publisher. A source archive, a native build and an installed
consumer are separate observations; none is automatically release approval.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fnmatch
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import stat
import subprocess
import tarfile
import uuid
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CATALOG = "tools/release-catalog.json"
EXCLUDED = {".git", ".tools", ".tmp", ".aws", ".codex", ".agents", "validation",
            ".local-review", "planning",
            "vendor", "node_modules", "target", "build", "dist", "artifacts",
            "obj", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
            ".build", ".swiftpm", "_build", "deps", ".gradle", ".dart_tool", ".venv",
            "dist-newstyle", ".zig-cache", ".kotlin", ".local", ".gocache", ".cpcache", ".release"}
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?")
IMAGE = re.compile(r"(?:[^\s]+@)?sha256:[0-9a-f]{64}")
STATES = {"PASS", "FAILED", "BLOCKED", "NOT_RUN", "SKIPPED"}


class ReleaseError(ValueError):
    """Invalid candidate, recipe or evidence; never ordinary build success."""


def read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256")
    return digest.hexdigest()


def is_reparse(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path.lstat(), "st_file_attributes", 0)
                                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def frozen_files(root: Path) -> set[str]:
    paths = set()
    for current, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(current) / name
            if is_reparse(path):
                raise ReleaseError(f"reparse/symlink in frozen source: {path.relative_to(root)}")
        paths.update((Path(current) / name).relative_to(root).as_posix() for name in files)
    return paths


def inside(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts or "\\" in relative or ":" in relative:
        raise ReleaseError(f"unsafe relative path: {relative}")
    path = root.joinpath(*pure.parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise ReleaseError(f"path escapes source root: {relative}")
    return path


def source_files(root: Path) -> list[str]:
    """Prune before descent, including reparse/symlink trees and local evidence."""
    paths = []
    for current, directories, names in os.walk(root, followlinks=False):
        directories[:] = sorted(name for name in directories
                                if name.casefold() not in EXCLUDED
                                and not name.casefold().startswith(".tmp")
                                and not (Path(current) / name).is_symlink()
                                and not getattr(Path(current) / name, "is_junction", lambda: False)())
        for name in sorted(names):
            path = Path(current) / name
            local_environment = name.casefold() == ".env" or (
                name.casefold().startswith(".env.") and name.casefold() not in {".env.example", ".env.sample"})
            if (path.is_symlink() or local_environment
                    or name.casefold() in {"dump.rdb", ".npmrc", ".pypirc", ".netrc", "nuget.config", "secrets.json"}
                    or name.endswith(".pyc")):
                continue
            relative = path.relative_to(root).as_posix()
            inside(root, relative)
            paths.append(relative)
    return paths


def matches(path: str, pattern: str) -> bool:
    # An explicit trailing /** includes the complete selected subtree.
    if pattern.endswith(("/**", "/**/*")):
        prefix = pattern[:(-5 if pattern.endswith("/**/*") else -3)].rstrip("/")
        return path == prefix or path.startswith(prefix + "/")
    return fnmatch.fnmatchcase(path, pattern)


def catalog(root: Path = ROOT) -> dict[str, dict[str, Any]]:
    data = read(root / CATALOG)
    if data.get("schema_version") != 1:
        raise ReleaseError("unsupported catalog schema")
    entries = data.get("entries", [])
    mapping = {entry["id"]: entry for entry in entries}
    if len(mapping) != len(entries):
        raise ReleaseError("release catalog must contain each SDK exactly once")
    for entry in entries:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", entry["id"]):
            raise ReleaseError("unsafe catalog identifier")
        for dependency in entry.get("dependencies", []):
            if dependency not in mapping:
                raise ReleaseError(f"unknown dependency for {entry['id']}: {dependency}")
        if not entry.get("source_include"):
            raise ReleaseError(f"source allowlist missing for {entry['id']}")
        for pattern in entry["source_include"] + entry.get("source_exclude", []):
            inside(root, pattern)
        build = entry.get("build", {})
        if build.get("image") and not IMAGE.fullmatch(build["image"]):
            raise ReleaseError(f"Docker image must be immutable for {entry['id']}")
        if build.get("cwd"):
            inside(root, build["cwd"])
        for argv in build.get("argv", []):
            if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) for arg in argv):
                raise ReleaseError(f"invalid argv for {entry['id']}")
            # Publication remains outside this orchestrator, including common
            # ecosystem publishing commands accidentally placed in a recipe.
            local_dotnet_publish = argv[:2] == ["dotnet", "publish"]
            if any(arg in {"push", "upload", "deploy", "hex.publish", "publish-module"}
                   or (arg == "publish" and not local_dotnet_publish) for arg in argv):
                raise ReleaseError(f"publisher command forbidden for {entry['id']}")
        for pattern in build.get("artifact_globs", []):
            inside(root, pattern[5:] if pattern.startswith("/out/") else pattern)
        consumer = entry.get("consumer", {})
        if consumer.get("image") and not IMAGE.fullmatch(consumer["image"]):
            raise ReleaseError(f"consumer Docker image must be immutable for {entry['id']}")
        for argv in consumer.get("argv", []):
            if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) for arg in argv):
                raise ReleaseError(f"invalid consumer argv for {entry['id']}")
            if any(arg in {"publish", "push", "upload", "deploy", "hex.publish", "publish-module"}
                   for arg in argv):
                raise ReleaseError(f"consumer publisher command forbidden for {entry['id']}")
    ordered(mapping, list(mapping))
    return mapping


def ordered(entries: dict[str, dict], selected: list[str]) -> list[str]:
    active, done, result = set(), set(), []

    def visit(key):
        if key not in entries:
            raise ReleaseError(f"unknown language: {key}")
        if key in active:
            raise ReleaseError(f"release dependency cycle: {key}")
        if key in done:
            return
        active.add(key)
        for dep in entries[key].get("dependencies", []):
            visit(dep)
        active.remove(key)
        done.add(key)
        result.append(key)

    for key in selected:
        visit(key)
    return result


def selected_sources(entries: dict[str, dict], key: str, available: list[str]) -> list[str]:
    included = {path for path in available if path in {"LICENSE", "NOTICE", "README.md"}
                or path.startswith("spec/")}
    for dep in ordered(entries, [key]):
        entry = entries[dep]
        paths = [path for path in available
                 if any(matches(path, pattern) for pattern in entry["source_include"])
                 and not any(matches(path, pattern) for pattern in entry.get("source_exclude", []))]
        if not paths:
            raise ReleaseError(f"empty source delivery for {dep}")
        included.update(paths)
    return sorted(included)


def archive(root: Path, target: Path, paths: list[str], metadata: dict) -> None:
    """Stable source archive bytes for identical selected inputs and metadata."""
    with target.open("xb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as output:
            for relative in paths:
                payload = inside(root, relative).read_bytes()
                info = tarfile.TarInfo(relative)
                info.size = len(payload)
                info.mode = 0o755 if relative.endswith((".sh", "/gradlew")) else 0o644
                output.addfile(info, io.BytesIO(payload))
            payload = (json.dumps(metadata, indent=2) + "\n").encode()
            info = tarfile.TarInfo("DILOCK-SOURCE-DELIVERY.json")
            info.size, info.mode = len(payload), 0o644
            output.addfile(info, io.BytesIO(payload))
            text = (f"# Dilock {metadata['language']} source delivery\n\n"
                    f"Candidate archive version: `{metadata['version']}`. This is a frozen source delivery, "
                    "not an installed binary package or registry publication. Native package manifests "
                    "retain their own versions.\n\n"
                    f"Delivery kind: {metadata['delivery_kind']}. Required engine source: "
                    f"{', '.join(metadata['dependencies']) or 'none'}.\n\n"
                    "The repository-relative SDK layout and shared spec/LICENSE/NOTICE are retained. "
                    "Start with the selected SDK README and package manifest. Historical validation links "
                    "refer to the full repository; those evidence files and private build caches are excluded.\n\n"
                    "The catalog release runner builds these bytes in Docker, retains native artifacts, "
                    "then attempts isolated consumers. Consult the candidate manifest for PASS, FAILED, "
                    "BLOCKED and NOT_RUN per phase. Lease expiry does not stop a stale process.\n").encode()
            info = tarfile.TarInfo("DILOCK-SOURCE-DELIVERY.md")
            info.size, info.mode = len(text), 0o644
            output.addfile(info, io.BytesIO(text))


def prepare(root: Path, output: Path, version: str, selected: list[str]) -> dict:
    if not VERSION.fullmatch(version):
        raise ReleaseError("version must be a numeric SemVer with optional prerelease")
    if output.exists():
        raise ReleaseError("candidate directory already exists; existing candidates are preserved")
    entries = catalog(root)
    keys = ordered(entries, selected or list(entries))
    available = source_files(root)
    source_map = {key: selected_sources(entries, key, available) for key in keys}
    output.mkdir(parents=True)
    snapshot = output / "source"
    snapshot.mkdir()
    inputs = []
    for relative in sorted({path for paths in source_map.values() for path in paths}):
        destination = inside(snapshot, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(inside(root, relative), destination)
        inputs.append({"path": relative, "sha256": sha(destination)})
    write(output / "catalog.json", {"schema_version": 1, "entries": [entries[key] for key in keys]})
    manifest = {"schema_version": 1, "version": version,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "scope": keys, "catalog_sha256": sha(output / "catalog.json"),
                "source": inputs, "results": {}, "publication_enabled": False}
    (output / "source-packages").mkdir()
    for key in keys:
        entry = entries[key]
        filename = f"dilock-{key}-{version}-source.tar.gz"
        target = output / "source-packages" / filename
        metadata = {"language": key, "version": version, "kind": "source-preview",
                    "delivery_kind": entry["delivery_kind"],
                    "dependencies": entry.get("dependencies", []),
                    "compiled": False, "installed_consumer": "NOT_RUN",
                    "publication_route": entry.get("publication", {}).get("route"),
                    "version_note": "Archive label only; native package versions are not rewritten",
                    "source": [record for record in inputs if record["path"] in source_map[key]]}
        archive(snapshot, target, source_map[key], metadata)
        manifest["results"][key] = {
            "source_delivery": {"state": "PASS", "path": target.relative_to(output).as_posix(), "sha256": sha(target)},
            "native_build": {"state": "NOT_RUN", "output_role": entry.get("build", {}).get("output_role", "package")},
            "installed_consumer": {"state": "NOT_RUN"},
            "registry_identity": entry.get("publication", {}).get("identity_state", "unverified"),
            "beta_eligible": False}
    write(output / "manifest.json", manifest)
    return manifest


def integrity(output: Path) -> dict:
    manifest = read(output / "manifest.json")
    if sha(output / "catalog.json") != manifest["catalog_sha256"]:
        raise ReleaseError("frozen recipe catalog changed")
    expected = {record["path"] for record in manifest["source"]}
    if len(expected) != len(manifest["source"]) or frozen_files(output / "source") != expected:
        raise ReleaseError("frozen source file inventory changed (unrecorded or missing input)")
    for record in manifest["source"]:
        path = inside(output / "source", record["path"])
        if not path.is_file() or sha(path) != record["sha256"]:
            raise ReleaseError(f"frozen source changed: {record['path']}")
    for cell in manifest["results"].values():
        for record in [cell["source_delivery"], *cell.get("native_build", {}).get("artifacts", [])]:
            path = inside(output, record["path"])
            if not path.is_file() or sha(path) != record["sha256"]:
                raise ReleaseError(f"candidate artifact changed: {record['path']}")
        if cell["native_build"]["state"] not in STATES or cell["installed_consumer"]["state"] not in STATES:
            raise ReleaseError("invalid execution state")
        if cell.get("beta_eligible"):
            raise ReleaseError("beta eligibility requires a separate reviewed installed-consumer/version/identity gate")
    return manifest


def run(argv: list[str], timeout: int = 20) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, errors="replace", timeout=timeout, check=False)


def timeout_evidence(attempt: Path, exc: subprocess.TimeoutExpired, container_name: str | None) -> dict:
    """Retain partial command output and report cleanup independently of failure."""
    for label, output in (("stdout", exc.stdout), ("stderr", exc.stderr)):
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        (attempt / f"{label}.log").write_text(output or "", encoding="utf-8")
    cleanup = {"state": "NOT_RUN", "reason": "No task container was started"}
    if container_name:
        try:
            result = run(["docker", "rm", "-f", container_name])
            cleanup = {"state": "PASS" if result.returncode == 0 else "FAILED",
                       "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        except (OSError, subprocess.TimeoutExpired) as error:
            cleanup = {"state": "BLOCKED", "reason": str(error)}
    return cleanup


def caches(workspace: Path, key: str) -> list[tuple[Path, str]]:
    """Optional local dependency caches; never expose them as source artifacts.

    Mount readonly, then copy to a task-owned container directory. The candidate
    retains paths used; version/closure checks remain separate from a build pass.
    """
    profiles = {
        "dotnet": [([".tools/dotnet-example-packages", ".tools/nugetpkgs"], "/root/.nuget/packages")],
        "java": [([".tools/m2repo"], "/root/.m2/repository")],
        "typescript": [([".tools/npm-cache"], "/root/.npm")],
        "go": [([".tools/gomodcache"], "/go/pkg/mod")],
        "rust": [([".tools/cargo/registry"], "/usr/local/cargo/registry"),
                 ([".tools/grpc-root"], "/opt/grpc-root"),
                 ([".tools/rust-etcd-trixie-docker-20261009/trixie-runtime"], "/opt/trixie-runtime"),
                 ([".tools/release-rust-protoc"], "/opt/dilock/rust-protoc")],
        "dart": [([".tools/dart-pub-cache"], "/root/.pub-cache")],
        "cpp": [([f".tools/{name}"], f"/opt/dilock/{name}")
                for name in ("cppinstall", "grpc-root", "cppdeps", "openssl", "openssl-runtime")],
        "python": [([".tools/release-python-build-tools"], "/opt/dilock/python-build"),
                   ([".tools/beta-fix-python-tools-20261012"], "/opt/dilock/python-runtime")],
        "elixir": [([".tools/release-elixir-tools"], "/opt/dilock/elixir-tools"),
                   ([".tools/elixir-hex-package-candidate-20261006/dependency-cache"], "/opt/dilock/elixir-deps")],
        "julia": [([".tmp/julia-scope-release-20261010/depot"], "/root/.julia")],
        "r": [([".tmp/r-current-source-20261010/r-lib"], "/opt/dilock/r-lib")],
    }
    aliases = {"fsharp": "dotnet", "vbnet": "dotnet", "powershell": "dotnet", "kotlin": "java",
               "scala": "java", "groovy": "java", "clojure": "java", "javascript": "typescript",
               "c": "cpp", "erlang": "elixir"}
    result = []
    for choices, destination in profiles.get(aliases.get(key, key), []):
        for relative in choices:
            source = inside(workspace, relative)
            if source.is_dir():
                result.append((source, destination))
                break
    return result


def cache_setup(selected_caches: list[tuple[Path, str]]) -> str:
    lines = []
    for index, (_, destination) in enumerate(selected_caches):
        lines += [f"mkdir -p {shlex.quote(destination)}", f"cp -R /cache/{index}/. {shlex.quote(destination)}/"]
        if destination == "/root/.nuget/packages":
            # Drop only Dilock copies in this fresh container; exact candidate
            # archives must not be shadowed by same-version cached binaries.
            lines += ['for package in /root/.nuget/packages/dilock*; do',
                      '  test ! -d "$package" || rm -rf "$package"', 'done']
        if destination == "/root/.m2/repository":
            lines += ['rm -rf /root/.m2/repository/org/dilock']
        if destination == "/opt/dilock/r-lib":
            lines += ['rm -rf /opt/dilock/r-lib/dilock']
        if destination == "/opt/dilock/rust-protoc":
            lines += ['chmod +x /opt/dilock/rust-protoc/protoc-wrapper']
    return "\n".join(lines) + "\n"


def cache_mounts(selected_caches: list[tuple[Path, str]]) -> list[str]:
    result = []
    for index, (path, _) in enumerate(selected_caches):
        result += ["--mount", f"type=bind,src={path.resolve()},dst=/cache/{index},readonly"]
    return result


def missing_artifacts(artifacts: list[dict], patterns: list[str]) -> list[str]:
    names = [PurePosixPath(record["path"]).name for record in artifacts]
    return [pattern for pattern in patterns if not any(fnmatch.fnmatchcase(name, pattern) for name in names)]


def builder_script(entry: dict) -> str:
    build = entry["build"]
    lines = ["#!/bin/sh", "set -eu", "mkdir -p /work /out/native", "cp -R /src/. /work/",
             "cd " + shlex.quote("/work/" + build["cwd"])]
    lines.extend(shlex.join(argv) for argv in build["argv"])
    # Glob expansion is intentional here; identifiers are validated, quoted
    # path components cannot introduce executable shell syntax.
    lines += ["found=0"]
    for pattern in build.get("artifact_globs", []):
        expression = pattern if pattern.startswith("/out/") else "/work/" + pattern
        expression = "*".join(shlex.quote(part) for part in expression.split("*"))
        lines += [f"for artifact in {expression}; do", '  test -e "$artifact" || continue',
                  '  basename=${artifact##*/}',
                  '  if test -d "$artifact"; then',
                  f'    tar -czf "/out/native/{entry["id"]}-$basename-install.tar.gz" -C "$artifact" .',
                  '    found=1', '    continue', '  fi',
                  '  test ! -e "/out/native/$basename" || { echo "duplicate artifact basename" >&2; exit 1; }',
                  '  cp "$artifact" "/out/native/$basename"', "  found=1", "done"]
    if build.get("output_role", "package") == "package":
        lines += ['test "$found" = 1 || { echo "NO_PACKAGE_ARTIFACTS" >&2; exit 3; }']
    return "\n".join(lines) + "\n"


def stage_packages(output: Path, manifest: dict, keys: list[str]) -> Path:
    packages = output / "consumer-packages"
    packages.mkdir(exist_ok=True)
    for key in keys:
        destination = packages / key
        destination.mkdir(exist_ok=True)
        cell = manifest["results"][key]
        records = [cell["source_delivery"], *cell["native_build"].get("artifacts", [])]
        expected = {inside(output, record["path"]).name for record in records}
        if frozen_files(destination) - expected:
            raise ReleaseError(f"unrecorded consumer package inputs for {key}")
        for record in records:
            path = inside(output, record["path"])
            target = destination / path.name
            if target.exists() and sha(target) != record["sha256"]:
                raise ReleaseError(f"consumer package changed: {target.name}")
            if not target.exists():
                shutil.copyfile(path, target)
    return packages


def build(output: Path, selected: list[str], timeout: int, workspace: Path = ROOT) -> dict:
    manifest = integrity(output)
    entries = {entry["id"]: entry for entry in read(output / "catalog.json")["entries"]}
    keys = ordered(entries, selected or manifest["scope"])
    docker_error = None
    try:
        probe = run(["docker", "version", "--format", "{{.Server.Version}}"])
        if probe.returncode:
            docker_error = probe.stderr[-1600:]
    except (OSError, subprocess.TimeoutExpired) as exc:
        docker_error = str(exc)
    for key in keys:
        container_name = None
        cell = manifest["results"][key]
        if cell["native_build"]["state"] != "NOT_RUN":
            raise ReleaseError(f"native attempt already recorded for {key}; prepare a new candidate to retry")
        entry = entries[key]
        recipe = entry.get("build", {})
        print(f"{key}: native_build=RUNNING", flush=True)
        dependency_failures = [dep for dep in entry.get("dependencies", [])
                               if manifest["results"][dep]["native_build"]["state"] != "PASS"]
        if docker_error:
            observation = {"state": "BLOCKED", "reason": "Docker unavailable", "detail": docker_error}
        elif dependency_failures:
            observation = {"state": "BLOCKED", "reason": "Candidate engine dependency did not pass its native build",
                           "dependencies": dependency_failures}
        elif not recipe.get("image") or not recipe.get("argv"):
            observation = {"state": "BLOCKED", "reason": "No immutable Docker/native package recipe configured",
                           "delivery_kind": entry["delivery_kind"]}
        else:
            try:
                probe = run(["docker", "image", "inspect", recipe["image"], "--format", "{{.Id}}"])
                if probe.returncode:
                    observation = {"state": "BLOCKED", "reason": "Pinned Docker image not cached; no pull attempted",
                                   "image": recipe["image"]}
                else:
                    attempt = output / "builds" / key
                    attempt.mkdir(parents=True, exist_ok=False)
                    script = attempt / "build.sh"
                    selected_caches = caches(workspace, key)
                    script_text = builder_script(entry).replace("set -eu\n", "set -eu\n" + cache_setup(selected_caches), 1)
                    script.write_text(script_text, encoding="utf-8", newline="\n")
                    packages = stage_packages(output, manifest, ordered(entries, [key]))
                    container_name = "dilock-release-build-" + uuid.uuid4().hex
                    argv = ["docker", "run", "--rm", "--name", container_name, "--pull=never", "--network=none",
                            "--mount", f"type=bind,src={(output / 'source').resolve()},dst=/src,readonly",
                            "--mount", f"type=bind,src={attempt.resolve()},dst=/out",
                            "--mount", f"type=bind,src={packages.resolve()},dst=/packages,readonly",
                            "--env", "DILOCK_OUTPUT=/out", "--env", "DILOCK_SOURCE=/src",
                            *cache_mounts(selected_caches),
                            "--entrypoint", "/bin/sh", recipe["image"], "/out/build.sh"]
                    result = run(argv, timeout)
                    (attempt / "stdout.log").write_text(result.stdout, encoding="utf-8")
                    (attempt / "stderr.log").write_text(result.stderr, encoding="utf-8")
                    artifacts = [{"path": path.relative_to(output).as_posix(), "sha256": sha(path)}
                                 for path in sorted((attempt / "native").glob("*")) if path.is_file()]
                    output_role = recipe.get("output_role", "package")
                    state = "PASS" if result.returncode == 0 and (artifacts or output_role == "build-validation") else "FAILED"
                    missing = missing_artifacts(artifacts, recipe.get("required_artifact_patterns", []))
                    if missing:
                        state = "FAILED"
                    if result.returncode == 127:
                        state = "BLOCKED"
                    diagnostic = (result.stderr or result.stdout).strip()
                    detail = diagnostic[-600:] if diagnostic else None
                    observation = {"state": state, "exit_code": result.returncode, "argv": argv,
                                   "image_id": probe.stdout.strip(), "artifacts": artifacts,
                                   "output_role": output_role,
                                   "native_package_built": state == "PASS" and output_role == "package",
                                   "missing_required_artifacts": missing,
                                   "dependency_caches": [{"path": str(path), "container_copy": destination}
                                                         for path, destination in selected_caches],
                                   "dependency_cache_provenance_verified": False,
                                   "reason": ("Exact native artifacts retained" if output_role == "package" else "Frozen-source build validation passed; no portable native package claim") if state == "PASS" else
                                   ("Required tool is absent from the cached builder image" if state == "BLOCKED" else
                                    "Native recipe failed; inspect retained logs"),
                                   "diagnostic": detail,
                                   "stdout": (attempt / "stdout.log").relative_to(output).as_posix(),
                                   "stderr": (attempt / "stderr.log").relative_to(output).as_posix(),
                                   "native_version_verified": False}
            except subprocess.TimeoutExpired as exc:
                cleanup = timeout_evidence(attempt, exc, container_name) if container_name else {"state": "NOT_RUN"}
                observation = {"state": "FAILED", "reason": "Native command exceeded its bounded timeout",
                               "timeout_seconds": timeout, "command": exc.cmd,
                               "task_container_cleanup": cleanup}
            except OSError as exc:
                observation = {"state": "BLOCKED", "reason": str(exc)}
        cell["native_build"] = observation
        write(output / "manifest.json", manifest)
        print(f"{key}: native_build={observation['state']}; installed_consumer=NOT_RUN", flush=True)
    return manifest


def summary(manifest: dict, delivery_catalog: dict | None = None) -> dict:
    result = {"languages": len(manifest["results"]), "source_deliveries": {}, "native_builds": {},
              "native_package_builds": {}, "source_build_validations": {},
              "other_delivery_builds": {}, "output_role_builds": {},
              "installed_consumers": {}, "beta_eligible": 0, "publication_enabled": False}
    result["consumer_profiles"] = {}
    roles = {entry["id"]: entry.get("build", {}).get("output_role", "package")
             for entry in (delivery_catalog or {}).get("entries", [])}
    for key, cell in manifest["results"].items():
        for key, target in [("source_delivery", "source_deliveries"), ("native_build", "native_builds"),
                            ("installed_consumer", "installed_consumers")]:
            state = cell[key]["state"]
            result[target][state] = result[target].get(state, 0) + 1
        role = cell["native_build"].get("output_role", roles.get(key, "package"))
        target = ("source_build_validations" if role == "build-validation" else
                  "native_package_builds" if role == "package" else "other_delivery_builds")
        state = cell["native_build"]["state"]
        result[target][state] = result[target].get(state, 0) + 1
        role_states = result["output_role_builds"].setdefault(role, {})
        role_states[state] = role_states.get(state, 0) + 1
        profile = cell["installed_consumer"].get("kind", "pending")
        result["consumer_profiles"][profile] = result["consumer_profiles"].get(profile, 0) + 1
    return result


def consume(output: Path, selected: list[str], timeout: int, workspace: Path = ROOT) -> dict:
    """Run exact-package consumers without exposing the checkout or snapshot."""
    manifest = integrity(output)
    entries = {entry["id"]: entry for entry in read(output / "catalog.json")["entries"]}
    keys = ordered(entries, selected or manifest["scope"])
    # Only recorded artifacts enter this mount; no source tree, caches or logs.
    packages = stage_packages(output, manifest, keys)
    for key in keys:
        cell = manifest["results"][key]
        if cell["installed_consumer"]["state"] != "NOT_RUN":
            raise ReleaseError(f"consumer attempt already recorded for {key}; prepare a new candidate to retry")
        entry = entries[key]
        recipe = entry.get("consumer", {})
        image = recipe.get("image") or entry["build"].get("image")
        if cell["native_build"]["state"] != "PASS":
            observation = {"state": "BLOCKED", "reason": "Native build/validation did not pass for this exact candidate"}
        elif not recipe.get("argv") or not image:
            observation = {"state": "BLOCKED", "reason": "Exact installed-consumer recipe not configured",
                           "existing_entrypoints": recipe.get("entrypoints", [])}
        else:
            attempt = output / "consumers" / key
            attempt.mkdir(parents=True, exist_ok=False)
            print(f"{key}: installed_consumer=RUNNING", flush=True)
            script = attempt / "consume.sh"
            selected_caches = caches(workspace, key)
            script.write_text("#!/bin/sh\nset -eu\n" + cache_setup(selected_caches) + "mkdir -p /consumer\ncd /consumer\n"
                              + "\n".join(shlex.join(argv) for argv in recipe["argv"]) + "\n",
                              encoding="utf-8", newline="\n")
            argv = ["docker", "run", "--rm", "--pull=never", "--network=none",
                    "--name", (container_name := "dilock-release-consumer-" + uuid.uuid4().hex),
                    "--mount", f"type=bind,src={packages.resolve()},dst=/packages,readonly",
                    "--mount", f"type=bind,src={attempt.resolve()},dst=/consumer",
                    *cache_mounts(selected_caches),
                    "--env", "DILOCK_PACKAGES=/packages", "--entrypoint", "/bin/sh", image,
                    "/consumer/consume.sh"]
            try:
                result = run(argv, timeout)
                (attempt / "stdout.log").write_text(result.stdout, encoding="utf-8")
                (attempt / "stderr.log").write_text(result.stderr, encoding="utf-8")
                observation = {"state": "PASS" if result.returncode == 0 else ("BLOCKED" if result.returncode == 127 else "FAILED"),
                               "exit_code": result.returncode, "argv": argv,
                               "source_checkout_visible": False, "live_backend_tests": "NOT_RUN",
                               "dependency_caches": [{"path": str(path), "container_copy": destination}
                                                     for path, destination in selected_caches],
                               "dependency_cache_provenance_verified": False,
                               "reason": "Exact-package installed consumer finished" if result.returncode == 0 else "Consumer command failed; inspect retained logs",
                               "stdout": (attempt / "stdout.log").relative_to(output).as_posix(),
                               "stderr": (attempt / "stderr.log").relative_to(output).as_posix()}
            except subprocess.TimeoutExpired as exc:
                cleanup = timeout_evidence(attempt, exc, container_name)
                observation = {"state": "FAILED", "reason": "Consumer exceeded bounded timeout", "timeout_seconds": timeout,
                               "task_container_cleanup": cleanup}
            except OSError as exc:
                observation = {"state": "BLOCKED", "reason": str(exc)}
        cell["installed_consumer"] = observation
        cell["installed_consumer"]["kind"] = recipe.get("kind", "unclassified")
        cell["installed_consumer"]["delivery_profile"] = recipe.get("delivery_profile")
        cell["installed_consumer"]["native_package_consumed"] = (
            observation["state"] == "PASS" and recipe.get("kind") == "installed-native-package")
        if observation["state"] == "PASS":
            cell["installed_consumer"]["reason"] = "Consumer completed for the recorded delivery profile"
        write(output / "manifest.json", manifest)
        print(f"{key}: installed_consumer={observation['state']}", flush=True)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    subs = parser.add_subparsers(dest="operation", required=True)
    subs.add_parser("catalog", help="validate complete catalog coverage")
    prepare_parser = subs.add_parser("prepare", help="freeze source deliveries; never publish")
    prepare_parser.add_argument("--version", required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--languages", nargs="+")
    build_parser = subs.add_parser("build", help="execute immutable locally cached Docker recipes")
    build_parser.add_argument("--candidate", type=Path, required=True)
    build_parser.add_argument("--languages", nargs="+")
    build_parser.add_argument("--timeout", type=int, default=600)
    consumer_parser = subs.add_parser("consume", help="run source-hidden exact-package install/import recipes")
    consumer_parser.add_argument("--candidate", type=Path, required=True)
    consumer_parser.add_argument("--languages", nargs="+")
    consumer_parser.add_argument("--timeout", type=int, default=600)
    verify_parser = subs.add_parser("verify", help="verify frozen hashes and report delivery states")
    verify_parser.add_argument("--candidate", type=Path, required=True)
    verify_parser.add_argument("--require-native", action="store_true")
    verify_parser.add_argument("--require-consumers", action="store_true")
    args = parser.parse_args()
    try:
        if args.operation == "catalog":
            entries = catalog(args.repo.resolve())
            print(json.dumps({"languages": len(entries), "order": ordered(entries, list(entries))}, indent=2))
            return 0
        if args.operation == "prepare":
            output = args.output.resolve()
            if not output.is_relative_to(args.repo.resolve()):
                raise ReleaseError("candidate must be inside the workspace")
            manifest = prepare(args.repo.resolve(), output, args.version, args.languages or [])
        elif args.operation in {"build", "consume"}:
            if not 1 <= args.timeout <= 3600:
                raise ReleaseError("timeout must be between 1 and 3600 seconds")
            operation = build if args.operation == "build" else consume
            manifest = operation(args.candidate.resolve(), args.languages or [], args.timeout, args.repo.resolve())
        else:
            manifest = integrity(args.candidate.resolve())
        frozen_catalog = read(args.candidate.resolve() / "catalog.json") if args.operation in {"build", "consume", "verify"} else None
        print(json.dumps(summary(manifest, frozen_catalog), indent=2))
        if args.operation in {"build", "consume"}:
            phase = "native_build" if args.operation == "build" else "installed_consumer"
            return int(any(cell[phase]["state"] != "PASS" for cell in manifest["results"].values()))
        if args.operation == "verify":
            return int(any((args.require_native and cell["native_build"]["state"] != "PASS")
                           or (args.require_consumers and cell["installed_consumer"]["state"] != "PASS")
                           for cell in manifest["results"].values()))
        return 0
    except (OSError, ValueError, KeyError) as exc:
        print(f"FAILED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
