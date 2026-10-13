"""Validate blueprint structure/data; does not test a distributed lock runtime."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
GENERATED_DIRS = {"artifacts", ".local", ".local-review", "ai", ".git", ".tools", ".tmp", "node_modules", "vendor", ".build", ".swiftpm", "target", "build", "_build", "deps", "dist", "coverage"}
sys.path.insert(0, str(ROOT))
from tools.reference_identity import redis_key, resource_id  # noqa: E402


def is_generated(path: Path) -> bool:
    """Keep local-only workspace notes, toolchains, vendored dependencies, and build output out of source checks."""
    return any(
        part.lower() in GENERATED_DIRS or part.lower().startswith(".tmp-")
        for part in path.relative_to(ROOT).parts
    )


def iter_source_files(root: Path, suffix: str):
    """Walk source files without descending into generated and cache trees."""
    for current, directories, files in os.walk(root, topdown=True, followlinks=False):
        directories[:] = sorted(
            name for name in directories
            if name.lower() not in GENERATED_DIRS and not name.lower().startswith(".tmp-")
        )
        for name in sorted(files):
            if name.endswith(suffix):
                yield Path(current) / name


def is_packaged_validation_snapshot(path: Path) -> bool:
    """Package README snapshots resolve repository-relative links from their original SDK path."""
    parts = path.relative_to(ROOT).parts
    return path.name.lower() == "readme.md" and any(
        parts[index].lower() == "validation"
        and any(part.lower().startswith("package") for part in parts[index + 1:])
        for index in range(len(parts) - 1)
    )


def is_validation_workspace_snapshot(path: Path) -> bool:
    """Copied source/package trees in validation artifacts are not repository documentation."""
    parts = tuple(part.lower() for part in path.relative_to(ROOT).parts)
    return any(
        parts[index] == "validation"
        and any(part in {"workspace", "packages"} for part in parts[index + 1:])
        for index in range(len(parts) - 1)
    )


def check_task_graph(tasks: list[dict]) -> None:
    """Reject duplicate IDs, missing dependencies and cycles."""
    mapping = {task["id"]: task for task in tasks}
    if len(mapping) != len(tasks):
        raise ValueError("duplicate task ID")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            raise ValueError("task dependency cycle at " + key)
        if key in visited:
            return
        if key not in mapping:
            raise ValueError("unknown dependency " + key)
        visiting.add(key)
        for dep in mapping[key]["depends_on"]:
            visit(dep)
        visiting.remove(key)
        visited.add(key)

    for key in mapping:
        visit(key)


def main() -> int:
    errors: list[str] = []
    count = 0
    task_graph_checked = False
    for path in iter_source_files(ROOT, ".json"):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            count += 1
        except (OSError, ValueError) as exc:
            errors.append(f"{path.relative_to(ROOT)}: {exc}")
    required = ["README.md", "docs/humans/README.md", "LICENSE",
                "spec/SEMANTICS.md", "spec/IDENTITY.md", "spec/NAMESPACE-METADATA.md",
                "docs/architecture/OBJECT-DESIGN.md", "conformance/cases.json"]
    for name in required:
        if not (ROOT / name).is_file():
            errors.append("missing required file: " + name)
    try:
        vectors = json.loads((ROOT / "spec/vectors/identity.json").read_text(encoding="utf-8"))
        for vector in vectors["valid"]:
            if resource_id(vector["namespace"], vector["resource"]) != vector["sha256"]:
                errors.append("identity mismatch: " + vector["id"])
            if redis_key(vector["namespace"], vector["resource"]) != vector["redis_key"]:
                errors.append("Redis key mismatch: " + vector["id"])
            if vector["etcd_lock_name"] != "/dilock/locks/" + resource_id(vector["namespace"], vector["resource"]):
                errors.append("etcd name mismatch: " + vector["id"])
        for vector in vectors["invalid"]:
            try:
                resource_id(vector["namespace"], vector["resource"])
                errors.append("accepted invalid vector: " + vector["id"])
            except ValueError:
                pass
        task_path = ROOT / "planning/tasks.json"
        if task_path.is_file():
            check_task_graph(json.loads(task_path.read_text(encoding="utf-8"))["tasks"])
            task_graph_checked = True
        cases = json.loads((ROOT / "conformance/cases.json").read_text(encoding="utf-8"))["cases"]
        ids = [case["id"] for case in cases]
        if len(ids) != len(set(ids)):
            errors.append("duplicate conformance case ID")
        if len(cases) < 30:
            errors.append("conformance catalog unexpectedly truncated")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append("data validation failed: " + str(exc))
    # Check ordinary inline Markdown relative links, not code-path prose or anchors.
    for path in iter_source_files(ROOT, ".md"):
        if is_packaged_validation_snapshot(path) or is_validation_workspace_snapshot(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"cannot read {path.relative_to(ROOT)}: {exc}")
            continue
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
            if target.startswith(("http:", "https:", "mailto:", "#")):
                continue
            rel = unquote(target.split("#", 1)[0])
            if rel and not (path.parent / rel).exists():
                errors.append(f"broken relative link in {path.relative_to(ROOT)}: {target}")
    if errors:
        for error in errors:
            print("FAIL: " + error)
        return 1
    task_graph = "task graph and " if task_graph_checked else ""
    print(f"PASS: blueprint structure, {count} JSON files, identity vectors, {task_graph}relative links.")
    print("No runtime SDK, backend integration, public registry or GitHub workflow was tested by this command.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
