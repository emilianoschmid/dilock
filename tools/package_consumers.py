"""Build and consume Dilock packages; the aggregate remains fail-closed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import uuid
import zipfile
from pathlib import Path
from typing import Any


SDK_NAMES = ("dotnet", "cpp", "python", "typescript", "go", "rust", "java")
SCOPE = "source-built package artifacts consumed outside the source checkout"
GO_BLOCK = (
    "BLOCKED: owner-qualified Go module identity has not been confirmed. "
    "The repository-local module path `dilock` is not a standalone external module identity."
)
NOT_IMPLEMENTED = "No checked-in exact-artifact consumer driver exists for this SDK yet."
DRIVER_PATHS = {
    "dotnet": "sdk/dotnet/Consumers/package_consumer.py",
    "cpp": "sdk/cpp/consumers/package_consumer.py",
    "typescript": "sdk/typescript/consumers/package_consumer.py",
    "rust": "sdk/rust/consumers/package_consumer.py",
    "java": "sdk/java/consumers/package_consumer.py",
}


class DriverError(RuntimeError):
    """A package build, archive check, install, or consumer command failed."""

    def __init__(self, message: str, *, commands: list[dict[str, Any]] | None = None,
                 artifacts: list[dict[str, str]] | None = None, blocked: bool = False) -> None:
        super().__init__(message)
        self.commands = commands or []
        self.artifacts = artifacts or []
        self.blocked = blocked


def command_record(argv: list[str], exit_code: int) -> dict[str, Any]:
    return {"argv": [str(part) for part in argv], "exit_code": exit_code}


def environment_blocked(detail: str) -> bool:
    normalized = detail.casefold()
    return (
        ("permissionerror" in normalized and ("winerror 5" in normalized or "permissionerror(13" in normalized))
        or "[winerror 5]" in normalized
        or "no matching distribution found" in normalized
    )


def run(argv: list[str], *, cwd: Path, env: dict[str, str],
        records: list[dict[str, Any]]) -> dict[str, Any]:
    print("+", shlex.join([str(part) for part in argv]), flush=True)
    try:
        completed = subprocess.run(argv, cwd=cwd, env=env, check=False, capture_output=True, text=True)
        exit_code = completed.returncode
    except OSError as error:
        exit_code = 127
        records.append(command_record(argv, exit_code))
        raise DriverError(f"could not start command: {error}", commands=records) from error
    record = command_record(argv, exit_code)
    records.append(record)
    if completed.stdout:
        print(completed.stdout, end="", flush=True)
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr, flush=True)
    if completed.returncode:
        detail = completed.stderr or completed.stdout
        blocked = environment_blocked(detail)
        raise DriverError(
            f"command exited {completed.returncode}: {shlex.join(record['argv'])}; {detail[-1200:]}",
            commands=records,
            blocked=blocked,
        )
    return record


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def venv_python(path: Path) -> Path:
    return path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def package_build_python(repo: Path, override: Path | None = None) -> Path:
    """Prefer the repository's pinned build environment over the agent runtime."""
    configured = override or (Path(os.environ["DILOCK_PYTHON_BUILD_PYTHON"])
                              if os.environ.get("DILOCK_PYTHON_BUILD_PYTHON") else None)
    if configured is not None:
        return configured.expanduser().resolve()
    candidate = venv_python(repo / ".tools" / "venv")
    return candidate if candidate.is_file() else Path(sys.executable)


def package_build_site_packages(repo: Path, build_python: Path | None = None) -> Path:
    """Return Hatchling's pinned site-packages for an offline sdist build."""
    if build_python is None:
        environment = repo / ".tools" / "venv"
    else:
        executable_parent = build_python.resolve().parent
        environment = executable_parent.parent if executable_parent.name.casefold() in {"scripts", "bin"} else executable_parent
    if os.name == "nt":
        return environment / "Lib" / "site-packages"
    candidates = sorted((environment / "lib").glob("python*/site-packages"))
    return candidates[-1] if candidates else environment / "lib" / "site-packages"


def check_archive_contents(wheel: Path, sdist: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        wheel_names = set(archive.namelist())
        required_wheel = {
            "dilock/py.typed",
            "dilock/redis-release.lua",
            "dilock/redis-renew.lua",
            "dilock/_etcd_proto/v3lock.proto",
        }
        missing = sorted(required_wheel - wheel_names)
        if missing:
            raise DriverError(f"wheel omits required public package resources: {missing}")
    with tarfile.open(sdist, "r:gz") as archive:
        names = set(archive.getnames())
        required_sdist = {
            "pyproject.toml",
            "src/dilock/__init__.py",
            "src/dilock/redis.py",
            "src/dilock/etcd.py",
        }
        missing = sorted(name for name in required_sdist if not any(p.endswith("/" + name) for p in names))
        if missing:
            raise DriverError(f"sdist omits required build/source files: {missing}")


def safe_sdist_target(destination: Path, member_name: str) -> Path:
    relative = Path(member_name)
    if relative.is_absolute() or ".." in relative.parts:
        raise DriverError(f"sdist contains an unsafe path: {member_name}")
    root = destination.resolve()
    target = (destination / relative).resolve()
    if root not in target.parents and target != root:
        raise DriverError(f"sdist member escapes its extraction directory: {member_name}")
    return target


def extract_sdist(sdist: Path, destination: Path) -> Path:
    """Extract only regular files and directories while rejecting path escapes."""
    destination.mkdir(parents=True)
    with tarfile.open(sdist, "r:gz") as archive:
        members = archive.getmembers()
        if not members:
            raise DriverError("sdist archive is empty")
        roots = {Path(member.name).parts[0] for member in members if Path(member.name).parts}
        if len(roots) != 1:
            raise DriverError("sdist must contain one top-level source directory")
        for member in members:
            if member.issym() or member.islnk() or not (member.isdir() or member.isfile()):
                raise DriverError(f"sdist contains an unsupported member type: {member.name}")
            target = safe_sdist_target(destination, member.name)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise DriverError(f"sdist member cannot be read: {member.name}")
            with source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
    source_root = destination / next(iter(roots))
    if not (source_root / "pyproject.toml").is_file():
        raise DriverError("extracted sdist is missing its pyproject.toml")
    return source_root


def initial_report() -> dict[str, Any]:
    results: dict[str, Any] = {}
    for sdk in SDK_NAMES:
        if sdk == "go":
            results[sdk] = {"status": "blocked", "reason": GO_BLOCK, "artifacts": [], "commands": []}
        else:
            results[sdk] = {
                "status": "not_run",
                "reason": NOT_IMPLEMENTED if sdk != "python" else "Python consumer has not run.",
                "artifacts": [],
                "commands": [],
            }
    return {
        "schema_version": 1,
        "overall_status": "incomplete",
        "scope": SCOPE,
        "results": results,
    }


def update_overall(report: dict[str, Any]) -> None:
    statuses = [cell["status"] for cell in report["results"].values()]
    if all(status == "passed" for status in statuses):
        report["overall_status"] = "passed"
    elif "failed" in statuses:
        report["overall_status"] = "failed"
    else:
        report["overall_status"] = "incomplete"


def run_external_sdk_consumer(repo: Path, sdk: str, scratch_root: Path) -> dict[str, Any]:
    """Run an SDK-owned exact-artifact driver and validate its evidence record."""
    driver_rel = DRIVER_PATHS.get(sdk)
    if driver_rel is None:
        return {
            "status": "not_run",
            "reason": NOT_IMPLEMENTED,
            "artifacts": [],
            "commands": [],
        }
    driver = repo / driver_rel
    if not driver.is_file():
        return {
            "status": "not_run",
            "reason": f"Checked-in driver is missing: {driver_rel}",
            "artifacts": [],
            "commands": [],
        }
    scratch_root.mkdir(parents=True, exist_ok=True)
    if any(scratch_root.iterdir()):
        raise DriverError(f"consumer workspace must be empty; preserving existing contents at {scratch_root}")
    work = scratch_root / "work"
    work.mkdir()
    child_report_path = scratch_root / "driver-result.json"
    argv = [sys.executable, str(driver), "--output", str(child_report_path), "--scratch", str(work)]
    env = os.environ.copy()
    env.update({
        "PIP_NO_INDEX": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "NPM_CONFIG_OFFLINE": "true",
        "CARGO_NET_OFFLINE": "true",
        "GOPROXY": "off",
    })
    try:
        completed = subprocess.run(argv, cwd=repo, env=env, capture_output=True, text=True, check=False)
    except OSError as error:
        return {
            "status": "failed",
            "reason": f"could not start exact-artifact driver: {error}",
            "artifacts": [],
            "commands": [command_record(argv, 127)],
            "consumer_workspace": str(scratch_root),
        }
    command = command_record(argv, completed.returncode)
    print("+", shlex.join(argv), flush=True)
    if completed.stdout:
        print(completed.stdout, end="", flush=True)
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr, flush=True)
    if not child_report_path.is_file():
        return {
            "status": "failed",
            "reason": f"driver exited {completed.returncode} without a JSON evidence report",
            "artifacts": [],
            "commands": [command],
            "consumer_workspace": str(scratch_root),
        }
    try:
        child = json.loads(child_report_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return {
            "status": "failed",
            "reason": f"driver evidence is unreadable: {error}",
            "artifacts": [],
            "commands": [command],
            "consumer_workspace": str(scratch_root),
        }
    return normalize_driver_cell(sdk, child, completed.returncode, command, scratch_root)


def normalize_driver_cell(
    sdk: str, child: dict[str, Any], exit_code: int, command: dict[str, Any], scratch_root: Path
) -> dict[str, Any]:
    """Validate a child driver's report and prevent a nonzero process claiming success."""
    if not isinstance(child, dict) or child.get("schema_version") != 1 or child.get("status") not in {
        "passed", "failed", "blocked", "skipped", "not_run"
    }:
        return {
            "status": "failed",
            "reason": "driver evidence schema_version or status is invalid",
            "artifacts": [],
            "commands": [command],
            "consumer_workspace": str(scratch_root),
        }
    child_artifacts = child.get("artifacts", [])
    child_commands = child.get("commands", [])
    if isinstance(child_artifacts, dict):
        # Some ecosystem drivers naturally key artifacts by package/module.
        # Normalize those records at the boundary while retaining the label.
        normalized_artifacts = []
        for label, artifact in child_artifacts.items():
            if not isinstance(label, str) or not isinstance(artifact, dict):
                return {
                    "status": "failed",
                    "reason": "driver artifact map must contain named artifact objects",
                    "artifacts": [],
                    "commands": [command],
                    "consumer_workspace": str(scratch_root),
                }
            normalized_artifacts.append({"unit": label, **artifact})
        child_artifacts = normalized_artifacts
    if not isinstance(child_artifacts, list) or not isinstance(child_commands, list):
        return {
            "status": "failed",
            "reason": "driver evidence artifacts and commands must be arrays",
            "artifacts": [],
            "commands": [command],
            "consumer_workspace": str(scratch_root),
        }
    reason = child.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        return {
            "status": "failed",
            "reason": "driver evidence must include a non-empty reason for every status",
            "artifacts": child_artifacts,
            "commands": [*child_commands, command],
            "consumer_workspace": child.get("consumer_workspace", str(scratch_root)),
        }
    cell = {
        "status": child["status"],
        "reason": reason,
        "artifacts": child_artifacts,
        "commands": [*child_commands, command],
        "consumer_workspace": child.get("consumer_workspace", str(scratch_root)),
    }
    if isinstance(child.get("consumers"), dict):
        cell["consumer_checks"] = child["consumers"]
    if exit_code != 0 and cell["status"] == "passed":
        cell["status"] = "failed"
        cell["reason"] = "driver reported pass but its process exited nonzero"
    if cell["status"] == "failed" and any(
        marker in reason.casefold()
        for marker in (
            "enotcached",
            "cache mode is 'only-if-cached'",
            "no cached response is available",
            "netsdk1127",
            "microsoft.netcore.app no está instalado",
            "microsoft.netcore.app is not installed",
        )
    ):
        cell["status"] = "blocked"
        if "netsdk1127" in reason.casefold() or "microsoft.netcore.app" in reason.casefold():
            cell["reason"] = f"Required .NET targeting pack is absent from the local toolchain: {reason}"
        else:
            cell["reason"] = f"Offline package dependency is absent from the local cache: {reason}"
    return cell


def run_python_consumer(
    repo: Path, scratch_root: Path, *, wheelhouse_override: Path | None = None,
    build_python_override: Path | None = None,
) -> dict[str, Any]:
    source = repo / "sdk" / "python"
    consumer = source / "consumers" / "artifact_smoke.py"
    if not consumer.is_file():
        raise DriverError(f"checked-in consumer fixture is missing: {consumer}")

    scratch_root.mkdir(parents=True, exist_ok=True)
    commands: list[dict[str, Any]] = []
    artifacts: list[dict[str, str]] = []
    # Keep generated archives and environments as inspectable local evidence.
    # Refuse to reuse a previous run so no user's artifacts are overwritten.
    temp = scratch_root
    temp.mkdir(parents=True, exist_ok=True)
    if any(temp.iterdir()):
        raise DriverError(f"consumer workspace must be empty; preserving existing contents at {temp}")
    dist = temp / "dist"
    dist.mkdir()
    isolated_temp = temp / "tmp"
    isolated_temp.mkdir()
    env = os.environ.copy()
    env.update({"TMP": str(isolated_temp), "TEMP": str(isolated_temp), "TMPDIR": str(isolated_temp)})
    wheelhouse = wheelhouse_override or (
        Path(os.environ["DILOCK_PYTHON_WHEELHOUSE"])
        if os.environ.get("DILOCK_PYTHON_WHEELHOUSE")
        else repo / ".tools" / "wheelhouse"
    )
    wheelhouse = wheelhouse.expanduser().resolve()
    if not wheelhouse.is_dir():
        raise DriverError(f"pinned offline wheelhouse is missing: {wheelhouse}")
    env.update({"PIP_NO_INDEX": "1", "PIP_FIND_LINKS": str(wheelhouse), "PIP_NO_CACHE_DIR": "1"})
    build_python = package_build_python(repo, build_python_override)
    run(
        [str(build_python), "-m", "hatchling", "build", "--directory", str(dist),
         "--target", "wheel", "--target", "sdist"],
        cwd=source,
        env=env,
        records=commands,
    )
    wheels = sorted(dist.glob("dilock-*.whl"))
    sdists = sorted(dist.glob("dilock-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise DriverError(f"expected one Dilock wheel and sdist; found {len(wheels)} and {len(sdists)}",
                          commands=commands)
    wheel, sdist = wheels[0], sdists[0]
    artifacts = [
        {"filename": wheel.name, "sha256": sha256(wheel)},
        {"filename": sdist.name, "sha256": sha256(sdist)},
    ]
    try:
        check_archive_contents(wheel, sdist)
    except DriverError as error:
        error.commands = commands
        error.artifacts = artifacts
        error.consumer_workspace = str(temp)
        raise

    checks: list[dict[str, str]] = []
    derived_builds: list[dict[str, str]] = []

    def install_and_smoke(artifact: Path, venv_dir: Path) -> None:
        run(
            [str(build_python), "-m", "venv", "--without-pip", str(venv_dir)],
            cwd=temp,
            env=env,
            records=commands,
        )
        run(
            [sys.executable, "-m", "pip", "--python", str(venv_dir), "install",
             "--disable-pip-version-check", "--no-index", "--find-links", str(wheelhouse),
             f"{artifact}[redis,etcd]"],
            cwd=temp,
            env=env,
            records=commands,
        )
        run([str(venv_python(venv_dir)), "-I", str(consumer)], cwd=temp, env=env, records=commands)

    try:
        install_and_smoke(wheel, temp / "consumer-wheel")
        checks.append({"name": "wheel_install_and_external_smoke", "status": "passed"})
    except DriverError as error:
        error.artifacts = artifacts
        error.consumer_workspace = str(temp)
        raise

    direct_sdist_block: str | None = None
    try:
        build_site = package_build_site_packages(repo, build_python)
        if not build_site.is_dir():
            raise DriverError(f"pinned Hatchling environment is missing: {build_site}",
                              commands=commands, artifacts=artifacts)
        sdist_venv = temp / "consumer-sdist"
        run(
            [str(build_python), "-m", "venv", "--without-pip", str(sdist_venv)],
            cwd=temp,
            env=env,
            records=commands,
        )
        metadata = tomllib.loads((source / "pyproject.toml").read_text(encoding="utf-8"))
        optional_dependencies = metadata["project"]["optional-dependencies"]
        runtime_requirements = [
            *optional_dependencies["redis"],
            *optional_dependencies["etcd"],
        ]
        # The pinned build environment is exposed to pip so it can import Hatchling
        # without network access. Install runtime requirements into the consumer
        # before that PYTHONPATH is added; otherwise pip can mistake the build
        # environment's copies for dependencies installed in this consumer.
        run(
            [str(build_python), "-m", "pip", "--python", str(sdist_venv), "install",
             "--disable-pip-version-check", "--no-index", "--find-links", str(wheelhouse),
             *runtime_requirements],
            cwd=temp,
            env=env,
            records=commands,
        )
        run(
            [sys.executable, "-m", "pip", "--python", str(sdist_venv), "install",
             "--disable-pip-version-check", "--no-index", "--find-links", str(wheelhouse),
             "--no-build-isolation", f"{sdist}[redis,etcd]"],
            cwd=temp,
            env={**env, "PYTHONPATH": str(build_site)},
            records=commands,
        )
        run([str(venv_python(sdist_venv)), "-I", str(consumer)], cwd=temp, env=env, records=commands)
        checks.append({"name": "sdist_pip_install_and_external_smoke", "status": "passed"})
    except DriverError as error:
        if not error.blocked:
            error.artifacts = artifacts
            error.consumer_workspace = str(temp)
            raise
        direct_sdist_block = str(error)
        checks.append({"name": "sdist_pip_install_and_external_smoke", "status": "blocked"})

    if direct_sdist_block is not None:
        extracted = extract_sdist(sdist, temp / "sdist-source")
        derived_dir = temp / "sdist-derived-wheel"
        derived_dir.mkdir()
        run(
            [str(build_python), "-m", "hatchling", "build", "--directory", str(derived_dir),
             "--target", "wheel"],
            cwd=extracted,
            env=env,
            records=commands,
        )
        derived_wheels = sorted(derived_dir.glob("dilock-*.whl"))
        if len(derived_wheels) != 1:
            raise DriverError(f"expected one wheel rebuilt from the sdist; found {len(derived_wheels)}",
                              commands=commands, artifacts=artifacts)
        derived_wheel = derived_wheels[0]
        derived_builds.append({
            "source_sdist": sdist.name,
            "derived_wheel": derived_wheel.name,
            "sha256": sha256(derived_wheel),
            "status": "passed",
        })
        try:
            install_and_smoke(derived_wheel, temp / "consumer-sdist-derived-wheel")
        except DriverError as error:
            error.artifacts = artifacts
            error.consumer_workspace = str(temp)
            raise
        checks.append({"name": "sdist_rebuild_and_external_smoke", "status": "passed"})

    if direct_sdist_block is not None:
        status = "blocked"
        reason = (
            "The exact wheel installed with both pinned backend extras and passed the external smoke. "
            "The exact sdist rebuilt to a wheel with pinned Hatchling, and that derived wheel also "
            "installed and passed; direct pip installation of the sdist is blocked by the Windows "
            f"sandbox: {direct_sdist_block}"
        )
    else:
        status = "passed"
        reason = (
            "Built wheel and sdist with pinned Hatchling; installed each exact archive and both backend "
            "extras from the local offline wheelhouse into clean Python 3.14 virtual environments, then "
            "ran the external consumer outside the checkout."
        )
    return {
        "status": status,
        "reason": reason,
        "artifacts": artifacts,
        "commands": commands,
        "checks": checks,
        "derived_builds": derived_builds,
        "consumer_workspace": str(temp),
    }


def validate_report(report: dict[str, Any]) -> None:
    if report.get("schema_version") != 1 or report.get("scope") != SCOPE:
        raise DriverError("aggregate report schema version or scope is invalid")
    results = report.get("results")
    if not isinstance(results, dict) or set(results) != set(SDK_NAMES):
        raise DriverError("aggregate report must contain exactly the seven primary SDK cells")
    go = results["go"]
    if go.get("status") != "blocked" or "owner-qualified Go module identity" not in go.get("reason", ""):
        raise DriverError("Go must remain BLOCKED until its owner-qualified module identity is confirmed")
    valid = {"passed", "failed", "skipped", "blocked", "not_run"}
    for sdk, cell in results.items():
        if not isinstance(cell, dict) or cell.get("status") not in valid:
            raise DriverError(f"invalid status cell for {sdk}")
        if not isinstance(cell.get("reason"), str) or not cell["reason"].strip():
            raise DriverError(f"{sdk} {cell['status']} cell must have a non-empty reason")
        if cell["status"] == "passed":
            artifacts = cell.get("artifacts", [])
            commands = cell.get("commands", [])
            if not isinstance(artifacts, list) or not isinstance(commands, list):
                raise DriverError(f"{sdk} passed cell artifacts and commands must be arrays")
            if len(artifacts) < 1 or len(commands) < 1:
                raise DriverError(f"{sdk} cannot pass without an archive hash and recorded commands")
            if any(command.get("exit_code") != 0 for command in commands):
                raise DriverError(f"{sdk} cannot pass with a failed command")
            if any(
                not isinstance(artifact, dict)
                or not artifact.get("filename")
                or not isinstance(artifact.get("sha256"), str)
                or len(artifact["sha256"]) != 64
                or any(char not in "0123456789abcdef" for char in artifact["sha256"])
                for artifact in artifacts
            ):
                raise DriverError(f"{sdk} cannot pass without SHA-256 for each archive")
            if any(
                not isinstance(command, dict)
                or not isinstance(command.get("argv"), list)
                or not command["argv"]
                for command in commands
            ):
                raise DriverError(f"{sdk} cannot pass without recorded command arguments")
            if not cell.get("consumer_workspace"):
                raise DriverError(f"{sdk} cannot pass without a retained consumer workspace path")
    update_overall(report)


def require_complete(report: dict[str, Any]) -> None:
    """Raise unless every required SDK consumer, including Go, has passed."""
    validate_report(report)
    if report["overall_status"] != "passed":
        raise DriverError("required package-consumer matrix is incomplete")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", choices=(*SDK_NAMES, "all"), default="python",
                        help="SDK driver to execute, or all available drivers")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True, help="new JSON report path; existing files are preserved")
    parser.add_argument("--scratch", type=Path, help="temporary build/venv parent (defaults to repository .tools)")
    parser.add_argument("--wheelhouse", type=Path,
                        help="offline Python wheelhouse (or DILOCK_PYTHON_WHEELHOUSE)")
    parser.add_argument("--build-python", type=Path,
                        help="Python with pinned Hatchling (or DILOCK_PYTHON_BUILD_PYTHON)")
    parser.add_argument("--require-all", action="store_true", help="return failure unless every primary SDK cell passed")
    args = parser.parse_args()

    repo = args.repo.resolve()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        parser.error(f"refusing to overwrite existing report: {output}")
    if args.scratch is not None:
        scratch = args.scratch.resolve()
    elif args.sdk == "python":
        scratch = (output.parent / ".package-consumer-temp" / "python").resolve()
    else:
        scratch = (Path(tempfile.gettempdir()) / f"dilock-package-consumers-{uuid.uuid4().hex}").resolve()
    report = initial_report()
    requested = SDK_NAMES if args.sdk == "all" else (args.sdk,)
    for sdk in requested:
        if sdk == "go":
            # Do not invent a module path or let a local replace masquerade as an
            # installable public Go module.
            continue
        sdk_scratch = scratch if args.sdk != "all" else scratch / sdk
        try:
            if sdk == "python":
                report["results"][sdk] = run_python_consumer(
                    repo, sdk_scratch, wheelhouse_override=args.wheelhouse,
                    build_python_override=args.build_python,
                )
            else:
                report["results"][sdk] = run_external_sdk_consumer(repo, sdk, sdk_scratch)
        except (DriverError, OSError, subprocess.SubprocessError) as error:
            status = "blocked" if isinstance(error, PermissionError) or getattr(error, "blocked", False) else "failed"
            report["results"][sdk] = {
                "status": status,
                "reason": f"{type(error).__name__}: {error}",
                "artifacts": getattr(error, "artifacts", []),
                "commands": getattr(error, "commands", []),
                **({"consumer_workspace": getattr(error, "consumer_workspace", str(sdk_scratch))}
                   if sdk_scratch.exists() else {}),
            }
    validate_report(report)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(output), "overall_status": report["overall_status"]}, indent=2))
    if args.require_all:
        try:
            require_complete(report)
        except DriverError:
            return 2
    selected = SDK_NAMES if args.sdk == "all" else (args.sdk,)
    if any(report["results"][sdk]["status"] != "passed" for sdk in selected):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
