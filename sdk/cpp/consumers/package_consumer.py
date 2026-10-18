"""Build a Linux CMake package offline and consume its exact tar archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any


class ConsumerError(RuntimeError):
    def __init__(self, message: str, *, blocked: bool = False) -> None:
        super().__init__(message)
        self.blocked = blocked


def validate_toolchain_file(value: str | None) -> Path | None:
    """Resolve an optional CMake toolchain file and reject missing paths."""
    if not value:
        return None
    path = Path(value).expanduser()
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ConsumerError(f"CMake toolchain file does not exist or cannot be accessed: {path}") from error
    if not resolved.is_file():
        raise ConsumerError(f"CMake toolchain path is not a file: {resolved}")
    return resolved


def toolchain_cmake_args(toolchain_file: Path | None) -> list[str]:
    """Return the exact CMake argv element for an explicitly selected toolchain."""
    if toolchain_file is None:
        return []
    return [f"-DCMAKE_TOOLCHAIN_FILE={toolchain_file}"]


def sdk_configure_args(cmake: str, sdk: Path, build: Path, stage: Path, toolchain_file: Path | None) -> list[str]:
    """Build the CMake argv used to configure and install the SDK package."""
    return [
        cmake, "-S", str(sdk), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release",
        "-DDILOCK_BUILD_TESTS=OFF", "-DDILOCK_BUILD_ETCD=ON", "-DDILOCK_BUILD_POSTGRES=ON",
        f"-DCMAKE_INSTALL_PREFIX={stage}", *toolchain_cmake_args(toolchain_file),
    ]


def consumer_configure_args(cmake: str, consumer: Path, build: Path, prefix: Path, toolchain_file: Path | None) -> list[str]:
    """Build the external consumer's CMake argv using the same dependency toolchain."""
    return [
        cmake, "-S", str(consumer), "-B", str(build),
        f"-DCMAKE_PREFIX_PATH={prefix}", "-DCMAKE_BUILD_TYPE=Release",
        *toolchain_cmake_args(toolchain_file),
    ]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(argv: list[str], cwd: Path, records: list[dict[str, Any]], env: dict[str, str]) -> None:
    print("+", " ".join(str(part) for part in argv), flush=True)
    try:
        result = subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True, check=False)
    except OSError as error:
        records.append({"argv": list(map(str, argv)), "exit_code": 127})
        raise ConsumerError(f"could not start command: {error}", blocked=True) from error
    records.append({"argv": list(map(str, argv)), "exit_code": result.returncode})
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    if result.returncode:
        combined = result.stdout + result.stderr
        unavailable = any(marker in combined.lower() for marker in (
            "could not find a package configuration file", "not found", "no such file",
            "could not be found", "is unavailable",
        ))
        raise ConsumerError(
            f"command exited {result.returncode}: {' '.join(map(str, argv))}; "
            f"{combined[-1500:]}", blocked=unavailable,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True, help="new empty scratch directory")
    parser.add_argument(
        "--toolchain-file", default=os.environ.get("DILOCK_CPP_TOOLCHAIN_FILE"),
        help="optional CMake toolchain file (default: DILOCK_CPP_TOOLCHAIN_FILE)",
    )
    args = parser.parse_args()
    output = args.output.resolve()
    scratch = args.scratch.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        parser.error(f"refusing to overwrite existing report: {output}")
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "not_run",
        "reason": "consumer has not started",
        "artifacts": [],
        "commands": [],
        "consumer_workspace": str(scratch),
    }
    try:
        toolchain_file = validate_toolchain_file(args.toolchain_file)
        if not sys.platform.startswith("linux"):
            raise ConsumerError("the checked-in C++ artifact profile is Linux-only; Linux SDK build/consumer was not run", blocked=True)
        if scratch.exists() and any(scratch.iterdir()):
            raise ConsumerError(f"scratch directory must be empty; preserving contents at {scratch}")
        scratch.mkdir(parents=True, exist_ok=True)
        repo = args.repo.resolve()
        sdk = repo / "sdk" / "cpp"
        build = scratch / "build"
        stage = scratch / "stage"
        cmake = shutil.which("cmake")
        if cmake is None:
            raise ConsumerError("cmake executable is unavailable", blocked=True)
        env = os.environ.copy()
        records = report["commands"]
        run(sdk_configure_args(cmake, sdk, build, stage, toolchain_file), repo, records, env)
        run([cmake, "--build", str(build), "--parallel", "2"], repo, records, env)
        run([cmake, "--install", str(build)], repo, records, env)

        archive = scratch / "dilock-cpp-0.1.0-linux-x86_64-cmake.tar.gz"
        with tarfile.open(archive, "w:gz", format=tarfile.PAX_FORMAT) as package:
            package.add(stage, arcname="prefix", recursive=True)
        report["artifacts"].append({"filename": archive.name, "sha256": sha256(archive)})
        extracted = scratch / "extracted"
        extracted.mkdir()
        with tarfile.open(archive, "r:gz") as package:
            members = package.getmembers()
            for member in members:
                target = (extracted / member.name).resolve()
                if extracted.resolve() not in target.parents and target != extracted.resolve():
                    raise ConsumerError(f"unsafe path in package archive: {member.name}")
                if member.issym() or member.islnk() or not (member.isdir() or member.isfile()):
                    raise ConsumerError(f"unsupported file type in package archive: {member.name}")
            package.extractall(extracted)
        prefix = extracted / "prefix"
        if not (prefix / "lib" / "cmake" / "DilockCpp" / "DilockCppConfig.cmake").is_file():
            raise ConsumerError("CMake package config is absent from the extracted archive")

        consumer = scratch / "consumer"
        shutil.copytree(sdk / "consumers" / "cmake", consumer)
        consumer_build = scratch / "consumer-build"
        run(consumer_configure_args(cmake, consumer, consumer_build, prefix, toolchain_file), repo, records, env)
        run([cmake, "--build", str(consumer_build), "--parallel", "2"], repo, records, env)
        exe = consumer_build / "dilock_cpp_artifact_consumer"
        if not exe.is_file():
            raise ConsumerError(f"external consumer executable was not produced: {exe}")
        run([str(exe)], consumer, records, env)
        report.update({
            "status": "passed",
            "reason": "Built current C++ Redis+etcd+PostgreSQL CMake install offline, archived and extracted that exact prefix, then configured, linked, and ran the external consumer against it.",
        })
    except (ConsumerError, OSError, tarfile.TarError, shutil.Error) as error:
        report["status"] = "blocked" if getattr(error, "blocked", False) or isinstance(error, PermissionError) else "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(output), "status": report["status"]}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
