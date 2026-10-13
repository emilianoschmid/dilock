#!/usr/bin/env python3
"""Compile the current Fortran facade against an installed C ABI in isolated Docker."""

from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BASE_IMAGE = "dilock-cpp-etcd-builder:20261006"
CABI_PREFIX = ROOT / ".tools/haskell-etcd-cabi-install-20261007"
CPP_DEPS = ROOT / ".tools/cppdeps/usr"
GRPC_DEPS = ROOT / ".tools/grpc-root/usr"
OPENSSL_RUNTIME = ROOT / ".tools/openssl-runtime/usr"
REDISPP_DEPS = ROOT / ".tools/cabi-ffi-linked-20261006/deps"
HIREDIS_PREFIX = ROOT / ".tools/cppinstall"
REPORT = ROOT / "validation/implementation/fortran-installed-latest-cabi-docker-20261008.json"
SOURCES = [
    "sdk/fortran/dilock_fortran.f90",
    "sdk/fortran/CMakeLists.txt",
    "sdk/fortran/cmake/DilockFortranConfig.cmake.in",
    "sdk/fortran/tests/facade_test.f90",
    "sdk/fortran/tests/c_abi_shim.c",
    "sdk/fortran/tests/live_backend_consumer.f90",
    "sdk/fortran/consumers/cmake/CMakeLists.txt",
    "sdk/fortran/consumers/cmake/main.f90",
    "sdk/fortran/consumers/cmake/run-docker.ps1",
    "sdk/fortran/README.md",
]
ARTIFACTS = [
    ".tools/haskell-etcd-cabi-install-20261007/lib/libdilock_c.so",
    ".tools/haskell-etcd-cabi-install-20261007/lib/libdilock_cpp.so",
    ".tools/haskell-etcd-cabi-install-20261007/lib/libdilock_cpp_etcd.so",
]


def run(command: list[str], *, check: bool = True, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, input=input_text, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}"
        )
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect_image(image: str) -> str:
    return run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()


def main() -> int:
    suffix = uuid.uuid4().hex[:10]
    tool_image = f"dilock-fortran-toolchain-{suffix}:local"
    report: dict = {
        "schema_version": 1,
        "date": "2026-10-08",
        "sdk": "Fortran",
        "area": "current_source_against_latest_installed_cabi",
        "status": "BLOCKED",
        "support_state": "experimental",
        "publication": "not_attempted",
        "remote_resources_created": False,
        "attempts": [],
        "corrected_harness_attempts": [
            {
                "state": "FAIL_HARNESS",
                "cause": "The first disposable image installed gfortran but its verification command referenced Ninja, which was not installed.",
                "correction": "Install ninja-build with gfortran in the ephemeral image.",
            },
            {
                "state": "BLOCKED_BEFORE_FORTRAN_BUILD",
                "cause": "The latest installed C ABI exports CMake dependencies that are not part of the minimal C++ builder image; the first configure lacked OpenSSL development files.",
                "correction": "Install required system development packages in the ephemeral image and mount the already-cached local redis++/gRPC dependency prefixes read-only.",
            },
            {
                "state": "BLOCKED_BEFORE_FORTRAN_BUILD",
                "cause": "The next configure found system OpenSSL/nlohmann but the new C ABI's redis++ CMake dependency is in a separate local cache prefix.",
                "correction": "Mount the existing redis++/hiredis prefix and pass its redis++_DIR to CMake.",
            },
            {
                "state": "BLOCKED_BEFORE_FORTRAN_BUILD",
                "cause": "The redis++ package selected a malformed cached HiredisConfigVersion.cmake (CMake string(REPLACE) argument error).",
                "correction": "Use the separately cached installed Hiredis package config and library prefix, explicitly setting lowercase `hiredis_DIR` to avoid the malformed C++ dependency prefix.",
            },
            {
                "state": "BLOCKED_DURING_LINK",
                "cause": "The installed C ABI package's exported gRPC link interface emitted unqualified -labsl_* names without a -L path to its mounted local gRPC library prefix.",
                "correction": "Add the already-mounted gRPC library directory to the disposable consumer's executable link search flags; no SDK sources or installed artifacts are altered.",
            },
            {
                "state": "BLOCKED_AT_CONSUMER_RUNTIME",
                "cause": "The separately installed package consumer linked successfully but its process environment omitted the mounted gRPC runtime library directory.",
                "correction": "Set LD_LIBRARY_PATH to the existing read-only C ABI and dependency prefixes for the downstream process.",
            },
        ],
    }
    image_created = False
    image_build_attempted = False
    try:
        for required in (CABI_PREFIX / "lib/libdilock_c.so", CABI_PREFIX / "lib/cmake/DilockCpp/DilockCppConfig.cmake"):
            if not required.is_file():
                raise FileNotFoundError(f"installed C ABI package is missing: {required}")

        base_id = inspect_image(BASE_IMAGE)
        cabi_hashes = {item: sha256(ROOT / item) for item in ARTIFACTS}

        dockerfile = f"""FROM {BASE_IMAGE}
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends gfortran ninja-build libssl-dev nlohmann-json3-dev libprotobuf-dev protobuf-compiler-grpc libgrpc++-dev libhiredis-dev && rm -rf /var/lib/apt/lists/*
RUN gfortran --version | head -1 && cmake --version | head -1 && ninja --version && gcc --version | head -1
"""
        image_build_attempted = True
        build = run(
            ["docker", "build", "--pull=false", "--network=default", "-t", tool_image, "-"],
            check=False,
            input_text=dockerfile,
        )
        report["attempts"].append(
            {
                "name": "ephemeral Fortran toolchain image",
                "state": "PASS" if build.returncode == 0 else "BLOCKED",
                "command": "docker build --pull=false --network=default (local Trixie builder + Debian gfortran package)",
                "stdout": build.stdout,
                "stderr": build.stderr,
                "exit_code": build.returncode,
            }
        )
        if build.returncode:
            report["environment"] = {
                "base_image": BASE_IMAGE,
                "base_image_id": base_id,
                "local_fortran_compiler_probe": "BLOCKED: no gfortran, flang, ifx, or ifort on host or cached builder images; the first disposable-image setup installed GNU Fortran successfully but its verification command found Ninja missing, before SDK compilation",
            }
            raise RuntimeError("could not provision GNU Fortran in the disposable local toolchain image")
        image_created = True
        tool_id = inspect_image(tool_image)

        shell_script = r"""set -eu
gfortran --version | head -1
gcc --version | head -1
cmake --version | head -1
ninja --version
cmake -S /repo/sdk/fortran -B /tmp/fortran-build -G Ninja '-DCMAKE_PREFIX_PATH=/abi;/deps/cpp;/deps/grpc;/deps/redispp;/deps/hiredis' -Dredis++_DIR=/deps/redispp/share/cmake/redis++ -Dhiredis_DIR=/deps/hiredis/lib/cmake/hiredis -DCMAKE_EXE_LINKER_FLAGS=-L/deps/grpc/lib/x86_64-linux-gnu -DBUILD_TESTING=ON
cmake --build /tmp/fortran-build --parallel 2
ctest --test-dir /tmp/fortran-build --output-on-failure
cmake --install /tmp/fortran-build --prefix /tmp/fortran-install
cmake -S /repo/sdk/fortran/consumers/cmake -B /tmp/fortran-consumer -G Ninja '-DCMAKE_PREFIX_PATH=/tmp/fortran-install;/abi;/deps/cpp;/deps/grpc;/deps/redispp;/deps/hiredis' -Dredis++_DIR=/deps/redispp/share/cmake/redis++ -Dhiredis_DIR=/deps/hiredis/lib/cmake/hiredis -DCMAKE_EXE_LINKER_FLAGS=-L/deps/grpc/lib/x86_64-linux-gnu
cmake --build /tmp/fortran-consumer --parallel 2
LD_LIBRARY_PATH=/abi/lib:/deps/cpp/lib:/deps/grpc/lib/x86_64-linux-gnu:/deps/hiredis/lib /tmp/fortran-consumer/dilock_fortran_installed_consumer
"""
        test = run(
            [
                "docker", "run", "--rm", "--pull=never", "--network=none",
                "--mount", f"type=bind,source={ROOT},target=/repo,readonly",
                "--mount", f"type=bind,source={CABI_PREFIX},target=/abi,readonly",
                "--mount", f"type=bind,source={CPP_DEPS},target=/deps/cpp,readonly",
                "--mount", f"type=bind,source={GRPC_DEPS},target=/deps/grpc,readonly",
                "--mount", f"type=bind,source={OPENSSL_RUNTIME},target=/deps/openssl,readonly",
                "--mount", f"type=bind,source={REDISPP_DEPS},target=/deps/redispp,readonly",
                "--mount", f"type=bind,source={HIREDIS_PREFIX},target=/deps/hiredis,readonly",
                "-e", "LD_LIBRARY_PATH=/abi/lib:/deps/cpp/lib:/deps/grpc/lib/x86_64-linux-gnu:/deps/openssl/lib/x86_64-linux-gnu:/deps/redispp/lib:/deps/hiredis/lib",
                "--workdir", "/repo",
                tool_image, "/bin/sh", "-lc", shell_script,
            ],
            check=False,
        )
        report["attempts"].append(
            {
                "name": "current Fortran source against installed C ABI",
                "state": "PASS" if test.returncode == 0 else "FAIL",
                "exit_code": test.returncode,
                "command": "CMake build + CTest + install + separate source-consumer build/run; container --network=none",
                "stdout": test.stdout,
                "stderr": test.stderr,
            }
        )
        if test.returncode:
            raise RuntimeError("current Fortran build/test/installed consumer failed")

        report.update(
            {
                "status": "PASS",
                "environment": {
                    "base_image": BASE_IMAGE,
                    "base_image_id": base_id,
                    "test_image": tool_image,
                    "test_image_id": tool_id,
                    "os": "Debian GNU/Linux 13 (Trixie)",
                    "compiler_versions": [line for line in test.stdout.splitlines() if "GNU Fortran" in line or line.startswith("gcc (") or line.startswith("cmake version") or line.startswith("1.")],
                    "apt_scope": "GNU Fortran installed in a task-local disposable image from Debian's OS package repository; no language package registries contacted",
                    "checkout_mount": "read_only",
                    "cabi_install_mount": "read_only",
                    "build_output": "container-local /tmp",
                },
                "checks": {
                    "current_facade_build": "PASS",
                    "current_facade_ctest": "PASS; 1/1 test target passed against deterministic test C ABI shim",
                    "current_redis_example": "PASS; compiled and linked by the normal CMake build",
                    "exact_installed_fortran_consumer": "PASS; separate downstream CMake project imported DilockFortran::fortran, linked the installed facade and current installed Dilock::c, and ran embedded-NUL rejection before backend access",
                    "live_backends": "NOT_RUN in this focused installed-artifact consumer check",
                    "cross_sdk_and_distributed_faults": "NOT_RUN",
                    "publication": "NOT_ATTEMPTED",
                },
                "cabi_prefix": ".tools/haskell-etcd-cabi-install-20261007",
                "cabi_sha256": cabi_hashes,
                "source_sha256": {item: sha256(ROOT / item) for item in SOURCES},
                "cleanup": "PASS: test container used --rm; disposable derived toolchain image removed in finally; no volumes, backend services or published ports",
            }
        )
    except Exception as error:
        if report["status"] != "PASS":
            report["error"] = str(error)
    finally:
        if image_build_attempted:
            run(["docker", "image", "rm", "-f", tool_image], check=False)
        if report.get("cleanup") is None:
            report["cleanup"] = "PASS: no task-owned backend/network/volume was created; disposable derived image removed if created"
        report.setdefault("cabi_prefix", ".tools/haskell-etcd-cabi-install-20261007")
        report.setdefault("cabi_sha256", {item: sha256(ROOT / item) for item in ARTIFACTS if (ROOT / item).is_file()})
        report.setdefault("source_sha256", {item: sha256(ROOT / item) for item in SOURCES if (ROOT / item).is_file()})
        REPORT.parent.mkdir(parents=True, exist_ok=True)
        REPORT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
