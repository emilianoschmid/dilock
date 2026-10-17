"""Build Rust crate archives offline and run an external consumer against them."""

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


CRATES = ("dilock", "dilock-redis", "dilock-etcd", "dilock-consul")


class ConsumerError(RuntimeError):
    def __init__(self, message: str, *, blocked: bool = False) -> None:
        super().__init__(message)
        self.blocked = blocked


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
        blocked = any(marker in combined.lower() for marker in (
            "no matching package", "no matching version", "could not find",
            "is not installed", "not found", "offline mode",
        ))
        raise ConsumerError(
            f"command exited {result.returncode}: {' '.join(map(str, argv))}; "
            f"{combined[-1500:]}", blocked=blocked,
        )


def write_consumer(project: Path) -> None:
    (project / "src").mkdir(parents=True)
    manifest = """[package]
name = "dilock-archive-consumer"
version = "0.0.0"
edition = "2024"

[dependencies]
dilock = { path = \"crates/dilock\" }
dilock-redis = { path = \"crates/dilock-redis\" }
dilock-etcd = { path = \"crates/dilock-etcd\" }
dilock-consul = { path = \"crates/dilock-consul\" }

[patch.crates-io]
dilock = { path = \"crates/dilock\" }
"""
    (project / "Cargo.toml").write_text(manifest, encoding="utf-8", newline="\n")
    (project / "src/main.rs").write_text(
        """use dilock::{Options, resource_id};
use dilock_etcd::EtcdProvider;
use dilock_consul::{ConsulOptions, ConsulProvider};
use dilock_redis::RedisProvider;

fn validate_public_surface() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        resource_id("default", "camera/123/reindex")?,
        "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6"
    );
    assert!(Options::default().validate().is_ok());
    let redis = RedisProvider::new("redis://127.0.0.1:1/", "default", "consumer-epoch")?;
    let _redis_handle = redis.lock("archive-consumer")?;
    let etcd = EtcdProvider::new("http://127.0.0.1:1", "default", "consumer-epoch")?;
    let _etcd_handle = etcd.lock("archive-consumer")?;
    let consul_options = ConsulOptions {
        namespace: "default".into(),
        coordination_epoch: "archive-consumer-epoch".into(),
        ..ConsulOptions::default()
    };
    let consul = ConsulProvider::new(&consul_options)?;
    let _consul_handle = consul.lock("archive-consumer")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    validate_public_surface()
}

#[cfg(test)]
mod tests {
    #[test]
    fn extracted_crates_expose_the_expected_api() {
        super::validate_public_surface().unwrap();
    }
}
""", encoding="utf-8", newline="\n")


def configured_path(argument: Path | None, variable: str, fallback: Path) -> Path | None:
    """Use explicit cache/tool config, retaining workspace defaults when present."""
    value = argument or (Path(os.environ[variable]) if os.environ.get(variable) else None)
    if value is not None:
        return value.expanduser().resolve()
    return fallback.resolve() if fallback.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True, help="new empty scratch directory")
    parser.add_argument("--archives-dir", type=Path, help="consume existing exact .crate files instead of rebuilding them")
    parser.add_argument("--cargo-home", type=Path, help="offline Cargo cache (or DILOCK_CARGO_HOME)")
    parser.add_argument("--rustup-home", type=Path, help="Rust toolchain home (or DILOCK_RUSTUP_HOME)")
    parser.add_argument("--protoc", type=Path, help="protoc executable (or DILOCK_PROTOC)")
    parser.add_argument(
        "--dilock-c-lib-dir", type=Path,
        help="directory containing the Consul-enabled dilock_c shared library (or DILOCK_C_LIB_DIR)",
    )
    parser.add_argument(
        "--target-dir", type=Path,
        help="reusable Cargo target directory (or DILOCK_CARGO_TARGET_DIR); never cleaned by this driver",
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
        if scratch.exists() and any(scratch.iterdir()):
            raise ConsumerError(f"scratch directory must be empty; preserving contents at {scratch}")
        scratch.mkdir(parents=True, exist_ok=True)
        source = scratch / "source"
        shutil.copytree(
            args.repo.resolve() / "sdk" / "rust",
            source,
            ignore=shutil.ignore_patterns("target", ".git", "__pycache__"),
        )
        env = os.environ.copy()
        env["CARGO_NET_OFFLINE"] = "true"
        native_c_value = args.dilock_c_lib_dir or (
            Path(os.environ["DILOCK_C_LIB_DIR"]) if os.environ.get("DILOCK_C_LIB_DIR") else None
        )
        native_c_lib = native_c_value.expanduser().resolve() if native_c_value else None
        if native_c_lib is None or not native_c_lib.is_dir():
            raise ConsumerError(
                "Consul crate consumer requires a built C ABI installation; pass --dilock-c-lib-dir or set DILOCK_C_LIB_DIR",
                blocked=True,
            )
        env["DILOCK_C_LIB_DIR"] = str(native_c_lib)
        if os.name == "nt":
            env["PATH"] = str(native_c_lib) + os.pathsep + env.get("PATH", "")
        else:
            env["LD_LIBRARY_PATH"] = str(native_c_lib) + os.pathsep + env.get("LD_LIBRARY_PATH", "")
        repo = args.repo.resolve()
        cargo_home = configured_path(args.cargo_home, "DILOCK_CARGO_HOME", repo / ".tools" / "cargo")
        rustup_home = configured_path(args.rustup_home, "DILOCK_RUSTUP_HOME", repo / ".tools" / "rustup")
        protoc_default = repo / ".tools" / "protoc-31.1" / "bin" / ("protoc.exe" if os.name == "nt" else "protoc")
        protoc = configured_path(args.protoc, "DILOCK_PROTOC", protoc_default)
        if cargo_home is not None:
            env["CARGO_HOME"] = str(cargo_home)
        if rustup_home is not None:
            env["RUSTUP_HOME"] = str(rustup_home)
        target_dir = args.target_dir or (
            Path(os.environ["DILOCK_CARGO_TARGET_DIR"])
            if os.environ.get("DILOCK_CARGO_TARGET_DIR") else None
        )
        if target_dir is not None:
            env["CARGO_TARGET_DIR"] = str(target_dir.expanduser().resolve())
        if protoc is not None:
            if not protoc.is_file():
                raise ConsumerError(f"configured protoc executable is missing: {protoc}", blocked=True)
            env["PROTOC"] = str(protoc)
        records = report["commands"]
        cargo = shutil.which("cargo")
        if cargo is None:
            cargo_exe = "cargo.exe" if os.name == "nt" else "cargo"
            default_rustup = rustup_home or Path(env.get("RUSTUP_HOME", Path.home() / ".rustup"))
            toolchains = sorted((default_rustup / "toolchains").glob(f"*/bin/{cargo_exe}"))
            if toolchains:
                cargo = str(toolchains[-1])
        if cargo is None:
            raise ConsumerError("cargo executable is unavailable on PATH and no workspace-local toolchain is installed", blocked=True)
        cargo_bin = str(Path(cargo).resolve().parent)
        env["PATH"] = cargo_bin + os.pathsep + env.get("PATH", "")
        if args.archives_dir is None:
            run([cargo, "package", "--locked", "--offline", "--workspace"], source, records, env)
            archive_dir = Path(env["CARGO_TARGET_DIR"]) / "package" if "CARGO_TARGET_DIR" in env else source / "target" / "package"
            report["package_build"] = "passed"
        else:
            archive_dir = args.archives_dir.expanduser().resolve()
            if not archive_dir.is_dir():
                raise ConsumerError(f"configured archive directory does not exist: {archive_dir}", blocked=True)
            report["package_build"] = "not_run_prebuilt_exact_archives"
            report["archive_source"] = str(archive_dir)
        artifact_dir = scratch / "artifacts"
        artifact_dir.mkdir()
        archives: dict[str, Path] = {}
        for crate in CRATES:
            archive = archive_dir / f"{crate}-0.1.0.crate"
            if not archive.is_file():
                raise ConsumerError(f"cargo package did not produce {archive}")
            copied = artifact_dir / archive.name
            shutil.copy2(archive, copied)
            archives[crate] = copied
            report["artifacts"].append({"filename": copied.name, "sha256": sha256(copied)})

        consumer = scratch / "consumer"
        crate_root = consumer / "crates"
        crate_root.mkdir(parents=True)
        extracted: dict[str, Path] = {}
        for crate, archive in archives.items():
            with tarfile.open(archive, "r:gz") as package:
                prefix = f"{crate}-0.1.0/"
                members = package.getmembers()
                if not members or any(not member.name.startswith(prefix) for member in members):
                    raise ConsumerError(f"unexpected paths in crate archive {archive.name}")
                if any(member.issym() or member.islnk() or not (member.isdir() or member.isfile()) for member in members):
                    raise ConsumerError(f"unsupported file type in crate archive {archive.name}")
                target = crate_root / crate
                target.mkdir()
                for member in members:
                    relative = member.name[len(prefix):]
                    if not relative:
                        continue
                    destination = (target / relative).resolve()
                    if target.resolve() not in destination.parents and destination != target.resolve():
                        raise ConsumerError(f"unsafe path in {archive.name}: {member.name}")
                    member.name = relative
                    package.extract(member, target)
                if not (target / "Cargo.toml").is_file():
                    raise ConsumerError(f"extracted crate is missing Cargo.toml: {archive.name}")
                extracted[crate] = target
        write_consumer(consumer)
        manifest = consumer / "Cargo.toml"
        run([cargo, "generate-lockfile", "--manifest-path", str(manifest), "--offline"], consumer, records, env)
        run([cargo, "check", "--manifest-path", str(manifest), "--locked", "--offline"], consumer, records, env)
        run([cargo, "test", "--manifest-path", str(manifest), "--locked", "--offline"], consumer, records, env)
        run([cargo, "run", "--manifest-path", str(manifest), "--locked", "--offline"], consumer, records, env)
        report.update({
            "status": "passed",
            "reason": (
                "Built current workspace crates offline, extracted those exact archives, then compiled, tested, and ran an external consumer."
                if args.archives_dir is None else
                "Consumed the exact supplied .crate archives offline, extracted them, then compiled, tested, and ran an external consumer."
            ),
            "cargo_target_dir": env.get("CARGO_TARGET_DIR"),
        })
    except (ConsumerError, OSError, tarfile.TarError, shutil.Error) as error:
        report["status"] = "blocked" if getattr(error, "blocked", False) or isinstance(error, PermissionError) else "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(output), "status": report["status"]}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
