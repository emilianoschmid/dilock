"""Pack and install all Dilock TypeScript workspace candidates from tarballs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any


FIXTURES = {
    "core": '''import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { identity, redisKey, AcquisitionTimeoutError } from "@dilock/core";
const require = createRequire(import.meta.url);
const id = identity("default", "consumer/core-only");
assert.equal(redisKey(id), `dilock:{${id}}:lease`);
assert.equal(typeof AcquisitionTimeoutError, "function");
for (const name of ["redis", "@grpc/grpc-js", "@grpc/proto-loader"]) {
  assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
}
console.log("core-only consumer OK: identity/errors load without Redis or gRPC");
''',
    "redis": '''import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { RedisProvider, AcquisitionTimeoutError, identity } from "@dilock/redis";
const require = createRequire(import.meta.url);
assert.equal(typeof RedisProvider.connect, "function");
assert.equal(typeof AcquisitionTimeoutError, "function");
assert.equal(typeof identity, "function");
require.resolve("redis");
for (const name of ["@grpc/grpc-js", "@grpc/proto-loader"]) {
  assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
}
console.log("Redis tarball consumer OK: Redis closure present, gRPC closure absent");
''',
    "etcd": '''import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { EtcdProvider, UnsupportedCapabilityError, identity } from "@dilock/etcd";
const require = createRequire(import.meta.url);
assert.equal(typeof EtcdProvider.connect, "function");
assert.equal(typeof UnsupportedCapabilityError, "function");
assert.equal(typeof identity, "function");
require.resolve("@grpc/grpc-js");
require.resolve("@grpc/proto-loader");
assert.throws(() => require.resolve("redis"), { code: "MODULE_NOT_FOUND" });
console.log("etcd tarball consumer OK: gRPC closure present, Redis closure absent");
''',
    "postgres": '''import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { PostgresProvider, PostgresLock, advisoryKey, provisionNamespace } from "@dilock/postgres";
const require = createRequire(import.meta.url);
assert.equal(typeof PostgresProvider.connect, "function");
assert.equal(typeof PostgresLock, "function");
assert.equal(typeof advisoryKey, "function");
assert.equal(typeof provisionNamespace, "function");
require.resolve("pg");
for (const name of ["redis", "@grpc/grpc-js", "@grpc/proto-loader"]) {
  assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
}
console.log("PostgreSQL tarball consumer OK: pg closure present, Redis and gRPC closures absent");
''',
}


def run(argv: list[str], cwd: Path, env: dict[str, str], commands: list[dict[str, Any]]) -> None:
    completed = subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True, check=False)
    commands.append({"cwd": str(cwd), "argv": [str(part) for part in argv], "exit_code": completed.returncode})
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode:
        raise subprocess.CalledProcessError(
            completed.returncode, argv, output=completed.stdout, stderr=completed.stderr
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scratch", type=Path)
    parser.add_argument("--npm-cache", type=Path)
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="allow npm to resolve third-party dependencies when the local cache is incomplete",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[3]
    sdk = Path(__file__).resolve().parents[1]
    scratch = args.scratch.resolve() if args.scratch else None
    output = args.output.resolve()
    commands: list[dict[str, Any]] = []
    artifacts: dict[str, dict[str, str]] = {}
    consumers: dict[str, str] = {}
    status, reason = "failed", "Consumer was not run."
    network_policy = "prefer-offline" if args.allow_network else "offline"

    try:
        (repo / ".tools").mkdir(parents=True, exist_ok=True)
        if scratch is None:
            scratch = Path(tempfile.mkdtemp(prefix="dilock-ts-packages-", dir=repo / ".tools"))
        else:
            if scratch == repo:
                raise ValueError(f"scratch cannot be the repository root: {scratch}")
            if scratch.exists():
                if not scratch.is_dir() or any(scratch.iterdir()):
                    raise ValueError(f"scratch must be an empty directory: {scratch}")
            else:
                scratch.mkdir(parents=True)
        npm = shutil.which("npm.cmd") or shutil.which("npm")
        node = shutil.which("node.exe") or shutil.which("node")
        if not npm or not node:
            raise FileNotFoundError("Node.js and npm must already be installed on PATH")

        cache = args.npm_cache or (
            Path(os.environ["DILOCK_NPM_CACHE"])
            if os.environ.get("DILOCK_NPM_CACHE") else repo / ".tools" / "npm-cache"
        )
        env = os.environ.copy()
        env.update({
            "npm_config_cache": str(cache.resolve()),
            "npm_config_audit": "false",
            "npm_config_fund": "false",
            "npm_config_update_notifier": "false",
            "npm_config_fetch_retries": "0",
        })
        if not args.allow_network:
            env["npm_config_offline"] = "true"
        install_policy = "--prefer-offline" if args.allow_network else "--offline"

        run([npm, "run", "build"], sdk, env, commands)
        artifact_dir = scratch / "artifacts"
        artifact_dir.mkdir()
        tarballs: dict[str, Path] = {}
        for unit in ("core", "redis", "etcd", "postgres"):
            package_name = f"@dilock/{unit}"
            run([npm, "pack", f"--workspace={package_name}", "--pack-destination", str(artifact_dir)], sdk, env, commands)
            matches = [path for path in artifact_dir.glob("*.tgz") if path.name.startswith(f"dilock-{unit}-")]
            if len(matches) != 1:
                raise RuntimeError(f"expected one {unit} npm archive, found {len(matches)}")
            tarball = matches[0]
            tarballs[unit] = tarball
            artifacts[unit] = {"filename": tarball.name, "sha256": hashlib.sha256(tarball.read_bytes()).hexdigest()}

        install_sets = {
            "core": [tarballs["core"]],
            "redis": [tarballs["core"], tarballs["redis"]],
            "etcd": [tarballs["core"], tarballs["etcd"]],
            "postgres": [tarballs["core"], tarballs["postgres"]],
        }
        for unit, packages in install_sets.items():
            consumer = scratch / f"consumer-{unit}"
            consumer.mkdir()
            (consumer / "package.json").write_text(json.dumps({
                "name": f"dilock-{unit}-offline-consumer",
                "private": True,
                "type": "module",
            }, indent=2) + "\n", encoding="utf-8")
            (consumer / "main.mjs").write_text(FIXTURES[unit], encoding="utf-8")
            run([npm, "install", install_policy, "--no-save", "--package-lock=false", "--ignore-scripts", "--no-audit", "--no-fund", "--omit=dev", *map(str, packages)], consumer, env, commands)
            run([node, "main.mjs"], consumer, env, commands)
            consumers[unit] = "passed"

        status = "passed"
        reason = ("All four local archives installed from exact npm tarballs. The core-only fixture resolved identity/errors with no backend packages; each adapter fixture verified its own runtime dependency closure and public exports. "
                  f"Third-party dependency resolution policy: {network_policy}; publication was not performed.")
    except Exception as exc:
        detail = getattr(exc, "stderr", None) or getattr(exc, "output", None)
        suffix = f"\n{detail[-2000:]}" if isinstance(detail, str) and detail else ""
        reason = f"{type(exc).__name__}: {exc}{suffix}"
        print(reason, file=sys.stderr)

    report = {
        "schema_version": 1,
        "status": status,
        "reason": reason,
        "artifacts": artifacts,
        "consumers": consumers,
        "commands": commands,
        "network_policy": network_policy,
        "scratch": str(scratch) if scratch else None,
        "publication": "not_performed",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Report: {output}")
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
