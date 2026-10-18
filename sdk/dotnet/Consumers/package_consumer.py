"""Build exact NuGet archives and run a clean consumer outside the checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any


VERSION = "0.1.0-dev.1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="new JSON report; existing files are preserved")
    parser.add_argument("--scratch", required=True, type=Path, help="empty directory outside the source checkout")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument(
        "--dotnet", default=os.environ.get("DILOCK_DOTNET", "dotnet"),
        help="dotnet executable (default: DILOCK_DOTNET or dotnet from PATH)",
    )
    parser.add_argument(
        "--nuget-cache", type=Path,
        default=os.environ.get("DILOCK_DOTNET_NUGET_CACHE"),
        help="pre-populated offline NuGet dependency cache (default: DILOCK_DOTNET_NUGET_CACHE or REPO/.tools/dotnet-example-packages)",
    )
    args = parser.parse_args()
    repo = args.repo.resolve()
    output = args.output.resolve()
    scratch = args.scratch.resolve()
    source_cache = Path(args.nuget_cache).resolve() if args.nuget_cache else repo / ".tools/dotnet-example-packages"
    fixture = Path(__file__).resolve().parent / "PackageConsumer"
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "not_run",
        "reason": "consumer has not run",
        "artifacts": [],
        "commands": [],
        "consumer_workspace": str(scratch),
    }
    if output.exists():
        parser.error(f"refusing to overwrite existing report: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    if any(scratch.iterdir()):
        report.update(status="blocked", reason=f"scratch directory must be empty; preserving {scratch}")
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": report["status"], "report": str(output)}, indent=2))
        return 1
    if scratch == repo or repo in scratch.parents:
        report.update(status="blocked", reason="scratch must be outside the source checkout")
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": report["status"], "report": str(output)}, indent=2))
        return 1

    env = os.environ.copy()
    env["DOTNET_NOLOGO"] = "1"
    env["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1"
    commands: list[dict[str, Any]] = report["commands"]

    def run(argv: list[str], cwd: Path) -> None:
        try:
            completed = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, check=False)
        except OSError as error:
            commands.append({"argv": argv, "exit_code": 127})
            raise RuntimeError(f"could not start {argv[0]}: {error}") from error
        commands.append({"argv": argv, "exit_code": completed.returncode})
        print("+", " ".join(argv), flush=True)
        if completed.stdout:
            print(completed.stdout, end="", flush=True)
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr, flush=True)
        if completed.returncode:
            raise RuntimeError(f"command exited {completed.returncode}: {argv!r}: {(completed.stderr or completed.stdout)[-1600:]}")

    try:
        feed = scratch / "feed"
        feed.mkdir()
        for project in (
            repo / "sdk/dotnet/Dilock.Core/Dilock.Core.csproj",
            repo / "sdk/dotnet/Dilock.Redis/Dilock.Redis.csproj",
            repo / "sdk/dotnet/Dilock.Etcd/Dilock.Etcd.csproj",
            repo / "sdk/dotnet/Dilock.Consul/Dilock.Consul.csproj",
            repo / "sdk/dotnet/Dilock.Postgres/Dilock.Postgres.csproj",
        ):
            run([args.dotnet, "pack", str(project), "--configuration", "Release", "--output", str(feed), "--no-restore"], repo)
        packages = sorted(feed.glob(f"Dilock.*.{VERSION}.nupkg"))
        expected = {"Dilock.Core", "Dilock.Redis", "Dilock.Etcd", "Dilock.Consul", "Dilock.Postgres"}
        if {path.name.removesuffix(f".{VERSION}.nupkg") for path in packages} != expected:
            raise RuntimeError(f"expected the exact Core, Redis, Etcd, Consul and PostgreSQL nupkg archives, found {[p.name for p in packages]}")
        report["artifacts"] = [{"filename": item.name, "sha256": sha256(item)} for item in packages]
        consumer = scratch / "consumer"
        shutil.copytree(fixture, consumer)
        nuget_config = consumer / "NuGet.Config"
        nuget_config.write_text(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
            "<configuration><packageSources><clear/><add key=\"local-exact-artifacts\" value=\""
            + str(feed).replace("&", "&amp;").replace("\\", "/")
            + "\"/></packageSources></configuration>\n",
            encoding="utf-8",
        )
        # NuGet's user-profile global cache is outside this sandbox's writable roots.
        # Seed an isolated copy of the already provisioned offline cache into scratch so
        # restore proves local-feed package resolution without mutating shared caches.
        package_cache = scratch / "global-packages"
        if not source_cache.is_dir():
            raise RuntimeError(f"offline NuGet dependency cache is unavailable: {source_cache}")
        shutil.copytree(
            source_cache,
            package_cache,
            ignore=shutil.ignore_patterns("dilock.core", "dilock.redis", "dilock.etcd", "dilock.consul", "dilock.postgres"),
        )
        env["NUGET_PACKAGES"] = str(package_cache)
        run([args.dotnet, "restore", str(consumer / "PackageConsumer.csproj"), "--configfile", str(nuget_config), "--ignore-failed-sources"], consumer)
        for artifact in packages:
            package_id = artifact.name.removesuffix(f".{VERSION}.nupkg")
            restored_copy = package_cache / package_id.lower() / VERSION / artifact.name.lower()
            if not restored_copy.is_file():
                raise RuntimeError(f"NuGet did not restore the exact local archive into the isolated package cache: {package_id}")
            if sha256(restored_copy) != sha256(artifact):
                raise RuntimeError(f"restored {package_id} archive hash differs from the package built into the local feed")
        run([args.dotnet, "run", "--project", str(consumer / "PackageConsumer.csproj"), "--configuration", "Release", "--no-restore"], consumer)
        report.update(status="passed", reason="Built five local NuGet archives from current SDK sources; removed same-version Dilock entries from an isolated offline cache, verified all restored NuGet archive hashes against the local feed, and ran the checkout-external consumer.")
    except Exception as error:  # Preserve a machine-readable failure with every attempted command.
        report.update(status="failed", reason=f"{type(error).__name__}: {error}")
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "report": str(output)}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
