"""Build Java backend artifacts and verify isolated Maven consumer dependency closures."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


VERSION = "0.1.0"
MODULES = ("core", "redis", "etcd", "consul", "postgres")


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="new JSON report; existing files are preserved")
    parser.add_argument("--scratch", required=True, type=Path, help="new empty workspace for the copied source and isolated consumers")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--maven", help="Maven executable (default: DILOCK_MAVEN, bundled Maven, or PATH)")
    parser.add_argument("--maven-repo", type=Path, help="Maven dependency cache; defaults to DILOCK_MAVEN_REPO or the checkout cache")
    args = parser.parse_args()
    repo = args.repo.resolve()
    output = args.output.resolve()
    scratch = args.scratch.resolve()
    source = repo / "sdk" / "java"
    bundled = repo / ".tools" / "apache-maven-3.10.0" / "bin" / ("mvn.cmd" if os.name == "nt" else "mvn")
    maven_value = args.maven or os.environ.get("DILOCK_MAVEN") or (str(bundled) if bundled.is_file() else "mvn")
    maven_path = Path(maven_value)
    maven = str(maven_path.resolve()) if maven_path.is_file() else maven_value
    maven_settings = repo / ".tools" / "maven-settings.xml"
    local_repo = (args.maven_repo or Path(os.environ["DILOCK_MAVEN_REPO"])
                  if os.environ.get("DILOCK_MAVEN_REPO") and args.maven_repo is None
                  else args.maven_repo or repo / ".tools" / "m2repo").resolve()
    report: dict[str, object] = {
        "schema_version": 1,
        "status": "not_run",
        "coordinate_status": "local_candidate_only",
        "source_copy": False,
        "commands": [],
        "artifacts": [],
        "module_consumers": {},
        "clean_package": {"status": "not_run", "reason": "maven-clean-plugin 3.5.0 is not present in the offline cache"},
        "publication": "not_run",
    }
    if output.exists():
        parser.error(f"refusing to overwrite existing report: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    if any(scratch.iterdir()):
        report["status"] = "blocked"
        report["reason"] = f"scratch directory must be empty; preserving {scratch}"
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return 1
    if scratch == repo or repo in scratch.parents:
        report["status"] = "blocked"
        report["reason"] = "scratch must be outside the checkout"
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return 1

    env = os.environ.copy()
    env.pop("HTTP_PROXY", None)
    env.pop("HTTPS_PROXY", None)
    env.pop("ALL_PROXY", None)
    env.pop("http_proxy", None)
    env.pop("https_proxy", None)
    env.pop("all_proxy", None)
    commands: list[dict[str, object]] = report["commands"]  # type: ignore[assignment]

    def run(label: str, argv: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        invocation = subprocess.list2cmdline(argv) if os.name == "nt" and argv[0].lower().endswith(".cmd") else argv
        result = subprocess.run(invocation, cwd=cwd, env=env, shell=isinstance(invocation, str),
                                capture_output=True, text=True, check=False)
        commands.append({"label": label, "argv": argv, "exit_code": result.returncode})
        if result.stdout:
            print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr, flush=True)
        if result.returncode:
            raise RuntimeError(f"{label} exited {result.returncode}: {(result.stderr or result.stdout)[-1800:]}")
        return result

    try:
        if not Path(maven).is_file() and not shutil.which(maven):
            raise RuntimeError("Maven executable is unavailable")
        source_copy = scratch / "source" / "sdk" / "java"
        shutil.copytree(source, source_copy, ignore=shutil.ignore_patterns("target", ".idea", ".vscode"))
        shutil.copytree(repo / "spec", scratch / "source" / "spec")
        shutil.copytree(source / "consumers", scratch / "consumer")
        report["source_copy"] = True
        settings = ["-s", str(maven_settings)] if maven_settings.is_file() else []
        common = [maven, "-o", "-B", "-ntp", *settings, f"-Dmaven.repo.local={local_repo}"]
        source_pom = source_copy / "pom.xml"
        run("fresh copied-source reactor install", [*common, "-f", str(source_pom), "-pl", "core,redis,etcd,consul,postgres", "-am", "install", "-DskipTests"], source_copy)

        artifacts: list[dict[str, object]] = []
        for module in MODULES:
            stem = f"dilock-{module}-{VERSION}"
            built = source_copy / module / "target"
            installed = local_repo / "org" / "dilock" / f"dilock-{module}" / VERSION
            for suffix in (".jar", "-sources.jar", "-javadoc.jar"):
                source_archive = built / f"{stem}{suffix}"
                local_archive = installed / f"{stem}{suffix}"
                if not source_archive.is_file() or not local_archive.is_file():
                    raise RuntimeError(f"missing packaged or installed classifier: {source_archive} / {local_archive}")
                built_hash, installed_hash = digest(source_archive), digest(local_archive)
                if built_hash != installed_hash:
                    raise RuntimeError(f"installed {source_archive.name} hash differs from its reactor output")
                artifacts.append({"module": module, "filename": source_archive.name, "sha256": built_hash,
                                  "installed_sha256": installed_hash, "status": "passed"})
        report["artifacts"] = artifacts

        consumer_pom = scratch / "consumer" / "pom.xml"
        run("isolated six-consumer Maven build", [*common, "-f", str(consumer_pom), "package"], consumer_pom.parent)
        module_results: dict[str, dict[str, object]] = {}
        for module in ("core-only", "redis-only", "etcd-only", "consul-only", "postgres-only", "combined"):
            pom = scratch / "consumer" / module / "pom.xml"
            output_file = scratch / f"{module}-classpath.txt"
            run(f"{module} runtime dependency closure", [*common, "-f", str(pom), "org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath",
                 "-Dmdep.includeScope=runtime", f"-Dmdep.outputFile={output_file}"], pom.parent)
            classpath = output_file.read_text(encoding="utf-8").strip()
            lowered = classpath.lower()
            forbidden = {"core-only": ("jedis", "jetcd", "consul", "postgresql"), "redis-only": ("jetcd", "consul", "postgresql"), "etcd-only": ("jedis", "consul", "postgresql"), "consul-only": ("jedis", "jetcd", "postgresql"), "postgres-only": ("jedis", "jetcd", "consul")}.get(module, ())
            leaked = [dependency for dependency in forbidden if dependency in lowered]
            if leaked:
                raise RuntimeError(f"{module} consumer unexpectedly resolved backend dependencies: {leaked}")
            if module == "redis-only" and "jedis" not in lowered:
                raise RuntimeError("Redis-only consumer did not resolve Jedis")
            if module == "etcd-only" and "jetcd" not in lowered:
                raise RuntimeError("etcd-only consumer did not resolve Jetcd")
            if module == "consul-only" and "jackson-databind" not in lowered:
                raise RuntimeError("Consul-only consumer did not resolve Jackson")
            if module == "postgres-only" and "postgresql" not in lowered:
                raise RuntimeError("PostgreSQL-only consumer did not resolve the JDBC driver")
            classes = scratch / "consumer" / module / "target" / "classes"
            main_class = {"core-only": "CoreConsumer", "redis-only": "RedisConsumer",
                          "etcd-only": "EtcdConsumer", "consul-only": "ConsulConsumer",
                          "postgres-only": "PostgresConsumer", "combined": "PackageConsumer"}[module]
            java = os.environ.get("DILOCK_JAVA") or shutil.which("java")
            if not java:
                candidate = Path(os.environ.get("JAVA_HOME", "")) / "bin" / ("java.exe" if os.name == "nt" else "java")
                if not candidate.is_file():
                    raise RuntimeError("Java executable unavailable; set DILOCK_JAVA or JAVA_HOME")
                java = str(candidate)
            runtime_classpath = os.pathsep.join((str(classes), classpath))
            run(f"run {module} consumer", [java, "-cp", runtime_classpath, main_class], scratch / "consumer")
            module_results[module] = {"status": "passed", "resolved_classpath": classpath,
                                      "forbidden_backend_dependencies": list(forbidden)}
        report["module_consumers"] = module_results
        report["status"] = "passed"
        report["reason"] = "Fresh source-copy reactor tests and install passed; core-only, Redis-only, etcd-only, Consul-only, PostgreSQL-only and combined external consumers compiled and ran with backend dependency isolation verified."
    except Exception as error:
        report["status"] = "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "report": str(output)}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
