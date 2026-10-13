"""Build and launch the native Java JSONL conformance worker."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SDK = ROOT / "sdk" / "java"
WORKER = SDK / "worker"
CLASSPATH_FILE = WORKER / "target" / "runtime-classpath.txt"
MAVEN = ROOT / ".tools" / "apache-maven-3.10.0" / "bin" / "mvn.cmd"
SETTINGS = ROOT / ".tools" / "maven-settings.xml"
MAVEN_REPO = ROOT / ".tools" / "m2repo"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    command = [str(MAVEN) if MAVEN.exists() else "mvn", "-q"]
    if SETTINGS.exists(): command += ["-s", str(SETTINGS)]
    command += [f"-Dmaven.repo.local={MAVEN_REPO}", "-f", str(SDK / "pom.xml"),
                "-Pconformance-worker", "-pl", "worker", "-am", "install"]
    environment = os.environ.copy()
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        if environment.get(name, "").lower() in {"http://127.0.0.1:9", "https://127.0.0.1:9"}:
            environment.pop(name, None)
    result = subprocess.run(subprocess.list2cmdline(command) if MAVEN.exists() else command,
                            cwd=ROOT, shell=MAVEN.exists(), env=environment, stdin=subprocess.DEVNULL)
    if result.returncode:
        return result.returncode
    classpath_command = [str(MAVEN) if MAVEN.exists() else "mvn", "-q"]
    if SETTINGS.exists(): classpath_command += ["-s", str(SETTINGS)]
    classpath_command += [f"-Dmaven.repo.local={MAVEN_REPO}", "-f", str(WORKER / "pom.xml"),
                          "dependency:build-classpath", "-Dmdep.includeScope=runtime",
                          f"-Dmdep.outputFile={CLASSPATH_FILE}"]
    classpath_result = subprocess.run(
        subprocess.list2cmdline(classpath_command) if MAVEN.exists() else classpath_command,
        cwd=ROOT, shell=MAVEN.exists(), env=environment, stdin=subprocess.DEVNULL)
    if classpath_result.returncode:
        return classpath_result.returncode
    module_classes = [SDK / name / "target" / "classes" for name in ("core", "redis", "etcd", "worker")]
    classpath = os.pathsep.join([*(str(path) for path in module_classes), CLASSPATH_FILE.read_text(encoding="utf-8").strip()])
    return subprocess.run(["java", "-cp", classpath, "org.dilock.redis.Worker", sys.argv[1]], cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
