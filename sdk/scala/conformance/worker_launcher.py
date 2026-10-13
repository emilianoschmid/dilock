"""Run the Scala test worker in a pinned Docker image while preserving JSONL stdio."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
IMAGE = "maven:3.9.9-eclipse-temurin-21@sha256:3a4ab3276a087bf276f79cae96b1af04f53731bec53fb2e651aca79e4b10211e"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: worker_launcher.py <config.json>", file=sys.stderr)
        return 2
    config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    backend = config.get("backend")
    network = os.environ.get("DILOCK_SCALA_WORKER_NETWORK")
    endpoint = os.environ.get(f"DILOCK_SCALA_{str(backend).upper()}_WORKER_ENDPOINT")
    if not network or not endpoint:
        print("Scala worker requires DILOCK_SCALA_WORKER_NETWORK and backend endpoint", file=sys.stderr)
        return 2
    config["endpoint"] = endpoint
    docker = [
        "docker", "run", "--rm", "--pull=never", "-i", "--network", network,
        "-e", "DILOCK_SCALA_WORKER_CONFIG_JSON=" + json.dumps(config, separators=(",", ":")),
        "-v", f"{ROOT / 'sdk' / 'scala'}:/source:ro",
        "-v", f"{ROOT / '.tools' / 'm2repo'}:/cache:ro",
        IMAGE, "bash", "-lc",
        "set -eu; CP=$(find /cache -type f -name '*.jar' "
        "! -path '/cache/org/dilock/dilock-java/*' | sort | paste -sd:); "
        "mkdir -p /tmp/scala-worker-classes; "
        "java -cp \"$CP\" dotty.tools.dotc.Main -deprecation -feature -unchecked "
        "-classpath \"$CP\" -d /tmp/scala-worker-classes "
        "/source/src/main/scala/org/dilock/scala3/Dilock.scala "
        "/source/src/test/scala/org/dilock/scala3/ConformanceWorker.scala; "
        "mvn -o -f /source/conformance/worker-runtime-pom.xml -Dmaven.repo.local=/cache "
        "-Dmdep.outputFile=/tmp/scala-worker-runtime-classpath.txt "
        "org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath; "
        "RUNTIME_CP=$(cat /tmp/scala-worker-runtime-classpath.txt); "
        "exec java -cp \"/tmp/scala-worker-classes:$RUNTIME_CP\" "
        "org.dilock.scala3.ConformanceWorker /worker-config-not-used.json",
    ]
    process = subprocess.Popen(docker, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None)

    def pump_input() -> None:
        assert process.stdin is not None
        try:
            while line := sys.stdin.buffer.readline():
                process.stdin.write(line)
                process.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                process.stdin.close()
            except OSError:
                pass

    # Keep the wrapper alive until its parent closes JSONL stdin. This avoids
    # abandoning a blocking buffered read when SBT exits after startup failure.
    thread = threading.Thread(target=pump_input, name="scala-worker-input", daemon=False)
    thread.start()
    assert process.stdout is not None
    for line in process.stdout:
        # SBT startup text may share stdout. Only protocol objects reach the harness.
        stripped = line.lstrip()
        if stripped.startswith(b"{"):
            try:
                json.loads(line)
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
                continue
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
        sys.stderr.buffer.write(line)
        sys.stderr.buffer.flush()
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
