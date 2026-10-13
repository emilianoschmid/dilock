"""Small dispatcher for the native Dilock toolchains; it never installs tools."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SDKS = ("dotnet", "cpp", "python", "typescript", "go", "rust", "java")
BACKENDS = ("redis", "etcd")


def command_for(sdk: str, task: str, backend: str | None = None) -> list[list[str]]:
    """Return native commands. Each package keeps its own build/test behavior."""
    local_python = str(ROOT / ".tools/venv/Scripts/python.exe")
    py = local_python if Path(local_python).is_file() else sys.executable
    maven = str(ROOT / ".tools/apache-maven-3.10.0/bin/mvn.cmd")
    if not Path(maven).is_file():
        maven = "mvn"
    if task == "lint":
        return {
            "dotnet": [["dotnet", "format", "Dilock.Core", "--verify-no-changes"]],
            "cpp": [["cmake", "--build", str(ROOT / "build/cpp"), "--parallel", "4"]],
            "python": [[py, "-m", "ruff", "check", "src", "tests"], [py, "-m", "mypy", "src/dilock"]],
            "typescript": [["npm", "run", "build"]],
            "go": [["gofmt", "-d", "."], ["go", "vet", "./..."]],
            "rust": [["cargo", "fmt", "--all", "--", "--check"], ["cargo", "clippy", "--locked", "--offline", "--workspace", "--all-targets", "--", "-D", "warnings"]],
            "java": [[maven, "-s", str(ROOT / ".tools/maven-settings.xml"), "-f", "pom.xml", "javadoc:javadoc"]],
        }[sdk]
    if task == "test":
        return {
            "dotnet": [["dotnet", "test", "Tests", "-c", "Release"]],
            "cpp": [["ctest", "--test-dir", str(ROOT / "build/cpp"), "--output-on-failure"]],
            "python": [[py, "-m", "pytest", "tests"]],
            "typescript": [["npm", "test"]],
            "go": [["go", "test", "./..."]],
            "rust": [["cargo", "test", "--locked", "--offline", "--workspace"]],
            "java": [[maven, "-s", str(ROOT / ".tools/maven-settings.xml"), "-f", "pom.xml", "test"]],
        }[sdk]
    if task == "integration":
        if backend not in BACKENDS:
            raise ValueError("integration requires --backend redis or etcd")
        if sdk == "python":
            return [[py, "-m", "pytest", f"tests/test_{backend}_real.py", "-v"]]
        if sdk == "go":
            return [["go", "test", "-count=1", f"./{backend}"]]
        if sdk == "rust":
            return [["cargo", "test", "--locked", "--offline", "-p", f"dilock-{backend}", "--test", f"{backend}_real", "--", "--ignored", "--nocapture"]]
        if sdk == "typescript":
            return [["npm", "test"]]
        if sdk == "java":
            if backend == "etcd":
                return [[py, "tools/etcd_worker_smoke.py"]]
        if sdk == "dotnet" and backend == "etcd":
            return [["dotnet", "test", "Tests", "-c", "Release"]]
        if sdk == "cpp":
            build = ROOT / (".tools/dilock-cpp-etcd" if backend == "etcd" else "build/cpp")
            return [["ctest", "--test-dir", str(build), "--output-on-failure", "-R", backend]]
        raise ValueError(f"no native real-{backend} integration command is configured for {sdk}")
    if task == "pack":
        return {
            "dotnet": [["dotnet", "pack", "Dilock.Core", "-c", "Release"],
                       ["dotnet", "pack", "Dilock.Redis", "-c", "Release"],
                       ["dotnet", "pack", "Dilock.Etcd", "-c", "Release"]],
            "cpp": [["cmake", "--install", str(ROOT / "build/cpp"), "--prefix", str(ROOT / ".tools/dev-cpp-stage")]],
            "python": [[py, "-m", "build", "--no-isolation"]],
            "typescript": [["npm", "run", "pack:check"]],
            "go": [["go", "list", "./..."]],
            "rust": [["cargo", "package", "--locked", "--offline", "--workspace", "--list"]],
            "java": [[maven, "-s", str(ROOT / ".tools/maven-settings.xml"), "-f", "pom.xml", "package"]],
        }[sdk]
    raise ValueError(f"unsupported native operation: {task} {sdk}")


def run(commands: list[list[str]], cwd: Path, env: dict[str, str] | None = None) -> int:
    for command in commands:
        print("$ " + subprocess.list2cmdline(command), flush=True)
        try:
            result = subprocess.run(command, cwd=cwd, env=env, check=False)
        except FileNotFoundError as exc:
            print(f"BLOCKED: required executable not found: {exc.filename}", file=sys.stderr)
            return 127
        if result.returncode:
            is_strict_style_run = any(Path(part).name == "run_style_examples.py" for part in command)
            if is_strict_style_run and result.returncode == 2:
                print("BLOCKED: strict local style run contains blocked or skipped samples", file=sys.stderr)
            else:
                print(f"FAILED: exit code {result.returncode}", file=sys.stderr)
            return result.returncode
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    doctor = sub.add_parser("doctor", help="report installed SDK tools; never installs")
    doctor.add_argument("--sdk", choices=SDKS, action="append")
    lint = sub.add_parser("lint")
    lint.add_argument("--sdk", choices=SDKS, required=True)
    test = sub.add_parser("test")
    test.add_argument("--sdk", choices=SDKS, required=True)
    integration = sub.add_parser("integration")
    integration.add_argument("--sdk", choices=SDKS, required=True)
    integration.add_argument("--backend", choices=BACKENDS, required=True)
    integration.add_argument("--endpoint")
    interop = sub.add_parser("interop")
    interop.add_argument("--backend", choices=BACKENDS, required=True)
    interop.add_argument("--endpoint")
    interop.add_argument("--sdks", nargs="+", choices=SDKS, default=list(SDKS))
    faults = sub.add_parser("faults", help="run real-process n-way, killed-owner expiry, and renewed-owner process-kill checks")
    faults.add_argument("--backend", choices=BACKENDS, required=True)
    faults.add_argument("--endpoint", help="Backend URL; kept out of the printed command line.")
    faults.add_argument("--sdks", nargs="+", choices=SDKS)
    faults.add_argument("--output")
    pack = sub.add_parser("pack")
    pack.add_argument("--sdk", choices=SDKS, required=True)
    docs = sub.add_parser("docs", help="execute style samples; does not test backends")
    docs.add_argument("--sdks", nargs="+", choices=SDKS)
    sub.add_parser("verify-release", help="run fail-closed local evidence gates")
    args = parser.parse_args()

    if args.operation == "doctor":
        tools = {"dotnet": "dotnet", "cpp": "cmake", "python": "python", "typescript": "node",
                 "go": "go", "rust": "cargo", "java": "java"}
        selected = args.sdk or list(SDKS)
        missing = []
        for sdk in selected:
            path = shutil.which(tools[sdk])
            state = f"available ({path})" if path else "missing"
            print(f"{sdk}: {state}")
            if not path:
                missing.append(sdk)
        print("No tools were installed or system settings changed.")
        return int(bool(missing))
    if args.operation == "interop":
        command = [str(ROOT / ".tools/venv/Scripts/python.exe"), "conformance/run.py",
                   "--backend", args.backend, "--sdks", *args.sdks]
        if not Path(command[0]).is_file():
            command[0] = sys.executable
        if args.endpoint:
            command += ["--endpoint", args.endpoint]
        return run([command], ROOT)
    if args.operation == "faults":
        py = str(ROOT / ".tools/venv/Scripts/python.exe")
        if not Path(py).is_file():
            py = sys.executable
        output = args.output or f"validation/implementation/m6-nway-{args.backend}-latest.json"
        command = [py, "conformance/nway.py", "--backend", args.backend, "--output", output]
        if args.sdks:
            command += ["--sdks", *args.sdks]
        env = os.environ.copy()
        if args.endpoint:
            env["DILOCK_NWAY_ENDPOINT"] = args.endpoint
        return run([command], ROOT, env)
    if args.operation == "docs":
        py = str(ROOT / ".tools/venv/Scripts/python.exe")
        if not Path(py).is_file():
            py = sys.executable
        command = [py, "tools/run_style_examples.py", "--strict"]
        command.extend(args.sdks or ["--all"])
        return run([command], ROOT)
    if args.operation == "verify-release":
        status = run([[sys.executable, "tools/bootstrap_check.py"],
                      [sys.executable, "tools/release_readiness.py"]], ROOT)
        return status
    sdk = args.sdk
    backend = getattr(args, "backend", None)
    env = None
    if args.operation == "integration":
        endpoint = args.endpoint or ("redis://127.0.0.1:16379" if backend == "redis" else "http://127.0.0.1:22379")
        namespace = "dev-check-" + uuid.uuid4().hex
        epoch = "local-dev-check-v1"
        env = os.environ.copy()
        if backend == "redis":
            env.update(DILOCK_REDIS_URL=endpoint, DILOCK_NAMESPACE=namespace, DILOCK_EPOCH=epoch)
            provision = [str(ROOT / ".tools/venv/Scripts/python.exe"), "tools/provision.py",
                         "--endpoint", endpoint, "--namespace", namespace, "--epoch", epoch]
        else:
            env.update(DILOCK_ETCD_ENDPOINT=endpoint, DILOCK_ETCD_NAMESPACE=namespace,
                       DILOCK_ETCD_EPOCH=epoch)
            provision = [str(ROOT / ".tools/venv/Scripts/python.exe"), "-c",
                         "from tools.provision_etcd import provision_etcd; import sys; print(provision_etcd(*sys.argv[1:]))",
                         endpoint, namespace, epoch]
        if not Path(provision[0]).is_file():
            provision[0] = sys.executable
        status = run([provision], ROOT, env)
        if status:
            return status
    commands = command_for(sdk, args.operation, backend)
    if args.operation == "integration" and sdk == "java" and backend == "etcd":
        commands[0].extend(["--endpoint", endpoint, "--namespace", namespace, "--epoch", epoch])
    return run(commands, ROOT / "sdk" / sdk, env)


if __name__ == "__main__":
    raise SystemExit(main())
