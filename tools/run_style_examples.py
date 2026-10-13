"""Build/run local-only samples with installed tools; never download toolchains.

Missing prerequisites are skipped, not passed. --strict fails on skipped samples.
Outputs are isolated in a unique workspace-local scratch directory. This does not run real backends.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
CATALOG = json.loads((ROOT / "tools/style-examples.json").read_text())
SAMPLES = [{"id": x["id"], "sample": x["path"]} for x in CATALOG["examples"]]

def commands(sdk: str, work: Path) -> tuple[list[str], list[list[str]]]:
    if sdk == "python": return [], [[sys.executable, "-B", "dilock_style.py"]]
    if sdk == "cpp": return ["g++"], [["g++", "-std=c++20", "-Wall", "-Wextra", "-Werror", "-pedantic", "-pthread", "main.cpp", "-o", "run"], [str(work / "run")]]
    if sdk == "c": return ["gcc"], [["gcc", "-std=c17", "-Wall", "-Wextra", "-Werror", "-pedantic", "main.c", "dilock_style.c", "-o", "run"], [str(work / "run")]]
    if sdk == "dotnet": return ["dotnet"], [["dotnet", "run", "--project", "Dilock.Style.csproj"]]
    if sdk == "typescript": return ["node"], [["node", str(ROOT / "sdk/typescript/node_modules/typescript/bin/tsc"), "-p", "tsconfig.json"], ["node", "dist/main.js"]]
    if sdk == "go": return ["go", "gcc"], [["go", "test", "-race", "-count=1", "-v", "./..."], ["go", "vet", "./..."]]
    if sdk == "rust":
        # The task may have a pinned rustup toolchain under .tools while keeping
        # its CARGO_HOME/bin empty. Resolve that installed binary without
        # downloading or installing anything.
        cargo = shutil.which("cargo")
        if not cargo:
            candidate = ROOT / ".tools/rustup/toolchains/1.99.0-x86_64-pc-windows-msvc/bin/cargo.exe"
            cargo = str(candidate) if candidate.is_file() else "cargo"
        return [cargo], [[cargo, "test", "--offline"], [cargo, "run", "--offline"]]
    if sdk == "java": return ["javac", "java"], [["javac", "--release", "21", "-Xlint:all", "DilockStyle.java"], ["java", "DilockStyle"]]
    if sdk == "kotlin": return ["kotlinc", "java"], [["kotlinc", "Main.kt", "-include-runtime", "-d", "run.jar"], ["java", "-jar", "run.jar"]]
    if sdk == "php": return ["php"], [["php", "-l", "main.php"], ["php", "main.php"]]
    if sdk == "ruby": return ["ruby"], [["ruby", "-w", "main.rb"]]
    if sdk == "swift": return ["swiftc"], [["swiftc", "-swift-version", "6", "-strict-concurrency=complete", "-warnings-as-errors", "-parse-as-library", "Main.swift", "-o", "run"], [str(work / "run")]]
    if sdk == "dart":
        dart = shutil.which("dart") or "dart"
        if os.name == "nt" and dart.lower().endswith((".bat", ".cmd")):
            bundled = Path(dart).parent / "cache" / "dart-sdk" / "bin" / "dart.exe"
            if bundled.is_file(): dart = str(bundled)
        return ["dart"], [[dart, "analyze", "main.dart"], [dart, "run", "main.dart"]]
    if sdk == "elixir": return ["elixir"], [["elixir", "main.exs"]]
    if sdk == "scala": return ["scala-cli"], [["scala-cli", "run", "main.scala", "--offline", "--server=false", "--main-class", "runDilockStyle"]]
    raise ValueError("unknown SDK " + sdk)

def is_process_launch_block(output: str) -> bool:
    """Recognize Windows sandbox failures emitted by a child launcher.

    Some runtimes (notably Dart) start successfully, then fail to create their
    analyzer/runtime child process. That is an environment block, not an SDK
    example assertion or compilation failure.
    """
    normalized = output.casefold()
    return "createfile failed 5" in normalized or (
        "processexception" in normalized
        and ("access denied" in normalized or "acceso denegado" in normalized)
    )

def run_one(sample: dict, base: Path) -> dict:
    sdk = sample["id"]
    work = base / sdk
    shutil.copytree(ROOT / sample["sample"], work)
    needed, steps = commands(sdk, work)
    missing = [tool for tool in needed if not shutil.which(tool)]
    result = {"sdk": sdk, "scope": "in-process-style-only", "status": "skipped" if missing else "passed", "missing_tools": missing, "commands": []}
    if missing:
        return result
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", GOTOOLCHAIN="local", GOWORK="off",
               GOPROXY="off", GOSUMDB="off", DOTNET_CLI_TELEMETRY_OPTOUT="1")
    if sdk == "rust":
        env.update(RUSTUP_HOME=str(ROOT / ".tools/rustup"), CARGO_HOME=str(ROOT / ".tools/cargo"),
                   RUSTUP_TOOLCHAIN="1.99.0")
    for command in steps:
        start = time.monotonic()
        try:
            completed = subprocess.run(command, cwd=work, env=env, text=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, timeout=120, check=False)
            record = {"command": command, "exit_code": completed.returncode,
                      "seconds": round(time.monotonic() - start, 3), "output": completed.stdout}
            result["commands"].append(record)
            if completed.returncode:
                result["status"] = "blocked" if is_process_launch_block(completed.stdout) else "failed"
                break
        except OSError as exc:
            result["commands"].append({"command": command, "error": str(exc)})
            result["status"] = "blocked" if isinstance(exc, PermissionError) else "failed"
            break
        except subprocess.TimeoutExpired as exc:
            result["commands"].append({"command": command, "error": str(exc)})
            result["status"] = "failed"
            break
    return result

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sdks", nargs="*")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "validation/style-results.json")
    args = parser.parse_args()
    known = {x["id"] for x in SAMPLES}
    unknown = set(args.sdks) - known
    if unknown: parser.error("unknown SDKs: " + ", ".join(sorted(unknown)))
    chosen = SAMPLES if args.all or not args.sdks else [x for x in SAMPLES if x["id"] in args.sdks]
    results = []
    # Keep scratch work inside the workspace. Restricted Windows runners may
    # deny access to the profile temp folder even when the project is writable.
    # TemporaryDirectory applies a restrictive ACL in this Windows workspace;
    # plain mkdir inherits the workspace ACL and keeps the run auditable.
    work_root = ROOT / ".tools" / ("dilock-style-" + uuid.uuid4().hex)
    work_root.mkdir(parents=True)
    for sample in chosen:
        result = run_one(sample, work_root)
        results.append(result)
        print(f'{result["status"].upper()}: {sample["id"]}' + (" (missing " + ", ".join(result["missing_tools"]) + ")" if result["missing_tools"] else ""), flush=True)
        if result["status"] in {"failed", "blocked"}:
            print(json.dumps(result, indent=2), flush=True)
    counts = {status: sum(x["status"] == status for x in results) for status in ["passed", "failed", "blocked", "skipped"]}
    evidence = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "not_a_distributed_test": True,
                "summary": counts, "results": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(counts));print("No backend integration, package publication or CI execution was performed.")
    if counts["failed"]:
        return 1
    if args.strict and (counts["blocked"] or counts["skipped"]):
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
