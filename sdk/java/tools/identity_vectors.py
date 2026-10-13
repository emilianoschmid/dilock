"""Compile the JDK-only identity implementation and verify all golden vectors."""
from __future__ import annotations

import json
import base64
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SDK = ROOT / "sdk" / "java"


def main() -> int:
    vectors = json.loads((ROOT / "spec" / "vectors" / "identity.json").read_text(encoding="utf-8"))
    output = SDK / "target" / "identity-check"
    output.mkdir(parents=True, exist_ok=True)
    compile_cmd = ["javac", "-d", str(output), str(SDK / "src/main/java/org/dilock/redis/Identity.java"), str(SDK / "tools/IdentitySmoke.java")]
    subprocess.run(compile_cmd, cwd=ROOT, check=True)
    for vector in vectors["valid"]:
        encoded_resource = base64.b64encode(vector["resource"].encode("utf-8")).decode("ascii")
        codepoints = subprocess.run(["java", "-cp", str(output), "IdentitySmoke", "--codes", encoded_resource],
                                   cwd=ROOT, text=True, capture_output=True, check=True)
        expected_codepoints = ",".join(format(ord(char), "x") for char in vector["resource"]) + ","
        if codepoints.stdout != expected_codepoints:
            raise AssertionError(f"{vector['id']}: command-line encoding changed resource: expected {expected_codepoints}, got {codepoints.stdout}")
        result = subprocess.run(["java", "-cp", str(output), "IdentitySmoke", "--vector", vector["namespace"], encoded_resource],
                                cwd=ROOT, text=True, capture_output=True, check=True)
        if result.stdout != vector["sha256"]:
            raise AssertionError(f"{vector['id']}: expected {vector['sha256']}, got {result.stdout}")
        if f"dilock:{{{result.stdout}}}:lease" != vector["redis_key"]:
            raise AssertionError(f"{vector['id']}: Redis key mismatch")
    subprocess.run(["java", "-cp", str(output), "IdentitySmoke"], cwd=ROOT, check=True)
    print(f"PASS Java identity: {len(vectors['valid'])} golden vectors and invalid boundary checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
