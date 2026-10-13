"""Verify the Java and TypeScript embedded scripts equal the canonical Redis Lua files."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def java_constant(text: str, name: str) -> str:
    match = re.search(rf"static final String {name} = (\"(?:\\.|[^\"\\])*\");", text)
    if match is None:
        raise AssertionError(f"missing Java {name} constant")
    return json.loads(match.group(1))


def typescript_constant(text: str, name: str) -> str:
    match = re.search(rf"export const {name} = `([^`]*)`;", text)
    if match is None:
        raise AssertionError(f"missing TypeScript {name} constant")
    return match.group(1).replace("\\n", "\n")


def main() -> None:
    java = (ROOT / "sdk/java/src/main/java/org/dilock/redis/RedisProvider.java").read_text(encoding="utf-8")
    ts = (ROOT / "sdk/typescript/src/redis.ts").read_text(encoding="utf-8")
    for name, file_name in (("RELEASE_LUA", "redis-release.lua"), ("RENEW_LUA", "redis-renew.lua")):
        expected = (ROOT / "spec/backends" / file_name).read_text(encoding="utf-8").replace("\r\n", "\n")
        assert java_constant(java, name) == expected, f"Java {name} differs from canonical Lua"
        assert typescript_constant(ts, name) == expected, f"TypeScript {name} differs from canonical Lua"
    print("PASS Java and TypeScript bundled release/renew Lua match canonical source")


if __name__ == "__main__":
    main()
