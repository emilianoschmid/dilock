"""Fail closed on missing release evidence. This is not a CI proof verifier."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def unresolved_checks(document: dict) -> list[str]:
    """Return incomplete check IDs; a verified check must cite non-empty evidence."""
    checks = document.get("checks")
    if not isinstance(checks, list) or not checks:
        raise ValueError("release evidence must contain a non-empty checks list")
    ids = set()
    pending = []
    for check in checks:
        key = check.get("id")
        if not isinstance(key, str) or not key or key in ids:
            raise ValueError("release check IDs must be non-empty and unique")
        ids.add(key)
        evidence = check.get("evidence")
        valid_evidence = isinstance(evidence, list) and bool(evidence) and all(
            isinstance(item, str) and bool(item.strip()) for item in evidence
        )
        if check.get("state") != "verified" or not valid_evidence:
            pending.append(key)
    return pending


def main() -> int:
    try:
        document = json.loads((ROOT / "planning/release-evidence.json").read_text(encoding="utf-8"))
        pending = unresolved_checks(document)
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        print(f"INVALID RELEASE EVIDENCE: {exc}")
        return 2
    if pending:
        print("NOT READY FOR PUBLICATION: " + ", ".join(pending))
        print("Expected for the blueprint. Continue implementation; do not bypass evidence gates.")
        return 2
    print("Planning evidence fields are complete. This does not verify CI provenance or authorize publication.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
