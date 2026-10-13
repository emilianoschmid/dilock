"""Read-only candidate checks. Absence is not proof a name is registrable.

No authentication or remote writes. Errors/rate limits remain unverified, not
'available'. Uses Python's standard library; network is optional for development.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


def targets(slug: str) -> list[tuple[str, str]]:
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,39}", slug):
        raise ValueError("candidate must be a lowercase ASCII slug, 3-40 characters")
    result = [
        ("github_repository_name_search", "https://api.github.com/search/repositories?" + urlencode({"q": f"{slug} in:name"})),
        ("github_user_or_org", f"https://api.github.com/users/{quote(slug, safe='')}"),
    ]
    for suffix in ("", "-redis", "-etcd"):
        result.append(("pypi_" + slug + suffix, f"https://pypi.org/pypi/{slug}{suffix}/json"))
    for suffix in (".core", ".redis", ".etcd"):
        result.append(("nuget_" + slug + suffix, f"https://api.nuget.org/v3-flatcontainer/{slug}{suffix}/index.json"))
    result.extend([
        ("npm_unscoped", "https://registry.npmjs.org/" + quote(slug, safe="")),
        ("npm_scoped_core_package_not_scope_reservation", "https://registry.npmjs.org/" + quote(f"@{slug}/core", safe="")),
    ])
    return result


def lookup(label: str, url: str, timeout: float) -> dict:
    item = {"target": label, "url": url, "result": "unverified"}
    request = Request(url, headers={"User-Agent": "Dilock-name-check/0.1", "Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            item["http_status"] = response.status
            payload = response.read(2_000_001)
            if len(payload) > 2_000_000:
                item["detail"] = "response exceeded local size limit"
                return item
            data = json.loads(payload)
        if label == "github_repository_name_search":
            item["result"] = "search_result"
            item["total_count"] = data.get("total_count")
            item["repositories"] = [repo.get("full_name") for repo in data.get("items", [])]
            item["detail"] = "Repository-name matches do not determine organization/scope availability."
        else:
            item["result"] = "found"
    except HTTPError as exc:
        item["http_status"] = exc.code
        item["result"] = "not_found_not_reserved" if exc.code == 404 else "unverified"
        item["detail"] = "A 404 is not a reservation or rights check." if exc.code == 404 else "HTTP failure; availability unknown."
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        item["detail"] = str(exc)
    return item


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", default=None)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 0 < args.timeout <= 60:
        parser.error("--timeout must be in (0, 60] seconds")
    if args.output and args.output.exists():
        parser.error("output already exists; choose a new dated file to preserve evidence")
    slug = args.candidate or json.loads((ROOT / "project.json").read_text(encoding="utf-8"))["slug"]
    try:
        checks = targets(slug)
    except ValueError as exc:
        parser.error(str(exc))
    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "candidate": slug, "reserved": False,
        "warning": "Read-only observations, not exhaustive availability, trademark or ownership clearance.",
        "observations": [lookup(label, url, args.timeout) for label, url in checks],
    }
    text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 2 if any(item["result"] == "unverified" for item in report["observations"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
