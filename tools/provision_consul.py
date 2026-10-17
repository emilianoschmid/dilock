"""Explicitly create or verify draft-1 Consul namespace metadata."""
from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request


def _request(endpoint: str, path: str, *, method: str, token: str | None,
             body: bytes | None = None) -> tuple[int, bytes]:
    headers = {"Accept": "application/json"}
    if token:
        headers["X-Consul-Token"] = token
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(endpoint.rstrip("/") + path, data=body,
                                     headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5, context=ssl.create_default_context()) as response:
            return response.status, response.read(16384)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Consul HTTP request rejected ({exc.code})") from None
    except (urllib.error.URLError, TimeoutError):
        raise RuntimeError("Consul request failed") from None


def provision_consul(endpoint: str, namespace: str, epoch: str,
                     token: str | None = None) -> str:
    """Create only when absent; a conflicting value is never overwritten."""
    from dilock.core import identity

    identity(namespace, "validation")
    if not isinstance(epoch, str) or not epoch or "\0" in epoch or len(epoch.encode("utf-8", "strict")) > 1024:
        raise ValueError("invalid epoch")
    metadata = {
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "consul-kv-session-v1",
        "coordination_epoch": epoch,
        "fixed_lease_ms": None,
    }
    key = f"dilock/namespaces/{namespace}/meta"
    route = "/v1/kv/" + urllib.parse.quote(key, safe="/")
    payload = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    status, result = _request(
        endpoint, route + "?cas=0", method="PUT", token=token, body=payload,
    )
    try:
        created = json.loads(result)
    except (ValueError, TypeError) as exc:
        raise RuntimeError("Consul CAS response was malformed") from exc
    status, result = _request(
        endpoint, route + "?consistent=true", method="GET", token=token,
    )
    try:
        rows = json.loads(result)
        if not isinstance(rows, list) or len(rows) != 1 or rows[0].get("Key") != key:
            raise ValueError
        raw = base64.b64decode(rows[0]["Value"], validate=True)
        current = json.loads(raw)
        if not isinstance(current, dict) or current != metadata:
            raise ValueError
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise RuntimeError("existing Consul namespace metadata conflicts; value was not modified") from exc
    if type(created) is not bool:
        raise RuntimeError("Consul CAS response was not boolean")
    return "created" if created else "compatible existing metadata"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--epoch", required=True)
    args = parser.parse_args()
    token = os.environ.get("CONSUL_HTTP_TOKEN")
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python" / "src"))
    print(provision_consul(args.endpoint, args.namespace, args.epoch, token))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
