"""Provision or verify draft-1 etcd namespace metadata for local conformance runs."""
from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]


def provision_etcd(endpoint: str, namespace: str, epoch: str) -> str:
    """Create metadata only in a unique test namespace; reject conflicting values."""
    if not namespace or not epoch:
        raise ValueError("namespace and epoch are required")
    parsed = urlsplit(endpoint if "://" in endpoint else f"http://{endpoint}")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("etcd endpoint must be an HTTP(S) gRPC host and port")
    target = f"{parsed.scheme}://{parsed.netloc}"
    configured = os.environ.get("ETCDCTL")
    bundled = ROOT / ".tools/etcd/etcd-v3.7.2-windows-amd64/etcdctl.exe"
    executable = configured or (str(bundled) if bundled.is_file() else shutil.which("etcdctl"))
    if not executable:
        raise RuntimeError("etcdctl is required to provision conformance namespace metadata")
    key = f"/dilock/namespaces/{namespace}/meta"
    value = json.dumps({
        "protocol_major": 1,
        "identity_scheme": "sha256-framed-utf8-v1",
        "profile": "etcd-native-lock-v1",
        "coordination_epoch": epoch,
        "fixed_lease_ms": None,
    }, separators=(",", ":"))
    result = subprocess.run(
        [executable, f"--endpoints={target}", "get", key, "--write-out=json"],
        check=True, capture_output=True, text=True, timeout=10,
    )
    data = json.loads(result.stdout)
    kvs = data.get("kvs", [])
    if kvs:
        existing = json.loads(base64.b64decode(kvs[0]["value"]).decode("utf-8"))
        if existing != json.loads(value):
            raise RuntimeError("existing etcd namespace metadata conflicts; left unchanged")
        return "verified-existing"
    subprocess.run(
        [executable, f"--endpoints={target}", "put", key, value],
        check=True, capture_output=True, text=True, timeout=10,
    )
    return "created"
