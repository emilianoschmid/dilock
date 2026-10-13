"""Keep the peer-partition quorum probe fail-closed on unrelated CLI errors."""

from __future__ import annotations

import subprocess
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from conformance import etcd_peer_partition
from conformance.etcd_peer_partition import WslPeerPartitionProxy, quorum_write_probe


def test_quorum_probe_accepts_only_explicit_leader_or_quorum_failure(
    monkeypatch,
) -> None:
    def run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["etcdctl"],
            returncode=1,
            stdout="",
            stderr="Error: context deadline exceeded",
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = quorum_write_probe("etcdctl", "http://127.0.0.1:2379", "/probe", 2)
    assert result["status"] == "failed"
    assert result["failure_class"] == "no_quorum_or_leader"


def test_quorum_probe_does_not_accept_unrelated_command_failure(monkeypatch) -> None:
    def run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["etcdctl"],
            returncode=1,
            stdout="",
            stderr="Error: unknown flag: --bad",
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = quorum_write_probe("etcdctl", "http://127.0.0.1:2379", "/probe", 2)
    assert result["status"] == "failed"
    assert result["failure_class"] == "unrelated_command_failure"


def test_quorum_probe_fails_if_write_committed(monkeypatch) -> None:
    def run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["etcdctl"], returncode=0, stdout="OK", stderr=""
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = quorum_write_probe("etcdctl", "http://127.0.0.1:2379", "/probe", 2)
    assert result["status"] == "committed"
    assert result["failure_class"] == "committed"


def test_quorum_probe_timeout_needs_prior_member_status_evidence(monkeypatch) -> None:
    def run(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="etcdctl put", timeout=5)

    monkeypatch.setattr(subprocess, "run", run)
    unverified = quorum_write_probe("etcdctl", "http://127.0.0.1:2379", "/probe", 2)
    verified = quorum_write_probe(
        "etcdctl",
        "http://127.0.0.1:2379",
        "/probe",
        2,
        endpoint_status_observed=True,
    )
    assert unverified["failure_class"] == "unverified_command_timeout"
    assert verified["failure_class"] == "no_quorum_or_leader_timeout"


def test_peer_proxy_control_requires_matching_synchronous_acknowledgment() -> None:
    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            request = json.loads(await reader.readline())
            await asyncio.sleep(0.01)
            writer.write(
                json.dumps(
                    {
                        "request_id": request["request_id"],
                        "status": "ok",
                        "state": {
                            "partitioned": request["command"] == "partition",
                            "active_connections": 0,
                            "partition_transitions": 1,
                        },
                    }
                ).encode()
                + b"\n"
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        proxy = WslPeerPartitionProxy.__new__(WslPeerPartitionProxy)
        proxy.control_port = server.sockets[0].getsockname()[1]
        proxy.last_snapshot = None
        proxy.process = None
        try:
            response = await proxy.control("partition")
            assert response["status"] == "ok"
            assert response["state"]["partitioned"] is True
            assert proxy.last_snapshot == response["state"]
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_peer_proxy_control_rejects_mismatched_request_id() -> None:
    async def scenario() -> None:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readline()
            writer.write(
                b'{"request_id":"stale","status":"ok","state":{"partitioned":true}}\n'
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        proxy = WslPeerPartitionProxy.__new__(WslPeerPartitionProxy)
        proxy.control_port = server.sockets[0].getsockname()[1]
        proxy.last_snapshot = None
        proxy.process = None
        try:
            with pytest.raises(RuntimeError, match="acknowledgment ID mismatch"):
                await proxy.control("partition")
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_majority_minority_matrix_covers_six_directed_edges_without_self_links(
    monkeypatch
) -> None:
    async def scenario() -> None:
        names = ["m6-p1", "m6-p2", "m6-p3"]
        majority = set(names[:2])
        expected = {
            (source, destination): source in majority and destination in majority
            for source in names
            for destination in names
            if source != destination
        }
        proxies = [SimpleNamespace(identity_member=name) for name in names]
        observed = []

        async def probe(
            proxy,
            source,
            destination,
            _cert_dir,
            fingerprint,
            should_forward,
            _timeout,
        ):
            row = {
                "source": source,
                "destination": destination,
                "status": "passed",
                "decision": "forwarded" if should_forward else "blocked",
                "certificate_sha256": fingerprint,
            }
            observed.append(row)
            return row

        monkeypatch.setattr(etcd_peer_partition, "probe_directed_peer_edge", probe)
        result = await etcd_peer_partition.verify_directed_peer_matrix(
            proxies,
            names,
            Path("."),
            {name: ("a" if name == "m6-p1" else "b") * 64 for name in names},
            expected,
            1,
        )
        assert result["status"] == "passed"
        assert result["valid_directed_edge_count"] == 6
        assert result["observed_directed_edge_count"] == 6
        assert len(observed) == 6
        assert not any(row["source"] == row["destination"] for row in observed)
        assert sum(row["decision"] == "forwarded" for row in observed) == 2
        assert sum(row["decision"] == "blocked" for row in observed) == 4

    asyncio.run(scenario())


def test_directed_matrix_rejects_self_links_and_non_majority_shapes() -> None:
    async def scenario() -> None:
        names = ["a", "b", "m"]
        proxies = [SimpleNamespace(identity_member=name) for name in names]
        expected = {
            (source, destination): True
            for source in names
            for destination in names
        }
        with pytest.raises(RuntimeError, match="six non-self edges"):
            await etcd_peer_partition.verify_directed_peer_matrix(
                proxies, names, Path("."), {}, expected, 1
            )

        crossing_links_allowed = {
            (source, destination): (
                (source != "m" and destination != "m")
                or (source == "m" and destination == "a")
            )
            for source in names
            for destination in names
            if source != destination
        }
        with pytest.raises(RuntimeError, match="two-edge majority partition"):
            await etcd_peer_partition.verify_directed_peer_matrix(
                proxies, names, Path("."), {}, crossing_links_allowed, 1
            )

    asyncio.run(scenario())
