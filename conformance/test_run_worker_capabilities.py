"""Tests for the worker capability encodings accepted by the pair runner."""
from conformance.run import (
    _PopenAsyncWriter,
    capability_enabled,
    expired_stale_release_status_allowed,
)
import pytest


def test_reads_boolean_capability_map():
    assert capability_enabled({"auto_renew_mandatory": True}, "auto_renew_mandatory")
    assert not capability_enabled({"auto_renew_mandatory": False}, "auto_renew_mandatory")
    assert not capability_enabled({"auto_renew_mandatory": 1}, "auto_renew_mandatory")


def test_reads_legacy_capability_string_list():
    assert capability_enabled(["etcd", "auto_renew_mandatory"], "auto_renew_mandatory")
    assert not capability_enabled(["etcd", "renewal"], "auto_renew_mandatory")


def test_ignores_malformed_capability_values():
    assert not capability_enabled(None, "auto_renew_mandatory")
    assert not capability_enabled("auto_renew_mandatory", "auto_renew_mandatory")


def test_expired_stale_release_preserves_unknown_etcd_outcome():
    assert expired_stale_release_status_allowed("etcd", "release_unknown")
    assert expired_stale_release_status_allowed("etcd", "already_not_owned")
    assert expired_stale_release_status_allowed("etcd", "released")
    assert not expired_stale_release_status_allowed("redis", "release_unknown")
    assert not expired_stale_release_status_allowed("etcd", "backend_unavailable")


def test_windows_worker_pipe_close_preserves_early_exit_diagnostics():
    class InvalidatedPipe:
        def close(self):
            raise OSError(22, "Invalid argument")

    writer = _PopenAsyncWriter(InvalidatedPipe(), process_exited=lambda: True)
    writer.close()

    assert writer.is_closing()
    writer.close()


def test_windows_worker_pipe_close_does_not_hide_live_pipe_error():
    class InvalidPipe:
        def close(self):
            raise OSError(22, "Invalid argument")

    writer = _PopenAsyncWriter(InvalidPipe(), process_exited=lambda: False)

    with pytest.raises(OSError, match="Invalid argument"):
        writer.close()
