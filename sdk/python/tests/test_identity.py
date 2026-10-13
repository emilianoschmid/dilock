import hashlib
import json
from pathlib import Path

import pytest

from dilock import AcquireOptions, Options, identity

ROOT = Path(__file__).resolve().parents[3]
VECTORS = json.loads((ROOT / "spec/vectors/identity.json").read_text())


@pytest.mark.parametrize("vector", VECTORS["valid"])
def test_valid_identity(vector):
    assert identity(vector["namespace"], vector["resource"]) == vector["sha256"]


@pytest.mark.parametrize("vector", VECTORS["invalid"])
def test_invalid_identity(vector):
    with pytest.raises(ValueError):
        identity(vector["namespace"], vector["resource"])


@pytest.mark.parametrize("resource", ["\U00040000", "\U000fffff"])
def test_four_byte_utf8_scalars_with_f1_to_f3_leads_are_accepted(resource):
    assert len(identity("default", resource)) == 64


def test_lone_surrogate_is_rejected_instead_of_replacement_encoded():
    with pytest.raises(ValueError):
        identity("default", "\ud800")


@pytest.mark.parametrize("name", ["redis-release.lua", "redis-renew.lua"])
def test_canonical_scripts(name):
    assert hashlib.sha256((ROOT / "spec/backends" / name).read_bytes()).digest() == hashlib.sha256(
        (ROOT / "sdk/python/src/dilock" / name).read_bytes()).digest()


@pytest.mark.parametrize("args", [dict(lease=0), dict(rpc=0), dict(cleanup=0),
                                  dict(lease=float("nan")), dict(lease=86401),
                                  dict(lease=.1, rpc=.2)])
def test_bad_budgets(args):
    with pytest.raises(ValueError):
        Options(**args)


def test_default_acquisition_policy_is_immutable_and_finite():
    policy = AcquireOptions()
    assert policy.wait == 5.0
    assert policy.lease.lease == 30.0
    assert policy.lease.rpc == 2.0
    assert policy.lease.cleanup == 2.0
    with pytest.raises(ValueError):
        AcquireOptions(wait=float("inf"))
