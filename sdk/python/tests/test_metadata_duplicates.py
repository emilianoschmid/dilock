import pytest

from dilock._metadata import parse_unique_top_level_object


@pytest.mark.parametrize("raw", [
    b'{"protocol_major":1,"protocol_major":2}',
    b'{"coordination_epoch":"wrong","coordination_epoch":"right"}',
    b'{"protocol_major":1,"protocol\\u005fmajor":1}',
])
def test_metadata_rejects_duplicate_top_level_fields(raw: bytes) -> None:
    with pytest.raises(ValueError):
        parse_unique_top_level_object(raw)


def test_metadata_preserves_opaque_nested_extensions() -> None:
    parsed = parse_unique_top_level_object(
        b'{"protocol_major":1,"extension":{"key":1,"key":2}}'
    )
    assert parsed["protocol_major"] == 1
