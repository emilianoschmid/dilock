"""Strict helpers for shared namespace metadata parsing."""
from __future__ import annotations

import json
from typing import Any


class _ObjectPairs(list[tuple[str, Any]]):
    """Preserve object pairs so duplicate top-level names remain observable."""


def _preserve_object_pairs(pairs: list[tuple[str, Any]]) -> _ObjectPairs:
    return _ObjectPairs(pairs)


def parse_unique_top_level_object(raw: str | bytes) -> dict[str, Any]:
    """Parse a JSON object and reject duplicate top-level property names.

    Unknown nested extension objects remain opaque and may contain repeated names;
    only the fields that define the namespace contract must be unambiguous.
    """
    parsed = json.loads(raw, object_pairs_hook=_preserve_object_pairs)
    if not isinstance(parsed, _ObjectPairs):
        raise ValueError("metadata root must be an object")
    result: dict[str, Any] = {}
    for key, value in parsed:
        if key in result:
            raise ValueError(f"duplicate metadata property: {key}")
        result[key] = value
    return result
