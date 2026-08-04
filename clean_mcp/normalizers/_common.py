"""Shared scalar normalization helpers for database metadata rows."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


def value_for(row: Mapping[str, Any], *names: str) -> Any:
    """Read a response field without changing the field's actual value."""

    normalized = {str(key).casefold(): value for key, value in row.items()}
    for name in names:
        if name.casefold() in normalized:
            return normalized[name.casefold()]
    return None


def optional_text(value: Any) -> str | None:
    """Preserve non-empty text exactly and normalize missing text to None."""

    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


def optional_int(value: Any) -> int | None:
    """Parse only complete integer values without rounding or partial parsing."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        return int(value.strip())
    return None


def optional_nullable(value: Any) -> bool | None:
    """Normalize information-schema YES/NO values into a tri-state boolean."""

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized == "yes":
            return True
        if normalized == "no":
            return False
    return None


def optional_bool(value: Any) -> bool | None:
    """Normalize common metadata boolean markers without inventing a value."""

    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"yes", "y", "true", "t", "1"}:
            return True
        if normalized in {"no", "n", "false", "f", "0"}:
            return False
    return None


def normalized_native_type(native_type: str | None) -> str:
    """Create a matching-only type name while retaining the original elsewhere."""

    if native_type is None:
        return ""
    normalized = re.sub(r"\s+", " ", native_type.strip().casefold())
    return re.sub(r"\s*\([^)]*\)", "", normalized).strip()
