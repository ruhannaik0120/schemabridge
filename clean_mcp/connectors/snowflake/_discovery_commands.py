"""Narrow, safely serialized Snowflake discovery commands."""

from __future__ import annotations

from typing import Any


_SHOW_PRIMARY_KEYS_PREFIX = "SHOW PRIMARY KEYS IN TABLE "


def _quote_identifier_component(value: Any) -> str:
    """Quote one exact Snowflake identifier component."""
    if isinstance(value, bool) or not isinstance(value, str) or value == "" or "\x00" in value:
        raise ValueError("Invalid Snowflake discovery identifier.")
    return '"' + value.replace('"', '""') + '"'


def _show_primary_keys_command(database: Any, schema: Any, table: Any) -> str:
    """Build the sole approved non-SELECT canonical discovery statement."""
    qualified_name = ".".join(
        _quote_identifier_component(component)
        for component in (database, schema, table)
    )
    return _SHOW_PRIMARY_KEYS_PREFIX + qualified_name


__all__ = ["_quote_identifier_component", "_show_primary_keys_command"]
