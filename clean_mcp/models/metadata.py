"""Vendor-neutral metadata models used by SchemaBridge."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any


class CanonicalType(str, Enum):
    """Database-independent column type families."""

    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    BINARY = "BINARY"
    SEMI_STRUCTURED = "SEMI_STRUCTURED"
    UNKNOWN = "UNKNOWN"


def _freeze(value: Any) -> Any:
    """Recursively isolate mutable vendor metadata from the frozen model."""

    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _json_value(value: Any) -> Any:
    """Convert frozen metadata and driver scalars into JSON-safe values."""

    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_json_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


@dataclass(frozen=True, slots=True)
class ColumnMetadata:
    """Immutable representation of one database column."""

    catalog_name: str | None
    schema_name: str | None
    table_name: str
    column_name: str
    ordinal_position: int | None
    native_type: str | None
    canonical_type: CanonicalType
    nullable: bool | None
    character_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    datetime_precision: int | None
    is_primary_key: bool | None = None
    is_foreign_key: bool | None = None
    vendor_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate identity and freeze a detached copy of vendor metadata."""

        if not isinstance(self.table_name, str) or not self.table_name.strip():
            raise ValueError("table_name must be a non-empty string.")
        if not isinstance(self.column_name, str) or not self.column_name.strip():
            raise ValueError("column_name must be a non-empty string.")
        if self.catalog_name is not None and not isinstance(self.catalog_name, str):
            raise TypeError("catalog_name must be a string or None.")
        if self.schema_name is not None and not isinstance(self.schema_name, str):
            raise TypeError("schema_name must be a string or None.")
        if not isinstance(self.vendor_metadata, Mapping):
            raise TypeError("vendor_metadata must be a mapping.")
        object.__setattr__(self, "vendor_metadata", _freeze(dict(self.vendor_metadata)))

    def to_dict(self) -> dict[str, Any]:
        """Return a normal dictionary that can be passed directly to json.dumps."""

        return {
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "ordinal_position": self.ordinal_position,
            "native_type": self.native_type,
            "canonical_type": self.canonical_type.value,
            "nullable": self.nullable,
            "character_length": self.character_length,
            "numeric_precision": self.numeric_precision,
            "numeric_scale": self.numeric_scale,
            "datetime_precision": self.datetime_precision,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "vendor_metadata": _json_value(self.vendor_metadata),
        }
