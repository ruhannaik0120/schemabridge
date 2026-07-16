"""Snowflake column metadata normalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from models.metadata import CanonicalType, ColumnMetadata
from normalizers._common import normalized_native_type, optional_int, optional_nullable, optional_text, value_for


def _canonical_type(native_type: str | None, numeric_scale: int | None) -> CanonicalType:
    normalized = normalized_native_type(native_type)
    if normalized in {"varchar", "char", "character", "string", "text"}:
        return CanonicalType.STRING
    if normalized in {"integer", "int", "bigint", "smallint", "tinyint", "byteint"}:
        return CanonicalType.INTEGER
    if normalized in {"number", "numeric", "decimal", "dec", "fixed"}:
        return CanonicalType.INTEGER if numeric_scale == 0 else CanonicalType.DECIMAL
    if normalized in {"float", "double", "double precision", "real", "float4", "float8"}:
        return CanonicalType.FLOAT
    if normalized == "boolean":
        return CanonicalType.BOOLEAN
    if normalized == "date":
        return CanonicalType.DATE
    if normalized == "time":
        return CanonicalType.TIME
    if normalized in {"timestamp", "timestamp_ntz", "datetime"}:
        return CanonicalType.TIMESTAMP
    if normalized in {"timestamp_tz", "timestamp_ltz"}:
        return CanonicalType.TIMESTAMP_TZ
    if normalized in {"binary", "varbinary"}:
        return CanonicalType.BINARY
    if normalized in {"variant", "object", "array"}:
        return CanonicalType.SEMI_STRUCTURED
    return CanonicalType.UNKNOWN


def normalize_snowflake_column(
    row: Mapping[str, Any],
    *,
    catalog_name: str | None,
    schema_name: str | None,
    table_name: str,
    is_primary_key: bool | None = None,
    is_foreign_key: bool | None = None,
) -> ColumnMetadata:
    """Convert one Snowflake describe-table row into canonical metadata."""

    native_type = optional_text(value_for(row, "data_type", "native_type"))
    numeric_scale = optional_int(value_for(row, "numeric_scale"))
    return ColumnMetadata(
        catalog_name=optional_text(catalog_name),
        schema_name=optional_text(schema_name),
        table_name=table_name,
        column_name=optional_text(value_for(row, "column_name")) or "",
        ordinal_position=optional_int(value_for(row, "ordinal_position")),
        native_type=native_type,
        canonical_type=_canonical_type(native_type, numeric_scale),
        nullable=optional_nullable(value_for(row, "is_nullable", "nullable")),
        character_length=optional_int(value_for(row, "character_maximum_length", "character_length")),
        numeric_precision=optional_int(value_for(row, "numeric_precision")),
        numeric_scale=numeric_scale,
        datetime_precision=optional_int(value_for(row, "datetime_precision")),
        is_primary_key=is_primary_key,
        is_foreign_key=is_foreign_key,
        vendor_metadata=row,
    )
