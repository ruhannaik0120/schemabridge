"""Snowflake column metadata normalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from models.metadata import CanonicalType, ColumnMetadata
from normalizers._common import normalized_native_type, optional_bool, optional_int, optional_nullable, optional_text, value_for


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


def _canonical_type_value(value: Any, fallback_native_type: str | None) -> CanonicalType | None:
    if isinstance(value, CanonicalType):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in CanonicalType.__members__:
            return CanonicalType[normalized]
    return _canonical_type(fallback_native_type, None) if fallback_native_type is not None else None


def _generated_marker(value: Any) -> bool | None:
    marker = optional_bool(value)
    if marker is not None:
        return marker
    if isinstance(value, str):
        if value.strip().casefold() in {"virtual", "virtual_column"}:
            return True
        if value.strip().casefold() in {"column", ""}:
            return False
    return None


def normalize_snowflake_column(
    row: Mapping[str, Any],
    *,
    catalog_name: str | None,
    schema_name: str | None,
    table_name: str,
    is_primary_key: bool | None = None,
    is_foreign_key: bool | None = None,
    is_unique_key: bool | None = None,
) -> ColumnMetadata:
    """Convert one Snowflake describe-table row into canonical metadata."""

    native_type = optional_text(value_for(row, "data_type", "native_type"))
    numeric_scale = optional_int(value_for(row, "numeric_scale"))
    element_native_type = optional_text(value_for(row, "element_native_type", "array_element_type"))
    explicit_vendor_metadata = value_for(row, "vendor_metadata")
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
        is_primary_key=is_primary_key if is_primary_key is not None else optional_bool(value_for(row, "is_primary_key", "primary_key")),
        is_foreign_key=is_foreign_key if is_foreign_key is not None else optional_bool(value_for(row, "is_foreign_key", "foreign_key")),
        vendor_metadata=(explicit_vendor_metadata if isinstance(explicit_vendor_metadata, Mapping) else row),
        default_expression=optional_text(value_for(row, "column_default", "default_expression", "default")),
        comment=optional_text(value_for(row, "column_comment", "comment")),
        collation=optional_text(value_for(row, "collation_name", "collation")),
        is_identity=optional_bool(value_for(row, "is_identity", "identity", "is_autoincrement")),
        identity_generation=optional_text(value_for(row, "identity_generation")),
        is_auto_increment=optional_bool(value_for(row, "is_auto_increment", "auto_increment", "is_autoincrement")),
        is_generated=_generated_marker(value_for(row, "is_generated", "generated", "kind")),
        generation_expression=optional_text(value_for(row, "generation_expression", "expression")),
        is_unique_key=is_unique_key if is_unique_key is not None else optional_bool(value_for(row, "is_unique_key", "unique_key")),
        array_dimensions=optional_int(value_for(row, "array_dimensions")),
        element_native_type=element_native_type,
        element_canonical_type=_canonical_type_value(
            value_for(row, "element_canonical_type", "array_element_canonical_type"),
            element_native_type,
        ),
    )
