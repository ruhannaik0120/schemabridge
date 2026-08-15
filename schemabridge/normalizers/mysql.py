"""MySQL column metadata normalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from schemabridge.models.metadata import CanonicalType, ColumnMetadata
from schemabridge.normalizers._common import (
    normalized_native_type,
    optional_int,
    optional_nullable,
    optional_text,
    value_for,
)


def _canonical_type(native_type: str | None, numeric_scale: int | None) -> CanonicalType:
    normalized = normalized_native_type(native_type)
    if normalized in {
        "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set"
    }:
        return CanonicalType.STRING
    if normalized in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year"}:
        return CanonicalType.INTEGER
    if normalized in {"decimal", "numeric"}:
        return CanonicalType.INTEGER if numeric_scale == 0 else CanonicalType.DECIMAL
    if normalized in {"float", "double", "real"}:
        return CanonicalType.FLOAT
    if normalized in {"bool", "boolean"}:
        return CanonicalType.BOOLEAN
    if normalized == "date":
        return CanonicalType.DATE
    if normalized == "time":
        return CanonicalType.TIME
    if normalized in {"datetime", "timestamp"}:
        return CanonicalType.TIMESTAMP
    if normalized in {"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit"}:
        return CanonicalType.BINARY
    if normalized == "json":
        return CanonicalType.SEMI_STRUCTURED
    return CanonicalType.UNKNOWN


def normalize_mysql_column(
    row: Mapping[str, Any],
    *,
    catalog_name: str | None,
    schema_name: str,
    table_name: str,
    is_primary_key: bool | None = None,
    is_unique_key: bool | None = None,
) -> ColumnMetadata:
    """Convert one INFORMATION_SCHEMA column row into canonical metadata."""

    native_type = optional_text(value_for(row, "data_type", "native_type"))
    numeric_scale = optional_int(value_for(row, "numeric_scale"))
    extra = optional_text(value_for(row, "extra")) or ""
    return ColumnMetadata(
        catalog_name=optional_text(catalog_name),
        schema_name=optional_text(schema_name),
        table_name=table_name,
        column_name=optional_text(value_for(row, "column_name")) or "",
        ordinal_position=optional_int(value_for(row, "ordinal_position")),
        native_type=native_type,
        canonical_type=_canonical_type(native_type, numeric_scale),
        nullable=optional_nullable(value_for(row, "is_nullable", "nullable")),
        character_length=optional_int(value_for(row, "character_maximum_length")),
        numeric_precision=optional_int(value_for(row, "numeric_precision")),
        numeric_scale=numeric_scale,
        datetime_precision=optional_int(value_for(row, "datetime_precision")),
        is_primary_key=is_primary_key,
        is_foreign_key=None,
        is_unique_key=is_unique_key,
        default_expression=optional_text(value_for(row, "column_default")),
        comment=optional_text(value_for(row, "column_comment")),
        collation=optional_text(value_for(row, "collation_name")),
        is_identity=None,
        identity_generation=None,
        is_auto_increment="auto_increment" in extra.casefold(),
        is_generated="generated" in extra.casefold(),
        generation_expression=optional_text(value_for(row, "generation_expression")),
        vendor_metadata=row,
    )


__all__ = ["normalize_mysql_column"]
