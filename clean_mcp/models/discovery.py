"""Immutable canonical models for cross-database schema discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from models.metadata import (
    ColumnMetadata,
    _MetadataModel,
    _freeze,
    _json_value,
    _sanitize_vendor_metadata,
    _validate_identifier,
    _validate_non_negative,
)

class DatabaseObjectType(str, Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    EXTERNAL_TABLE = "EXTERNAL_TABLE"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    FOREIGN_TABLE = "FOREIGN_TABLE"
    PARTITIONED_TABLE = "PARTITIONED_TABLE"
    UNKNOWN = "UNKNOWN"


class ObjectPersistence(str, Enum):
    PERMANENT = "PERMANENT"
    TRANSIENT = "TRANSIENT"
    TEMPORARY = "TEMPORARY"
    UNLOGGED = "UNLOGGED"
    UNKNOWN = "UNKNOWN"


class ConstraintType(str, Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    UNIQUE = "UNIQUE"


class CoverageStatus(str, Enum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


def _freeze_vendor_metadata(model: object, value: Mapping[str, Any]) -> None:
    if not isinstance(value, Mapping):
        raise TypeError("vendor_metadata must be a mapping.")
    object.__setattr__(model, "vendor_metadata", _freeze(_sanitize_vendor_metadata(value)))


def _validate_optional_text(value: str | None, field_name: str) -> None:
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string or None.")


def _validated_columns(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple.")
    for value in values:
        _validate_identifier(value, field_name, required=True)
    return values


@dataclass(frozen=True, slots=True, kw_only=True)
class SchemaMetadata(_MetadataModel):
    catalog_name: str | None
    schema_name: str
    system: str
    owner: str | None = None
    comment: str | None = None
    is_system_managed: bool | None = None
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _validate_identifier(self.catalog_name, "catalog_name", required=False)
        _validate_identifier(self.schema_name, "schema_name", required=True)
        _validate_identifier(self.system, "system", required=True)
        _validate_optional_text(self.owner, "owner")
        _validate_optional_text(self.comment, "comment")
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class DatabaseObjectMetadata(_MetadataModel):
    catalog_name: str | None
    schema_name: str
    object_name: str
    system: str
    object_type: DatabaseObjectType
    persistence: ObjectPersistence
    owner: str | None = None
    comment: str | None = None
    estimated_row_count: int | None = None
    is_system_managed: bool | None = None
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _validate_identifier(self.catalog_name, "catalog_name", required=False)
        _validate_identifier(self.schema_name, "schema_name", required=True)
        _validate_identifier(self.object_name, "object_name", required=True)
        _validate_identifier(self.system, "system", required=True)
        _validate_optional_text(self.owner, "owner")
        _validate_optional_text(self.comment, "comment")
        if not isinstance(self.object_type, DatabaseObjectType):
            raise TypeError("object_type must be a DatabaseObjectType.")
        if not isinstance(self.persistence, ObjectPersistence):
            raise TypeError("persistence must be an ObjectPersistence.")
        _validate_non_negative(self.estimated_row_count, "estimated_row_count")
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyConstraintMetadata(_MetadataModel):
    name: str | None
    constraint_type: ConstraintType
    columns: tuple[str, ...]
    is_enforced: bool | None = None
    is_validated: bool | None = None
    is_rely: bool | None = None
    is_deferrable: bool | None = None
    initially_deferred: bool | None = None
    comment: str | None = None
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "name", required=False)
        if not isinstance(self.constraint_type, ConstraintType):
            raise TypeError("constraint_type must be a ConstraintType.")
        _validated_columns(self.columns, "columns")
        _validate_optional_text(self.comment, "comment")
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class ForeignKeyMetadata(_MetadataModel):
    name: str | None
    local_columns: tuple[str, ...]
    referenced_catalog: str | None
    referenced_schema: str | None
    referenced_table: str
    referenced_columns: tuple[str, ...]
    match_option: str | None = None
    update_rule: str | None = None
    delete_rule: str | None = None
    is_enforced: bool | None = None
    is_validated: bool | None = None
    is_rely: bool | None = None
    is_deferrable: bool | None = None
    initially_deferred: bool | None = None
    comment: str | None = None
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "name", required=False)
        _validated_columns(self.local_columns, "local_columns")
        _validate_identifier(self.referenced_catalog, "referenced_catalog", required=False)
        _validate_identifier(self.referenced_schema, "referenced_schema", required=False)
        _validate_identifier(self.referenced_table, "referenced_table", required=True)
        _validated_columns(self.referenced_columns, "referenced_columns")
        if self.local_columns and self.referenced_columns and len(self.local_columns) != len(self.referenced_columns):
            raise ValueError("Foreign-key local and referenced column counts must match.")
        for field_name in ("match_option", "update_rule", "delete_rule", "comment"):
            _validate_optional_text(getattr(self, field_name), field_name)
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckConstraintMetadata(_MetadataModel):
    name: str | None
    expression: str
    is_enforced: bool | None = None
    is_validated: bool | None = None
    is_rely: bool | None = None
    comment: str | None = None
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "name", required=False)
        if not isinstance(self.expression, str) or not self.expression.strip():
            raise ValueError("expression must be a non-empty string.")
        _validate_optional_text(self.comment, "comment")
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscoveryCoverage(_MetadataModel):
    columns: CoverageStatus
    primary_key: CoverageStatus
    unique_constraints: CoverageStatus
    foreign_keys: CoverageStatus
    check_constraints: CoverageStatus
    comments: CoverageStatus
    estimated_row_count: CoverageStatus
    view_definition: CoverageStatus
    partitioning: CoverageStatus
    clustering: CoverageStatus
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            if name != "warnings" and not isinstance(getattr(self, name), CoverageStatus):
                raise TypeError(f"{name} must be a CoverageStatus.")
        if not isinstance(self.warnings, tuple):
            raise TypeError("warnings must be a tuple.")
        for warning in self.warnings:
            if not isinstance(warning, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", warning) is None:
                raise ValueError("warnings must contain fixed safe warning codes.")

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True, slots=True, kw_only=True)
class TableMetadata(_MetadataModel):
    catalog_name: str | None
    schema_name: str
    object_name: str
    system: str
    object_type: DatabaseObjectType
    persistence: ObjectPersistence
    owner: str | None = None
    comment: str | None = None
    estimated_row_count: int | None = None
    is_system_managed: bool | None = None
    columns: tuple[ColumnMetadata, ...]
    primary_key: KeyConstraintMetadata | None = None
    unique_constraints: tuple[KeyConstraintMetadata, ...] = ()
    foreign_keys: tuple[ForeignKeyMetadata, ...] = ()
    check_constraints: tuple[CheckConstraintMetadata, ...] = ()
    view_definition: str | None = None
    clustering_expression: str | None = None
    is_partitioned: bool | None = None
    partitioning_expression: str | None = None
    coverage: DiscoveryCoverage
    vendor_metadata: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        DatabaseObjectMetadata(
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
            object_name=self.object_name,
            system=self.system,
            object_type=self.object_type,
            persistence=self.persistence,
            owner=self.owner,
            comment=self.comment,
            estimated_row_count=self.estimated_row_count,
            is_system_managed=self.is_system_managed,
            vendor_metadata={},
        )
        for field_name, values, expected_type in (
            ("columns", self.columns, ColumnMetadata),
            ("unique_constraints", self.unique_constraints, KeyConstraintMetadata),
            ("foreign_keys", self.foreign_keys, ForeignKeyMetadata),
            ("check_constraints", self.check_constraints, CheckConstraintMetadata),
        ):
            if not isinstance(values, tuple) or not all(isinstance(value, expected_type) for value in values):
                raise TypeError(f"{field_name} must be a tuple of {expected_type.__name__} values.")
        if self.primary_key is not None:
            if not isinstance(self.primary_key, KeyConstraintMetadata):
                raise TypeError("primary_key must be KeyConstraintMetadata or None.")
            if self.primary_key.constraint_type is not ConstraintType.PRIMARY_KEY:
                raise ValueError("primary_key must use the PRIMARY_KEY constraint type.")
        if any(item.constraint_type is not ConstraintType.UNIQUE for item in self.unique_constraints):
            raise ValueError("unique_constraints must use the UNIQUE constraint type.")
        if not isinstance(self.coverage, DiscoveryCoverage):
            raise TypeError("coverage must be DiscoveryCoverage.")
        for name in ("view_definition", "clustering_expression", "partitioning_expression"):
            _validate_optional_text(getattr(self, name), name)
        _freeze_vendor_metadata(self, self.vendor_metadata)

    def to_dict(self) -> dict[str, Any]:
        return _json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


__all__ = [
    "CheckConstraintMetadata",
    "ConstraintType",
    "CoverageStatus",
    "DatabaseObjectMetadata",
    "DatabaseObjectType",
    "DiscoveryCoverage",
    "ForeignKeyMetadata",
    "KeyConstraintMetadata",
    "ObjectPersistence",
    "SchemaMetadata",
    "TableMetadata",
]
