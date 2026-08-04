"""Pure PostgreSQL catalog-row normalization for canonical discovery models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from models.discovery import (
    CheckConstraintMetadata,
    ConstraintType,
    DatabaseObjectMetadata,
    DatabaseObjectType,
    DiscoveryCoverage,
    ForeignKeyMetadata,
    KeyConstraintMetadata,
    ObjectPersistence,
    SchemaMetadata,
    TableMetadata,
)
from models.metadata import ColumnMetadata
from normalizers._common import optional_bool, optional_int, optional_text, value_for
from normalizers._discovery_common import (
    aggregate_foreign_key_rows,
    aggregate_key_rows,
    deduplicate_and_sort_columns,
    deduplicate_models,
    default_coverage,
    optional_name_tuple,
    required_text,
    safe_vendor_metadata,
)
from normalizers.postgresql import normalize_postgresql_column


def _object_type(value: Any) -> DatabaseObjectType:
    normalized = str(value).strip().casefold() if value is not None else ""
    return {
        "r": DatabaseObjectType.TABLE,
        "table": DatabaseObjectType.TABLE,
        "base table": DatabaseObjectType.TABLE,
        "p": DatabaseObjectType.PARTITIONED_TABLE,
        "partitioned table": DatabaseObjectType.PARTITIONED_TABLE,
        "v": DatabaseObjectType.VIEW,
        "view": DatabaseObjectType.VIEW,
        "m": DatabaseObjectType.MATERIALIZED_VIEW,
        "materialized view": DatabaseObjectType.MATERIALIZED_VIEW,
        "f": DatabaseObjectType.FOREIGN_TABLE,
        "foreign table": DatabaseObjectType.FOREIGN_TABLE,
        "external table": DatabaseObjectType.EXTERNAL_TABLE,
    }.get(normalized, DatabaseObjectType.UNKNOWN)


def _persistence(value: Any) -> ObjectPersistence:
    normalized = str(value).strip().casefold() if value is not None else ""
    return {
        "p": ObjectPersistence.PERMANENT,
        "permanent": ObjectPersistence.PERMANENT,
        "u": ObjectPersistence.UNLOGGED,
        "unlogged": ObjectPersistence.UNLOGGED,
        "t": ObjectPersistence.TEMPORARY,
        "temporary": ObjectPersistence.TEMPORARY,
        "temp": ObjectPersistence.TEMPORARY,
    }.get(normalized, ObjectPersistence.UNKNOWN)


def _constraint_type(value: Any, default: ConstraintType | None = None) -> ConstraintType:
    normalized = str(value).strip().casefold() if value is not None else ""
    if normalized in {"p", "primary key", "primary_key", "primary"}:
        return ConstraintType.PRIMARY_KEY
    if normalized in {"u", "unique", "unique key", "unique_key"}:
        return ConstraintType.UNIQUE
    if default is not None:
        return default
    raise ValueError("constraint_type must identify a primary key or unique constraint.")


def normalize_postgresql_schema(row: Mapping[str, Any]) -> SchemaMetadata:
    return SchemaMetadata(
        catalog_name=optional_text(value_for(row, "catalog_name", "table_catalog", "database_name")),
        schema_name=required_text(row, "schema_name", "table_schema", "nspname"),
        system="postgresql",
        owner=optional_text(value_for(row, "owner", "schema_owner", "rolname")),
        comment=optional_text(value_for(row, "comment", "schema_comment")),
        is_system_managed=optional_bool(value_for(row, "is_system_managed")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_postgresql_object(row: Mapping[str, Any]) -> DatabaseObjectMetadata:
    return DatabaseObjectMetadata(
        catalog_name=optional_text(value_for(row, "catalog_name", "table_catalog", "database_name")),
        schema_name=required_text(row, "schema_name", "table_schema", "nspname"),
        object_name=required_text(row, "object_name", "table_name", "relname"),
        system="postgresql",
        object_type=_object_type(value_for(row, "object_type", "table_type", "relkind")),
        persistence=_persistence(value_for(row, "persistence", "relpersistence")),
        owner=optional_text(value_for(row, "owner", "table_owner", "rolname")),
        comment=optional_text(value_for(row, "comment", "table_comment", "object_comment")),
        estimated_row_count=optional_int(value_for(row, "estimated_row_count", "row_count", "reltuples")),
        is_system_managed=optional_bool(value_for(row, "is_system_managed")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_postgresql_key_constraint(
    row: Mapping[str, Any], *, constraint_type: ConstraintType | None = None
) -> KeyConstraintMetadata:
    return KeyConstraintMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        constraint_type=_constraint_type(value_for(row, "constraint_type", "contype"), constraint_type),
        columns=optional_name_tuple(row, "columns", "column_names", "local_columns"),
        is_enforced=optional_bool(value_for(row, "is_enforced", "conenforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "convalidated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        is_deferrable=optional_bool(value_for(row, "is_deferrable", "condeferrable")),
        initially_deferred=optional_bool(value_for(row, "initially_deferred", "condeferred")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_postgresql_foreign_key(row: Mapping[str, Any]) -> ForeignKeyMetadata:
    return ForeignKeyMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        local_columns=optional_name_tuple(row, "local_columns", "columns", "column_names"),
        referenced_catalog=optional_text(value_for(row, "referenced_catalog", "foreign_table_catalog")),
        referenced_schema=optional_text(value_for(row, "referenced_schema", "foreign_table_schema")),
        referenced_table=required_text(row, "referenced_table", "foreign_table_name", "confrelname"),
        referenced_columns=optional_name_tuple(row, "referenced_columns", "foreign_column_names"),
        match_option=optional_text(value_for(row, "match_option", "confmatchtype")),
        update_rule=optional_text(value_for(row, "update_rule", "confupdtype")),
        delete_rule=optional_text(value_for(row, "delete_rule", "confdeltype")),
        is_enforced=optional_bool(value_for(row, "is_enforced", "conenforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "convalidated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        is_deferrable=optional_bool(value_for(row, "is_deferrable", "condeferrable")),
        initially_deferred=optional_bool(value_for(row, "initially_deferred", "condeferred")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_postgresql_check_constraint(row: Mapping[str, Any]) -> CheckConstraintMetadata:
    return CheckConstraintMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        expression=required_text(row, "expression", "check_clause", "constraint_definition"),
        is_enforced=optional_bool(value_for(row, "is_enforced", "conenforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "convalidated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def _normalized_columns(object_metadata: DatabaseObjectMetadata, rows: Iterable[Mapping[str, Any]]) -> tuple[ColumnMetadata, ...]:
    return deduplicate_and_sort_columns(
        normalize_postgresql_column(
            row,
            catalog_name=object_metadata.catalog_name,
            schema_name=object_metadata.schema_name,
            table_name=object_metadata.object_name,
        )
        for row in rows
    )


def _sorted_unique_constraints(rows: Iterable[Mapping[str, Any]], constraint_type: ConstraintType) -> tuple[KeyConstraintMetadata, ...]:
    models = [normalize_postgresql_key_constraint(row, constraint_type=constraint_type) for row in aggregate_key_rows(rows, column_names=("columns", "column_names", "local_columns"))]
    deduplicated = deduplicate_models(models, lambda item: (item.constraint_type, item.name, item.columns))
    return tuple(sorted(deduplicated, key=lambda item: (item.name is None, item.name or "", item.columns)))


def _sorted_foreign_keys(rows: Iterable[Mapping[str, Any]]) -> tuple[ForeignKeyMetadata, ...]:
    models = [normalize_postgresql_foreign_key(row) for row in aggregate_foreign_key_rows(rows)]
    deduplicated = deduplicate_models(
        models,
        lambda item: (
            item.name,
            item.local_columns,
            item.referenced_catalog,
            item.referenced_schema,
            item.referenced_table,
            item.referenced_columns,
        ),
    )
    return tuple(sorted(deduplicated, key=lambda item: (item.name is None, item.name or "", item.local_columns, item.referenced_columns)))


def _sorted_checks(rows: Iterable[Mapping[str, Any]]) -> tuple[CheckConstraintMetadata, ...]:
    models = [normalize_postgresql_check_constraint(row) for row in rows]
    deduplicated = deduplicate_models(models, lambda item: (item.name, item.expression))
    return tuple(sorted(deduplicated, key=lambda item: (item.name is None, item.name or "", item.expression)))


def normalize_postgresql_table(
    object_row: Mapping[str, Any],
    *,
    column_rows: Iterable[Mapping[str, Any]],
    primary_key_rows: Iterable[Mapping[str, Any]] | None = None,
    unique_constraint_rows: Iterable[Mapping[str, Any]] | None = None,
    foreign_key_rows: Iterable[Mapping[str, Any]] | None = None,
    check_constraint_rows: Iterable[Mapping[str, Any]] | None = None,
    coverage: DiscoveryCoverage | None = None,
) -> TableMetadata:
    """Construct one PostgreSQL table/view model from already-fetched catalog rows."""

    object_metadata = normalize_postgresql_object(object_row)
    primary_keys = _sorted_unique_constraints(primary_key_rows or (), ConstraintType.PRIMARY_KEY)
    return TableMetadata(
        catalog_name=object_metadata.catalog_name,
        schema_name=object_metadata.schema_name,
        object_name=object_metadata.object_name,
        system=object_metadata.system,
        object_type=object_metadata.object_type,
        persistence=object_metadata.persistence,
        owner=object_metadata.owner,
        comment=object_metadata.comment,
        estimated_row_count=object_metadata.estimated_row_count,
        is_system_managed=object_metadata.is_system_managed,
        columns=_normalized_columns(object_metadata, column_rows),
        primary_key=primary_keys[0] if primary_keys else None,
        unique_constraints=_sorted_unique_constraints(unique_constraint_rows or (), ConstraintType.UNIQUE),
        foreign_keys=_sorted_foreign_keys(foreign_key_rows or ()),
        check_constraints=_sorted_checks(check_constraint_rows or ()),
        view_definition=optional_text(value_for(object_row, "view_definition")),
        clustering_expression=None,
        is_partitioned=optional_bool(value_for(object_row, "is_partitioned", "relispartition")),
        partitioning_expression=optional_text(value_for(object_row, "partitioning_expression", "partition_key_definition")),
        coverage=coverage
        or default_coverage(
            object_is_view=object_metadata.object_type in {DatabaseObjectType.VIEW, DatabaseObjectType.MATERIALIZED_VIEW},
            primary_key_rows=primary_key_rows,
            unique_constraint_rows=unique_constraint_rows,
            foreign_key_rows=foreign_key_rows,
            check_constraint_rows=check_constraint_rows,
            object_row=object_row,
        ),
        vendor_metadata=safe_vendor_metadata(object_row),
    )


__all__ = [
    "normalize_postgresql_check_constraint",
    "normalize_postgresql_foreign_key",
    "normalize_postgresql_key_constraint",
    "normalize_postgresql_object",
    "normalize_postgresql_schema",
    "normalize_postgresql_table",
]
