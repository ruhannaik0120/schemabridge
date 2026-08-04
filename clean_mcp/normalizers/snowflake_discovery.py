"""Pure Snowflake catalog-row normalization for canonical discovery models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from models.discovery import (
    CheckConstraintMetadata,
    ConstraintType,
    CoverageStatus,
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
    foreign_key_coverage,
    key_constraint_coverage,
    optional_name_tuple,
    required_text,
    safe_vendor_metadata,
)
from normalizers.snowflake import normalize_snowflake_column


def _object_type(row: Mapping[str, Any]) -> DatabaseObjectType:
    if optional_bool(value_for(row, "is_dynamic")) is True:
        return DatabaseObjectType.DYNAMIC_TABLE
    normalized = str(value_for(row, "object_type", "table_type") or "").strip().casefold()
    return {
        "base table": DatabaseObjectType.TABLE,
        "table": DatabaseObjectType.TABLE,
        "view": DatabaseObjectType.VIEW,
        "materialized view": DatabaseObjectType.MATERIALIZED_VIEW,
        "external table": DatabaseObjectType.EXTERNAL_TABLE,
        "dynamic table": DatabaseObjectType.DYNAMIC_TABLE,
        "foreign table": DatabaseObjectType.FOREIGN_TABLE,
    }.get(normalized, DatabaseObjectType.UNKNOWN)


def _persistence(row: Mapping[str, Any]) -> ObjectPersistence:
    if optional_bool(value_for(row, "is_temporary")) is True:
        return ObjectPersistence.TEMPORARY
    if optional_bool(value_for(row, "is_transient")) is True:
        return ObjectPersistence.TRANSIENT
    normalized = str(value_for(row, "persistence", "table_kind", "kind") or "").strip().casefold()
    return {
        "permanent": ObjectPersistence.PERMANENT,
        "table": ObjectPersistence.PERMANENT,
        "transient": ObjectPersistence.TRANSIENT,
        "temporary": ObjectPersistence.TEMPORARY,
        "temp": ObjectPersistence.TEMPORARY,
    }.get(normalized, ObjectPersistence.UNKNOWN)


def _constraint_type(value: Any, default: ConstraintType | None = None) -> ConstraintType:
    normalized = str(value).strip().casefold() if value is not None else ""
    if normalized in {"primary key", "primary_key", "primary"}:
        return ConstraintType.PRIMARY_KEY
    if normalized in {"unique", "unique key", "unique_key"}:
        return ConstraintType.UNIQUE
    if default is not None:
        return default
    raise ValueError("constraint_type must identify a primary key or unique constraint.")


def normalize_snowflake_schema(row: Mapping[str, Any]) -> SchemaMetadata:
    return SchemaMetadata(
        catalog_name=optional_text(value_for(row, "catalog_name", "table_catalog", "database_name")),
        schema_name=required_text(row, "schema_name", "table_schema"),
        system="snowflake",
        owner=optional_text(value_for(row, "owner", "schema_owner")),
        comment=optional_text(value_for(row, "comment", "schema_comment")),
        is_system_managed=optional_bool(value_for(row, "is_system_managed")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_snowflake_object(row: Mapping[str, Any]) -> DatabaseObjectMetadata:
    return DatabaseObjectMetadata(
        catalog_name=optional_text(value_for(row, "catalog_name", "table_catalog", "database_name")),
        schema_name=required_text(row, "schema_name", "table_schema"),
        object_name=required_text(row, "object_name", "table_name"),
        system="snowflake",
        object_type=_object_type(row),
        persistence=_persistence(row),
        owner=optional_text(value_for(row, "owner", "table_owner")),
        comment=optional_text(value_for(row, "comment", "table_comment", "object_comment")),
        estimated_row_count=optional_int(value_for(row, "estimated_row_count", "row_count")),
        is_system_managed=optional_bool(value_for(row, "is_system_managed")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_snowflake_key_constraint(
    row: Mapping[str, Any], *, constraint_type: ConstraintType | None = None
) -> KeyConstraintMetadata:
    return KeyConstraintMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        constraint_type=_constraint_type(value_for(row, "constraint_type"), constraint_type),
        columns=optional_name_tuple(row, "columns", "column_names", "local_columns"),
        is_enforced=optional_bool(value_for(row, "is_enforced", "enforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "validated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        is_deferrable=optional_bool(value_for(row, "is_deferrable")),
        initially_deferred=optional_bool(value_for(row, "initially_deferred")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_snowflake_foreign_key(row: Mapping[str, Any]) -> ForeignKeyMetadata:
    local_columns = optional_name_tuple(row, "local_columns", "columns", "column_names")
    referenced_columns = optional_name_tuple(row, "referenced_columns", "foreign_column_names")
    if local_columns and referenced_columns and len(local_columns) != len(referenced_columns):
        referenced_columns = ()
    return ForeignKeyMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        local_columns=local_columns,
        referenced_catalog=optional_text(value_for(row, "referenced_catalog", "unique_constraint_catalog")),
        referenced_schema=optional_text(value_for(row, "referenced_schema", "unique_constraint_schema")),
        referenced_table=required_text(row, "referenced_table", "foreign_table_name"),
        referenced_columns=referenced_columns,
        match_option=optional_text(value_for(row, "match_option")),
        update_rule=optional_text(value_for(row, "update_rule")),
        delete_rule=optional_text(value_for(row, "delete_rule")),
        is_enforced=optional_bool(value_for(row, "is_enforced", "enforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "validated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        is_deferrable=optional_bool(value_for(row, "is_deferrable")),
        initially_deferred=optional_bool(value_for(row, "initially_deferred")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def normalize_snowflake_check_constraint(row: Mapping[str, Any]) -> CheckConstraintMetadata:
    return CheckConstraintMetadata(
        name=optional_text(value_for(row, "constraint_name", "name")),
        expression=required_text(row, "expression", "check_clause", "constraint_definition"),
        is_enforced=optional_bool(value_for(row, "is_enforced", "enforced")),
        is_validated=optional_bool(value_for(row, "is_validated", "validated")),
        is_rely=optional_bool(value_for(row, "is_rely", "rely")),
        comment=optional_text(value_for(row, "comment", "constraint_comment")),
        vendor_metadata=safe_vendor_metadata(row),
    )


def _normalized_columns(object_metadata: DatabaseObjectMetadata, rows: Iterable[Mapping[str, Any]]) -> tuple[ColumnMetadata, ...]:
    return deduplicate_and_sort_columns(
        normalize_snowflake_column(
            row,
            catalog_name=object_metadata.catalog_name,
            schema_name=object_metadata.schema_name,
            table_name=object_metadata.object_name,
        )
        for row in rows
    )


def _sorted_unique_constraints(rows: Iterable[Mapping[str, Any]], constraint_type: ConstraintType) -> tuple[KeyConstraintMetadata, ...]:
    models = [normalize_snowflake_key_constraint(row, constraint_type=constraint_type) for row in aggregate_key_rows(rows, column_names=("columns", "column_names", "local_columns"))]
    deduplicated = deduplicate_models(models, lambda item: (item.constraint_type, item.name, item.columns))
    return tuple(sorted(deduplicated, key=lambda item: (item.name is None, item.name or "", item.columns)))


def _sorted_foreign_keys(rows: Iterable[Mapping[str, Any]]) -> tuple[ForeignKeyMetadata, ...]:
    models = [normalize_snowflake_foreign_key(row) for row in aggregate_foreign_key_rows(rows)]
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
    models = [normalize_snowflake_check_constraint(row) for row in rows]
    deduplicated = deduplicate_models(models, lambda item: (item.name, item.expression))
    return tuple(sorted(deduplicated, key=lambda item: (item.name is None, item.name or "", item.expression)))


def _membership_aware_coverage(
    base: DiscoveryCoverage,
    *,
    primary_key_rows: tuple[Mapping[str, Any], ...] | None,
    unique_constraint_rows: tuple[Mapping[str, Any], ...] | None,
    foreign_key_rows: tuple[Mapping[str, Any], ...] | None,
) -> DiscoveryCoverage:
    """Cap Snowflake constraint coverage at the membership evidence available."""

    def resolved_status(rows, evidence, existing):
        if rows is None or not rows or evidence is not CoverageStatus.COMPLETE:
            return evidence
        return existing

    primary_key_evidence = key_constraint_coverage(primary_key_rows)
    unique_evidence = key_constraint_coverage(unique_constraint_rows)
    foreign_key_evidence = foreign_key_coverage(foreign_key_rows)
    return replace(
        base,
        primary_key=resolved_status(primary_key_rows, primary_key_evidence, base.primary_key),
        unique_constraints=resolved_status(unique_constraint_rows, unique_evidence, base.unique_constraints),
        foreign_keys=resolved_status(foreign_key_rows, foreign_key_evidence, base.foreign_keys),
    )


def normalize_snowflake_table(
    object_row: Mapping[str, Any],
    *,
    column_rows: Iterable[Mapping[str, Any]],
    primary_key_rows: Iterable[Mapping[str, Any]] | None = None,
    unique_constraint_rows: Iterable[Mapping[str, Any]] | None = None,
    foreign_key_rows: Iterable[Mapping[str, Any]] | None = None,
    check_constraint_rows: Iterable[Mapping[str, Any]] | None = None,
    coverage: DiscoveryCoverage | None = None,
) -> TableMetadata:
    """Construct one Snowflake table/view model from already-fetched catalog rows."""

    object_metadata = normalize_snowflake_object(object_row)
    primary_rows = None if primary_key_rows is None else tuple(primary_key_rows)
    unique_rows = None if unique_constraint_rows is None else tuple(unique_constraint_rows)
    foreign_rows = None if foreign_key_rows is None else tuple(foreign_key_rows)
    check_rows = None if check_constraint_rows is None else tuple(check_constraint_rows)
    primary_keys = _sorted_unique_constraints(primary_rows or (), ConstraintType.PRIMARY_KEY)
    base_coverage = coverage or default_coverage(
        object_is_view=object_metadata.object_type in {DatabaseObjectType.VIEW, DatabaseObjectType.MATERIALIZED_VIEW},
        primary_key_rows=primary_rows,
        unique_constraint_rows=unique_rows,
        foreign_key_rows=foreign_rows,
        check_constraint_rows=check_rows,
        object_row=object_row,
    )
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
        unique_constraints=_sorted_unique_constraints(unique_rows or (), ConstraintType.UNIQUE),
        foreign_keys=_sorted_foreign_keys(foreign_rows or ()),
        check_constraints=_sorted_checks(check_rows or ()),
        view_definition=optional_text(value_for(object_row, "view_definition")),
        clustering_expression=optional_text(value_for(object_row, "clustering_expression", "clustering_key")),
        is_partitioned=optional_bool(value_for(object_row, "is_partitioned")),
        partitioning_expression=optional_text(value_for(object_row, "partitioning_expression")),
        coverage=_membership_aware_coverage(
            base_coverage,
            primary_key_rows=primary_rows,
            unique_constraint_rows=unique_rows,
            foreign_key_rows=foreign_rows,
        ),
        vendor_metadata=safe_vendor_metadata(object_row),
    )


__all__ = [
    "normalize_snowflake_check_constraint",
    "normalize_snowflake_foreign_key",
    "normalize_snowflake_key_constraint",
    "normalize_snowflake_object",
    "normalize_snowflake_schema",
    "normalize_snowflake_table",
]
