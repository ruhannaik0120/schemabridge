"""Focused contract tests for canonical schema-discovery models and adapters."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, fields
from types import MappingProxyType

import pytest

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
from models.metadata import CanonicalType, ColumnMetadata
from normalizers.postgresql import normalize_postgresql_column
from normalizers.postgresql_discovery import normalize_postgresql_table
from normalizers.snowflake import normalize_snowflake_column
from normalizers.snowflake_discovery import normalize_snowflake_table


def _coverage(status: CoverageStatus = CoverageStatus.COMPLETE) -> DiscoveryCoverage:
    return DiscoveryCoverage(
        columns=status,
        primary_key=status,
        unique_constraints=status,
        foreign_keys=status,
        check_constraints=status,
        comments=status,
        estimated_row_count=status,
        view_definition=status,
        partitioning=status,
        clustering=status,
        warnings=("METADATA_VISIBILITY_LIMITED",),
    )


def _column(name: str = "id", ordinal: int = 1) -> ColumnMetadata:
    return ColumnMetadata(
        catalog_name="Catalog",
        schema_name="Public",
        table_name="Order Details",
        column_name=name,
        ordinal_position=ordinal,
        native_type="INTEGER",
        canonical_type=CanonicalType.INTEGER,
        nullable=None,
        character_length=None,
        numeric_precision=None,
        numeric_scale=None,
        datetime_precision=None,
    )


def _table(**overrides: object) -> TableMetadata:
    values: dict[str, object] = {
        "catalog_name": "Catalog",
        "schema_name": "Public",
        "object_name": "Order Details",
        "system": "postgresql",
        "object_type": DatabaseObjectType.TABLE,
        "persistence": ObjectPersistence.PERMANENT,
        "columns": (_column(),),
        "coverage": _coverage(),
        "vendor_metadata": {},
    }
    values.update(overrides)
    return TableMetadata(**values)


def _assert_plain_json_values(value: object) -> None:
    assert not isinstance(value, MappingProxyType)
    if isinstance(value, dict):
        for item in value.values():
            _assert_plain_json_values(item)
    elif isinstance(value, list):
        for item in value:
            _assert_plain_json_values(item)


def test_discovery_enums_have_the_approved_values():
    assert {item.value for item in DatabaseObjectType} == {
        "TABLE", "VIEW", "MATERIALIZED_VIEW", "EXTERNAL_TABLE", "DYNAMIC_TABLE", "FOREIGN_TABLE", "PARTITIONED_TABLE", "UNKNOWN",
    }
    assert {item.value for item in ObjectPersistence} == {"PERMANENT", "TRANSIENT", "TEMPORARY", "UNLOGGED", "UNKNOWN"}
    assert {item.value for item in ConstraintType} == {"PRIMARY_KEY", "UNIQUE"}
    assert {item.value for item in CoverageStatus} == {"COMPLETE", "PARTIAL", "UNAVAILABLE", "NOT_APPLICABLE"}


def test_models_are_frozen_deeply_isolated_and_json_safe():
    source_vendor_metadata = {"nested": [{"items": {"one", "two"}}]}
    schema = SchemaMetadata(
        catalog_name="Sales Db",
        schema_name="Select; --",
        system="postgresql",
        owner="Owner Name",
        vendor_metadata=source_vendor_metadata,
    )
    source_vendor_metadata["nested"][0]["items"].add("changed")
    table = _table(
        primary_key=KeyConstraintMetadata(
            name="pk order",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=("tenant id", "order-id"),
            vendor_metadata={"details": ["kept"]},
        ),
        foreign_keys=(
            ForeignKeyMetadata(
                name="fk; DROP TABLE",
                local_columns=("tenant id", "order-id"),
                referenced_catalog="Sales Db",
                referenced_schema="public",
                referenced_table="Parent Table",
                referenced_columns=("tenant id", "id"),
                vendor_metadata={},
            ),
        ),
        check_constraints=(
            CheckConstraintMetadata(name=None, expression='"amount" > 0', vendor_metadata={}),
        ),
        vendor_metadata={"schema": schema},
    )

    with pytest.raises(FrozenInstanceError):
        schema.schema_name = "other"
    with pytest.raises(TypeError):
        schema.vendor_metadata["new"] = "value"
    assert schema.vendor_metadata["nested"][0]["items"] == frozenset({"one", "two"})

    payload = table.to_dict()
    _assert_plain_json_values(payload)
    assert json.loads(json.dumps(payload)) == payload
    assert payload["vendor_metadata"]["schema"]["schema_name"] == "Select; --"


def test_existing_column_constructor_order_is_preserved_and_new_fields_are_appended():
    metadata = ColumnMetadata(
        None, None, "orders", "id", 1, "integer", CanonicalType.INTEGER,
        False, None, None, None, None, False, False, {"source": "legacy"},
    )
    names = [item.name for item in fields(ColumnMetadata)]
    assert names[:15] == [
        "catalog_name", "schema_name", "table_name", "column_name", "ordinal_position", "native_type", "canonical_type",
        "nullable", "character_length", "numeric_precision", "numeric_scale", "datetime_precision", "is_primary_key", "is_foreign_key", "vendor_metadata",
    ]
    assert metadata.default_expression is None
    assert metadata.to_dict()["vendor_metadata"] == {"source": "legacy"}


@pytest.mark.parametrize("value", ["", "\x00bad"])
def test_required_identifiers_reject_empty_or_nul_values(value):
    with pytest.raises(ValueError):
        SchemaMetadata(catalog_name=None, schema_name=value, system="postgresql", vendor_metadata={})
    with pytest.raises(ValueError):
        _table(object_name=value)


def test_identifier_content_and_native_casing_are_preserved_exactly():
    table = _table(catalog_name="Sales DB", schema_name="Mixed Case", object_name='Select; "Quoted"')
    assert table.catalog_name == "Sales DB"
    assert table.schema_name == "Mixed Case"
    assert table.object_name == 'Select; "Quoted"'


def test_foreign_key_membership_allows_unavailable_references_but_rejects_mismatch():
    unavailable = ForeignKeyMetadata(
        name="fk_orders",
        local_columns=("customer_id",),
        referenced_catalog=None,
        referenced_schema=None,
        referenced_table="customers",
        referenced_columns=(),
        vendor_metadata={},
    )
    assert unavailable.referenced_columns == ()
    with pytest.raises(ValueError, match="column counts"):
        ForeignKeyMetadata(
            name="fk_orders",
            local_columns=("tenant_id", "customer_id"),
            referenced_catalog=None,
            referenced_schema=None,
            referenced_table="customers",
            referenced_columns=("id",),
            vendor_metadata={},
        )


def test_non_negative_dimensions_are_validated_but_negative_numeric_scale_is_valid():
    metadata = _column()
    valid = ColumnMetadata(
        **{**metadata.to_dict(), "canonical_type": CanonicalType.DECIMAL, "numeric_scale": -3, "vendor_metadata": {}}
    )
    assert valid.numeric_scale == -3
    with pytest.raises(ValueError, match="array_dimensions"):
        ColumnMetadata(
            **{**metadata.to_dict(), "array_dimensions": -1, "vendor_metadata": {}}
        )
    with pytest.raises(ValueError, match="estimated_row_count"):
        _table(estimated_row_count=-1)


def test_postgresql_column_normalizes_extended_metadata_and_array_element_type():
    metadata = normalize_postgresql_column(
        {
            "COLUMN_NAME": "Items",
            "DATA_TYPE": "integer[]",
            "COLUMN_DEFAULT": "ARRAY[]::integer[]",
            "COLUMN_COMMENT": "item ids",
            "COLLATION_NAME": "C",
            "IS_IDENTITY": "YES",
            "IDENTITY_GENERATION": "BY DEFAULT",
            "IS_AUTO_INCREMENT": "NO",
            "IS_GENERATED": "ALWAYS",
            "GENERATION_EXPRESSION": "upper(code)",
            "IS_UNIQUE_KEY": "YES",
            "ARRAY_DIMENSIONS": "2",
            "ELEMENT_NATIVE_TYPE": "int4",
        },
        catalog_name="SalesDb",
        schema_name="Mixed Case",
        table_name="Order Items",
    )
    assert metadata.default_expression == "ARRAY[]::integer[]"
    assert metadata.comment == "item ids"
    assert metadata.collation == "C"
    assert metadata.is_identity is True
    assert metadata.identity_generation == "BY DEFAULT"
    assert metadata.is_generated is True
    assert metadata.is_unique_key is True
    assert metadata.array_dimensions == 2
    assert metadata.element_native_type == "int4"
    assert metadata.element_canonical_type is CanonicalType.INTEGER


def test_snowflake_column_normalizes_identity_default_virtual_and_semi_structured_facts():
    metadata = normalize_snowflake_column(
        {
            "COLUMN_NAME": "Payload",
            "DATA_TYPE": "ARRAY",
            "COLUMN_DEFAULT": "PARSE_JSON('[]')",
            "COMMENT": "raw payload",
            "IS_IDENTITY": "YES",
            "IDENTITY_GENERATION": "BY DEFAULT",
            "IS_AUTOINCREMENT": "YES",
            "KIND": "VIRTUAL_COLUMN",
            "EXPRESSION": "src:items",
            "IS_UNIQUE_KEY": "NO",
        },
        catalog_name="TARGET_DB",
        schema_name="PUBLIC",
        table_name="Events",
    )
    assert metadata.canonical_type is CanonicalType.SEMI_STRUCTURED
    assert metadata.default_expression == "PARSE_JSON('[]')"
    assert metadata.comment == "raw payload"
    assert metadata.is_identity is True
    assert metadata.is_auto_increment is True
    assert metadata.is_generated is True
    assert metadata.generation_expression == "src:items"
    assert metadata.is_unique_key is False
    assert metadata.element_native_type is None


@pytest.mark.parametrize("normalizer", [normalize_postgresql_table, normalize_snowflake_table])
def test_table_normalizers_sort_columns_preserve_composite_order_and_deduplicate(normalizer):
    object_row = {
        "TABLE_CATALOG": "Catalog",
        "TABLE_SCHEMA": "Mixed Case",
        "TABLE_NAME": "Order Details",
        "TABLE_TYPE": "BASE TABLE",
        "PERSISTENCE": "PERMANENT",
        "COMMENT": None,
        "VENDOR_METADATA": {"safe": {"nested": [1]}},
    }
    table = normalizer(
        object_row,
        column_rows=[
            {"COLUMN_NAME": "second", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 2},
            {"COLUMN_NAME": "first", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1},
            {"COLUMN_NAME": "first", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1},
        ],
        primary_key_rows=[
            {"CONSTRAINT_NAME": "pk_orders", "COLUMN_NAME": "second", "KEY_SEQUENCE": 2},
            {"CONSTRAINT_NAME": "pk_orders", "COLUMN_NAME": "first", "KEY_SEQUENCE": 1},
            {"CONSTRAINT_NAME": "pk_orders", "COLUMN_NAME": "first", "KEY_SEQUENCE": 1},
        ],
        unique_constraint_rows=[
            {"CONSTRAINT_NAME": "z_unique", "COLUMN_NAME": "second", "KEY_SEQUENCE": 1},
            {"CONSTRAINT_NAME": "a_unique", "COLUMN_NAME": "first", "KEY_SEQUENCE": 1},
            {"CONSTRAINT_NAME": "a_unique", "COLUMN_NAME": "second", "KEY_SEQUENCE": 2},
        ],
        foreign_key_rows=[
            {
                "CONSTRAINT_NAME": "fk_parent", "LOCAL_COLUMN_NAME": "second", "REFERENCED_COLUMN_NAME": "id2",
                "REFERENCED_TABLE": "Parent", "KEY_SEQUENCE": 2,
            },
            {
                "CONSTRAINT_NAME": "fk_parent", "LOCAL_COLUMN_NAME": "first", "REFERENCED_COLUMN_NAME": "id1",
                "REFERENCED_TABLE": "Parent", "KEY_SEQUENCE": 1,
            },
        ],
        check_constraint_rows=[
            {"CONSTRAINT_NAME": "check_value", "CHECK_CLAUSE": "first > 0"},
            {"CONSTRAINT_NAME": "check_value", "CHECK_CLAUSE": "first > 0"},
        ],
    )
    assert [column.column_name for column in table.columns] == ["first", "second"]
    assert table.primary_key is not None
    assert table.primary_key.columns == ("first", "second")
    assert [item.name for item in table.unique_constraints] == ["a_unique", "z_unique"]
    assert table.unique_constraints[0].columns == ("first", "second")
    assert table.foreign_keys[0].local_columns == ("first", "second")
    assert table.foreign_keys[0].referenced_columns == ("id1", "id2")
    assert len(table.check_constraints) == 1
    assert table.coverage.comments is CoverageStatus.COMPLETE
    assert table.vendor_metadata == {"safe": {"nested": (1,)}}


def test_unavailable_and_confirmed_empty_constraint_collections_are_distinct():
    object_row = {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"}
    unavailable = normalize_snowflake_table(object_row, column_rows=[])
    confirmed_empty = normalize_snowflake_table(
        object_row,
        column_rows=[],
        primary_key_rows=[],
        unique_constraint_rows=[],
        foreign_key_rows=[],
        check_constraint_rows=[],
    )
    assert unavailable.unique_constraints == ()
    assert unavailable.coverage.unique_constraints is CoverageStatus.UNAVAILABLE
    assert confirmed_empty.unique_constraints == ()
    assert confirmed_empty.coverage.unique_constraints is CoverageStatus.COMPLETE


def test_snowflake_declared_unenforced_constraint_and_partial_coverage_are_preserved():
    coverage = _coverage(CoverageStatus.PARTIAL)
    table = normalize_snowflake_table(
        {"TABLE_CATALOG": "TARGET", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "TABLE_TYPE": "BASE TABLE"},
        column_rows=[],
        unique_constraint_rows=[
            {"CONSTRAINT_NAME": "uq_orders", "COLUMNS": ["ORDER_ID"], "ENFORCED": "NO", "RELY": "YES"},
        ],
        coverage=coverage,
    )
    assert table.unique_constraints[0].is_enforced is False
    assert table.unique_constraints[0].is_rely is True
    assert table.coverage.unique_constraints is CoverageStatus.PARTIAL


def test_discovery_vendor_metadata_excludes_explicit_sensitive_values():
    schema = SchemaMetadata(
        catalog_name="DB",
        schema_name="PUBLIC",
        system="snowflake",
        vendor_metadata={"safe": "value", "nested": {"token": "secret"}},
    )
    assert schema.to_dict()["vendor_metadata"] == {"safe": "value", "nested": {}}
    table = normalize_snowflake_table(
        {
            "TABLE_CATALOG": "DB",
            "TABLE_SCHEMA": "PUBLIC",
            "TABLE_NAME": "T",
            "TABLE_TYPE": "BASE TABLE",
            "VENDOR_METADATA": {"safe": "value", "password": "secret", "nested": {"token": "secret"}},
        },
        column_rows=[],
    )
    assert table.vendor_metadata == {"safe": "value", "nested": {}}


def test_column_vendor_metadata_excludes_driver_failures_and_connection_details():
    metadata = normalize_postgresql_column(
        {
            "COLUMN_NAME": "id",
            "DATA_TYPE": "integer",
            "safe": "value",
            "password": "secret",
            "nested": {"host": "private.example", "kept": "value"},
            "driver_error": RuntimeError("secret"),
        },
        catalog_name="DB",
        schema_name="public",
        table_name="orders",
    )
    assert metadata.vendor_metadata == {
        "COLUMN_NAME": "id",
        "DATA_TYPE": "integer",
        "safe": "value",
        "nested": {"kept": "value"},
    }


def test_vendor_metadata_sanitizer_preserves_legitimate_larger_words_and_removes_exact_sensitive_keys():
    legitimate = {
        "accounting_code": "A-17",
        "business_username": "customer_handle",
        "error_policy": "continue",
        "hostile_takeover_flag": False,
        "database_type": "warehouse",
        "catalog_version": "2",
        "account_status": "active",
        "connection_type": "native",
        "object_type": "BASE TABLE",
    }
    prohibited = {
        "PASSWORD": "password-marker",
        "Access Token": "token-marker",
        "PRIVATE.KEY": "key-marker",
        "RAW-PROFILE JSON": "profile-marker",
        "driver_error": "driver-marker",
        "account_identifier": "account-marker",
    }
    schema = SchemaMetadata(
        catalog_name="DB",
        schema_name="PUBLIC",
        system="snowflake",
        vendor_metadata={**legitimate, **prohibited},
    )

    payload = schema.to_dict()["vendor_metadata"]
    assert payload == legitimate
    serialized = json.dumps(schema.to_dict())
    assert not any(marker in serialized for marker in prohibited.values())


def test_vendor_metadata_sanitization_handles_nested_frozensets_and_never_exposes_raw_exceptions():
    exception_marker = "raw-exception-marker"
    profile_marker = "raw-profile-marker"
    schema = SchemaMetadata(
        catalog_name="DB",
        schema_name="PUBLIC",
        system="snowflake",
        vendor_metadata={
            "nested": (frozenset({RuntimeError(exception_marker)}), [{"raw_profile_json": profile_marker}]),
            "safe": "kept",
        },
    )

    assert not isinstance(schema.vendor_metadata["nested"][0], BaseException)
    assert all(not isinstance(item, BaseException) for item in schema.vendor_metadata["nested"][0])
    payload = schema.to_dict()
    serialized = json.dumps(payload)
    assert exception_marker not in serialized
    assert profile_marker not in serialized
    assert exception_marker not in repr(schema)
    assert profile_marker not in repr(schema)
    assert payload["vendor_metadata"]["safe"] == "kept"


def test_vendor_metadata_is_excluded_from_every_generated_model_repr():
    marker = "vendor-secret-marker"
    models = [
        ColumnMetadata(None, None, "T", "C", 1, "INTEGER", CanonicalType.INTEGER, None, None, None, None, None, vendor_metadata={"safe": marker}),
        SchemaMetadata(catalog_name=None, schema_name="S", system="snowflake", vendor_metadata={"safe": marker}),
        DatabaseObjectMetadata(
            catalog_name=None,
            schema_name="S",
            object_name="T",
            system="snowflake",
            object_type=DatabaseObjectType.TABLE,
            persistence=ObjectPersistence.PERMANENT,
            vendor_metadata={"safe": marker},
        ),
        KeyConstraintMetadata(name="PK", constraint_type=ConstraintType.PRIMARY_KEY, columns=("C",), vendor_metadata={"safe": marker}),
        ForeignKeyMetadata(
            name="FK",
            local_columns=("C",),
            referenced_catalog=None,
            referenced_schema=None,
            referenced_table="P",
            referenced_columns=("C",),
            vendor_metadata={"safe": marker},
        ),
        CheckConstraintMetadata(name="CK", expression="C > 0", vendor_metadata={"safe": marker}),
        _table(vendor_metadata={"safe": marker}),
    ]

    for model in models:
        assert "vendor_metadata" not in repr(model)
        assert marker not in repr(model)


def test_mutating_to_dict_result_cannot_change_the_model():
    schema = SchemaMetadata(
        catalog_name="DB",
        schema_name="PUBLIC",
        system="snowflake",
        vendor_metadata={"nested": {"values": ["original"]}},
    )
    payload = schema.to_dict()
    payload["schema_name"] = "CHANGED"
    payload["vendor_metadata"]["nested"]["values"].append("changed")

    assert schema.schema_name == "PUBLIC"
    assert schema.to_dict()["vendor_metadata"]["nested"]["values"] == ["original"]


@pytest.mark.parametrize("value", ["", "   ", "bad\x00column"])
def test_constraint_column_membership_rejects_empty_or_nul_identifiers(value):
    with pytest.raises(ValueError):
        KeyConstraintMetadata(
            name="PK",
            constraint_type=ConstraintType.PRIMARY_KEY,
            columns=(value,),
            vendor_metadata={},
        )
    with pytest.raises(ValueError):
        ForeignKeyMetadata(
            name="FK",
            local_columns=(value,),
            referenced_catalog=None,
            referenced_schema=None,
            referenced_table="PARENT",
            referenced_columns=(),
            vendor_metadata={},
        )
    with pytest.raises(ValueError):
        ForeignKeyMetadata(
            name="FK",
            local_columns=("valid",),
            referenced_catalog=None,
            referenced_schema=None,
            referenced_table="PARENT",
            referenced_columns=(value,),
            vendor_metadata={},
        )


@pytest.mark.parametrize("normalizer", [normalize_postgresql_table, normalize_snowflake_table])
def test_equal_and_missing_ordinals_use_exact_native_column_name_as_tie_break(normalizer):
    object_row = {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"}
    rows = [
        {"COLUMN_NAME": "zeta", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": None},
        {"COLUMN_NAME": "Beta", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1},
        {"COLUMN_NAME": "alpha", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": None},
        {"COLUMN_NAME": "Alpha", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1},
    ]

    forward = normalizer(object_row, column_rows=rows)
    reverse = normalizer(object_row, column_rows=list(reversed(rows)))
    expected = ["Alpha", "Beta", "alpha", "zeta"]
    assert [column.column_name for column in forward.columns] == expected
    assert [column.column_name for column in reverse.columns] == expected


@pytest.mark.parametrize("normalizer", [normalize_postgresql_table, normalize_snowflake_table])
def test_conflicting_duplicate_column_rows_choose_richest_then_canonical_json_lexically(normalizer):
    object_row = {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"}
    rich_rows = [
        {"COLUMN_NAME": "C", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1, "COMMENT": None},
        {"COLUMN_NAME": "C", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 1, "COMMENT": "kept", "COLLATION": "C"},
    ]
    lexical_rows = [
        {"COLUMN_NAME": "D", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 2, "COMMENT": "zeta"},
        {"COLUMN_NAME": "D", "DATA_TYPE": "INTEGER", "ORDINAL_POSITION": 2, "COMMENT": "alpha"},
    ]

    for rows in (rich_rows + lexical_rows, list(reversed(rich_rows + lexical_rows))):
        table = normalizer(object_row, column_rows=rows)
        assert table.columns[0].comment == "kept"
        assert table.columns[0].collation == "C"
        assert table.columns[1].comment == "alpha"


@pytest.mark.parametrize(
    ("argument", "row", "coverage_field"),
    [
        ("primary_key_rows", {"CONSTRAINT_NAME": "PK_T", "ENFORCED": "NO", "RELY": "YES"}, "primary_key"),
        ("unique_constraint_rows", {"CONSTRAINT_NAME": "UQ_T", "ENFORCED": "NO", "RELY": "YES"}, "unique_constraints"),
        (
            "foreign_key_rows",
            {"CONSTRAINT_NAME": "FK_T", "REFERENCED_TABLE": "P", "REFERENCED_COLUMNS": ["ID"]},
            "foreign_keys",
        ),
        (
            "foreign_key_rows",
            {"CONSTRAINT_NAME": "FK_T", "REFERENCED_TABLE": "P", "LOCAL_COLUMNS": ["ID"]},
            "foreign_keys",
        ),
    ],
)
def test_snowflake_nonempty_constraints_with_missing_membership_are_partial(argument, row, coverage_field):
    object_row = {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"}
    table = normalize_snowflake_table(object_row, column_rows=[], **{argument: [row]})

    assert getattr(table.coverage, coverage_field) is CoverageStatus.PARTIAL
    if argument == "primary_key_rows":
        assert table.primary_key is not None
        assert table.primary_key.name == "PK_T"
        assert table.primary_key.columns == ()
        assert table.primary_key.is_enforced is False
        assert table.primary_key.is_rely is True
    elif argument == "unique_constraint_rows":
        assert table.unique_constraints[0].name == "UQ_T"
        assert table.unique_constraints[0].columns == ()
        assert table.unique_constraints[0].is_enforced is False
        assert table.unique_constraints[0].is_rely is True
    else:
        assert table.foreign_keys[0].name == "FK_T"


def test_snowflake_empty_and_omitted_constraint_results_keep_distinct_coverage_for_all_memberships():
    object_row = {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"}
    omitted = normalize_snowflake_table(object_row, column_rows=[])
    empty = normalize_snowflake_table(
        object_row,
        column_rows=[],
        primary_key_rows=[],
        unique_constraint_rows=[],
        foreign_key_rows=[],
    )

    for field_name in ("primary_key", "unique_constraints", "foreign_keys"):
        assert getattr(omitted.coverage, field_name) is CoverageStatus.UNAVAILABLE
        assert getattr(empty.coverage, field_name) is CoverageStatus.COMPLETE


def test_snowflake_mismatched_foreign_key_membership_is_partial_without_inventing_references():
    table = normalize_snowflake_table(
        {"TABLE_CATALOG": "DB", "TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "T", "TABLE_TYPE": "BASE TABLE"},
        column_rows=[],
        foreign_key_rows=[
            {
                "CONSTRAINT_NAME": "FK_T",
                "LOCAL_COLUMNS": ["TENANT_ID", "PARENT_ID"],
                "REFERENCED_TABLE": "PARENT",
                "REFERENCED_COLUMNS": ["ID"],
                "ENFORCED": "NO",
                "RELY": "YES",
            },
        ],
    )

    foreign_key = table.foreign_keys[0]
    assert foreign_key.local_columns == ("TENANT_ID", "PARENT_ID")
    assert foreign_key.referenced_columns == ()
    assert foreign_key.is_enforced is False
    assert foreign_key.is_rely is True
    assert table.coverage.foreign_keys is CoverageStatus.PARTIAL


def test_json_dumps_succeeds_for_every_canonical_model_directly():
    table = _table(
        primary_key=KeyConstraintMetadata(name="PK", constraint_type=ConstraintType.PRIMARY_KEY, columns=("id",), vendor_metadata={}),
        unique_constraints=(KeyConstraintMetadata(name="UQ", constraint_type=ConstraintType.UNIQUE, columns=("id",), vendor_metadata={}),),
        foreign_keys=(
            ForeignKeyMetadata(
                name="FK",
                local_columns=("id",),
                referenced_catalog=None,
                referenced_schema=None,
                referenced_table="P",
                referenced_columns=("id",),
                vendor_metadata={},
            ),
        ),
        check_constraints=(CheckConstraintMetadata(name="CK", expression="id > 0", vendor_metadata={}),),
    )
    models = [
        SchemaMetadata(catalog_name=None, schema_name="S", system="postgresql", vendor_metadata={}),
        DatabaseObjectMetadata(
            catalog_name=None,
            schema_name="S",
            object_name="T",
            system="postgresql",
            object_type=DatabaseObjectType.TABLE,
            persistence=ObjectPersistence.PERMANENT,
            vendor_metadata={},
        ),
        table.primary_key,
        table.unique_constraints[0],
        table.foreign_keys[0],
        table.check_constraints[0],
        table.coverage,
        table,
    ]

    for model in models:
        assert model is not None
        json.dumps(model.to_dict())
